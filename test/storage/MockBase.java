// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.storage;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.bind.DatatypeConverter;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Ignore;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.stumbleupon.async.Deferred;

/**
 * Mock HBase implementation useful in testing calls to and from storage with
 * actual pretend data. The underlying data store is just a simple tree map 
 * with a hash map of byte arrays. Keys and qualifiers are all converted to hex
 * encoded strings, since you can't use byte arrays as map keys in the default
 * Java collections.
 * <p>
 * It's not a perfect mock but is useful for the majority of unit tests. Gets,
 * puts, cas, deletes and scans are currently supported. See notes for each
 * inner class below about what does and doesn't work.
 * <p>
 * <b>Note:</b> At this time, the implementation does not support multiple 
 * column families since almost all unit tests for OpenTSDB only work with one
 * CF at a time. There is also only one table and we don't have any timestamps.
 * <p>
 * <b>Warning:</b> To use this class, you need to prepare the classes for testing
 * with the @PrepareForTest annotation. The classes you need to prepare are:
 * <ul><li>TSDB</li>
 * <li>HBaseClient</li>
 * <li>GetRequest</li>
 * <li>PutRequest</li>
 * <li>KeyValue</li>
 * <li>Scanner</li>
 * <li>DeleteRequest</li>
 * <li>AtomicIncrementRequest</li></ul>
 * @since 2.0
 */
@Ignore
public final class MockBase {
  private static final Charset ASCII = Charset.forName("ISO-8859-1");
  private TSDB tsdb;
  private TreeMap<String, HashMap<String, byte[]>> storage = 
    new TreeMap<String, HashMap<String, byte[]>>();
  private HashSet<Integer> used_scanners = new HashSet<Integer>(2);
  private MockScanner local_scanner;
  private Scanner current_scanner;
  
  /**
   * Setups up mock intercepts for all of the calls. Depending on the given
   * flags, some mocks may not be enabled, allowing local unit tests to setup
   * their own mocks.
   * @param default_get Enable the default .get() mock
   * @param default_put Enable the default .put() and .compareAndSet() mocks
   * @param default_delete Enable the default .delete() mock
   * @param default_scan Enable the Scanner mock implementation
   * @return
   */
  public MockBase(
      final TSDB tsdb, final HBaseClient client,
      final boolean default_get, 
      final boolean default_put,
      final boolean default_delete,
      final boolean default_scan) {
    this.tsdb = tsdb;
 
    // replace the "real" field objects with mocks
    Field cl;
    try {
      cl = tsdb.getClass().getDeclaredField("client");
      cl.setAccessible(true);
      cl.set(tsdb, client);
      cl.setAccessible(false);
    } catch (SecurityException e) {
      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }

    // Default get answer will return one or more columns from the requested row
    if (default_get) {
      when(client.get((GetRequest)any())).thenAnswer(new MockGet());
    }
    
    // Default put answer will store the given values in the proper location.
    if (default_put) {
      when(client.put((PutRequest)any())).thenAnswer(new MockPut());
      when(client.compareAndSet((PutRequest)any(), (byte[])any()))
        .thenAnswer(new MockCAS());
    }

    if (default_delete) {
      when(client.delete((DeleteRequest)any())).thenAnswer(new MockDelete());
    }
    
    if (default_scan) {
      current_scanner = mock(Scanner.class);
      local_scanner = new MockScanner(current_scanner);

      // to facilitate unit tests where more than one scanner is used (i.e. in a
      // callback chain) we have to provide a new mock scanner for each new
      // scanner request. That's the way the mock scanner method knows when a
      // second call has been issued and it should return a null.
      when(client.newScanner((byte[]) any())).thenAnswer(new Answer<Scanner>() {

        @Override
        public Scanner answer(InvocationOnMock arg0) throws Throwable {
          if (used_scanners.contains(current_scanner.hashCode())) {            
            current_scanner = mock(Scanner.class);
            local_scanner = new MockScanner(current_scanner);
          }
          when(current_scanner.nextRows()).thenAnswer(local_scanner);
          return current_scanner;
        }
        
      });      

    }
    
    when(client.atomicIncrement((AtomicIncrementRequest)any()))
      .then(new MockAtomicIncrement());
    when(client.bufferAtomicIncrement((AtomicIncrementRequest)any()))
    .then(new MockAtomicIncrement());
  }

  public MockBase(
      final boolean default_get, 
      final boolean default_put,
      final boolean default_delete,
      final boolean default_scan) throws IOException {
    this(new TSDB(new Config(false)), mock(HBaseClient.class), 
        default_get, default_put, default_delete, default_scan);
  }
  
  /**
   * Add a column to the hash table. The proper row will be created if it doesn't
   * exist. If the column already exists, the original value will be overwritten 
   * with the new data
   * @param key The row key
   * @param qualifier The qualifier
   * @param value The value to store
   */
  public void addColumn(final byte[] key, final byte[] qualifier, 
      final byte[] value) {
    if (!storage.containsKey(bytesToString(key))) {
      storage.put(bytesToString(key), new HashMap<String, byte[]>(1));
    }
    storage.get(bytesToString(key)).put(bytesToString(qualifier), value);
  }
  
  /** @return TTotal number of rows in the hash table */
  public int numRows() {
    return storage.size();
  }
  
  /**
   * Total number of columns in the given row
   * @param key The row to search for
   * @return -1 if the row did not exist, otherwise the number of columns.
   */
  public int numColumns(final byte[] key) {
    if (!storage.containsKey(bytesToString(key))) {
      return -1;
    }
    return storage.get(bytesToString(key)).size();
  }

  /**
   * Retrieve the contents of a single column
   * @param key The row key of the column
   * @param qualifier The column qualifier
   * @return The byte array of data or null if not found
   */
  public byte[] getColumn (final byte[] key, final byte[] qualifier) {
    if (!storage.containsKey(bytesToString(key))) {
      return null;
    }
    return storage.get(bytesToString(key)).get(bytesToString(qualifier));
  }
  
  /**
   * Return the mocked TSDB object to use for HBaseClient access
   * @return
   */
  public TSDB getTSDB() {
    return tsdb;
  }
  
  /**
   * Clears the entire hash table. Use it if your unit test needs to start fresh
   */
  public void flushStorage() {
    storage.clear();
  }
  
  /**
   * Removes the entire row from the hash table
   * @param key The row to remove
   */
  public void flushRow(final byte[] key) {
    storage.remove(bytesToString(key));
  }
  
  /**
   * Dumps the entire storage hash to stdout with the row keys and (optionally)
   * qualifiers as hex encoded byte strings. The byte values will pass be
   * converted to ASCII strings. Useful for debugging when writing unit tests,
   * but don't depend on it.
   * @param qualifier_ascii Whether or not the qualifiers should be converted
   * to ASCII.
   */
  public void dumpToSystemOut(final boolean qualifier_ascii) {
    if (storage.isEmpty()) {
      System.out.println("Empty");
      return;
    }
    
    for (Map.Entry<String, HashMap<String, byte[]>> row : storage.entrySet()) {
      System.out.println("Row: " + row.getKey());
      
      for (Map.Entry<String, byte[]> column : row.getValue().entrySet()) {
        System.out.println("  Qualifier: " + (qualifier_ascii ?
            "\"" + new String(stringToBytes(column.getKey()), ASCII) + "\""
            : column.getKey()));
        System.out.println("    Value: " + new String(column.getValue(), ASCII));
      }
    }
  }
  
  /**
   * Helper to convert an array of bytes to a hexadecimal encoded string.
   * @param bytes The byte array to convert
   * @return A hex string
   */
  public static String bytesToString(final byte[] bytes) {
    return DatatypeConverter.printHexBinary(bytes);
  }
  
  /**
   * Helper to convert a hex encoded string into a byte array.
   * <b>Warning:</b> This method won't pad the string to make sure it's an
   * even number of bytes.
   * @param bytes The hex encoded string to convert
   * @return A byte array from the hex string
   * @throws IllegalArgumentException if the string contains illegal characters
   * or can't be converted.
   */
  public static byte[] stringToBytes(final String bytes) {
    return DatatypeConverter.parseHexBinary(bytes);
  }
  
  /** @return Returns the ASCII character set */
  public static Charset ASCII() {
    return ASCII;
  }
  
  /**
   * Gets one or more columns from a row. If the row does not exist, a null is
   * returned. If no qualifiers are given, the entire row is returned.
   */
  private class MockGet implements Answer<Deferred<ArrayList<KeyValue>>> {
    @Override
    public Deferred<ArrayList<KeyValue>> answer(InvocationOnMock invocation)
        throws Throwable {
      final Object[] args = invocation.getArguments();
      final GetRequest get = (GetRequest)args[0];
      final String key = bytesToString(get.key());
      final HashMap<String, byte[]> row = storage.get(key);

      if (row == null) {
        return Deferred.fromResult((ArrayList<KeyValue>)null);
      } if (get.qualifiers() == null || get.qualifiers().length == 0) { 

        // return all columns from the given row
        final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(row.size());
        for (Map.Entry<String, byte[]> entry : row.entrySet()) {
          KeyValue kv = mock(KeyValue.class);
          when(kv.value()).thenReturn(entry.getValue());
          when(kv.qualifier()).thenReturn(stringToBytes(entry.getKey()));
          when(kv.key()).thenReturn(get.key());
          kvs.add(kv);
        }
        return Deferred.fromResult(kvs);
        
      } else {
        
        final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(
            get.qualifiers().length);
        
        for (byte[] q : get.qualifiers()) {
          final String qualifier = bytesToString(q);
          if (!row.containsKey(qualifier)) {
            continue;
          }

          KeyValue kv = mock(KeyValue.class);
          when(kv.value()).thenReturn(row.get(qualifier));
          when(kv.qualifier()).thenReturn(stringToBytes(qualifier));
          when(kv.key()).thenReturn(get.key());
          kvs.add(kv);
        }
        
        if (kvs.size() < 1) {
          return Deferred.fromResult((ArrayList<KeyValue>)null);
        }
        return Deferred.fromResult(kvs);
      }
    }
  }
  
  /**
   * Stores one or more columns in a row. If the row does not exist, it's
   * created.
   */
  private class MockPut implements Answer<Deferred<Boolean>> {
    @Override
    public Deferred<Boolean> answer(final InvocationOnMock invocation) 
      throws Throwable {
      final Object[] args = invocation.getArguments();
      final PutRequest put = (PutRequest)args[0];
      final String key = bytesToString(put.key());
      
      HashMap<String, byte[]> column = storage.get(key);
      if (column == null) {
        column = new HashMap<String, byte[]>();
        storage.put(key, column);
      }
      
      for (int i = 0; i < put.qualifiers().length; i++) {
        column.put(bytesToString(put.qualifiers()[i]), put.values()[i]);
      }
      
      return Deferred.fromResult(true);
    }
  }
  
  /**
   * Imitates the compareAndSet client call where a {@code PutRequest} is passed
   * along with a byte array to compared the stored value against. If the stored
   * value doesn't match, the put is ignored and a "false" is returned. If the 
   * comparator matches, the new put is recorded.
   * <b>Warning:</b> While a put works on multiple qualifiers, CAS only works
   * with one. So if the put includes more than one qualifier, only the first
   * one will be processed in this CAS call. 
   */
  private class MockCAS implements Answer<Deferred<Boolean>> {
    
    @Override
    public Deferred<Boolean> answer(final InvocationOnMock invocation) 
      throws Throwable {
      final Object[] args = invocation.getArguments();
      final PutRequest put = (PutRequest)args[0];
      final byte[] expected = (byte[])args[1];
      final String key = bytesToString(put.key());
      
      HashMap<String, byte[]> column = storage.get(key);
      if (column == null) {
        if (expected != null && expected.length > 0) {
          return Deferred.fromResult(false);
        }
        
        column = new HashMap<String, byte[]>();
        storage.put(key, column);
      }
      
      // CAS can only operate on one cell, so if the put request has more than 
      // one, we ignore any but the first
      final byte[] stored = column.get(bytesToString(put.qualifiers()[0])); 
      if (stored == null && (expected != null && expected.length > 0)) {
        return Deferred.fromResult(false);
      }
      if (stored != null && (expected == null || expected.length < 1)) {
        return Deferred.fromResult(false);
      }
      if (stored != null && expected != null && 
          Bytes.memcmp(stored, expected) != 0) {
        return Deferred.fromResult(false);
      }
      
      // passed CAS!
      column.put(bytesToString(put.qualifiers()[0]), put.value());
      return Deferred.fromResult(true);
    }
    
  }
  
  /**
   * Deletes one or more columns. If a row no longer has any valid columns, the
   * entire row will be removed.
   */
  private class MockDelete implements Answer<Deferred<Object>> {
    
    @Override
    public Deferred<Object> answer(InvocationOnMock invocation)
        throws Throwable {
      final Object[] args = invocation.getArguments();
      final DeleteRequest delete = (DeleteRequest)args[0];
      final String key = bytesToString(delete.key());
      
      if (!storage.containsKey(key)) {
        return Deferred.fromResult(null);
      }
      
      // if no qualifiers, then delete the row
      if (delete.qualifiers() == null) {
        storage.remove(key);
        return Deferred.fromResult(new Object());
      }
      
      HashMap<String, byte[]> column = storage.get(key);
      final byte[][] qualfiers = delete.qualifiers();       
      
      for (byte[] qualifier : qualfiers) {
        final String q = bytesToString(qualifier);
        if (!column.containsKey(q)) {
          continue;
        }
        column.remove(q);
      }
      
      // if all columns were deleted, wipe the row
      if (column.isEmpty()) {
        storage.remove(key);
      }
      return Deferred.fromResult(new Object());
    }
    
  }
  
  /**
   * This is a limited implementation of the scanner object. The only fields
   * caputred and acted on are:
   * <ul><li>KeyRegexp</li>
   * <li>StartKey</li>
   * <li>StopKey</li>
   * <li>Qualifier</li>
   * <li>Qualifiers</li></ul>
   * Hence timestamps are ignored as are the max number of rows and qualifiers.
   * All matching rows/qualifiers will be returned in the first {@code nextRows}
   * call. The second {@code nextRows} call will always return null. Multiple
   * qualifiers are supported for matching.
   * <p>
   * Since the treemap is hex sorted, it should mimic the byte order of HBase
   * and the start and stop rows should match properly.
   * <p>
   * The KeyRegexp can be set and it will run against the hex value of the 
   * row key. In testing it seems to work nicely even with byte patterns.
   */
  private class MockScanner implements 
    Answer<Deferred<ArrayList<ArrayList<KeyValue>>>> {
    
    private String start = null;
    private String stop = null;
    private HashSet<String> scnr_qualifiers = null;
    private String regex = null;
    
    public MockScanner(final Scanner mock_scanner) {

      // capture the scanner fields when set
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          regex = (String)args[0];
          return null;
        }      
      }).when(mock_scanner).setKeyRegexp(anyString());
      
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          start = bytesToString((byte[])args[0]);
          return null;
        }      
      }).when(mock_scanner).setStartKey((byte[])any());
      
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          stop = bytesToString((byte[])args[0]);
          return null;
        }      
      }).when(mock_scanner).setStopKey((byte[])any());
      
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          scnr_qualifiers = new HashSet<String>(1);
          scnr_qualifiers.add(bytesToString((byte[])args[0]));
          return null;
        }      
      }).when(mock_scanner).setQualifier((byte[])any());
      
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          final byte[][] qualifiers = (byte[][])args[0];
          scnr_qualifiers = new HashSet<String>(qualifiers.length);
          for (byte[] qualifier : qualifiers) {
            scnr_qualifiers.add(bytesToString(qualifier));
          }
          return null;
        }      
      }).when(mock_scanner).setQualifiers((byte[][])any());
      
    }
    
    @Override
    public Deferred<ArrayList<ArrayList<KeyValue>>> answer(
        final InvocationOnMock invocation) throws Throwable {
      
      // It's critical to see if this scanner has been processed before, 
      // otherwise the code under test will likely wind up in an infinite loop.
      // If the scanner has been seen before, we return null.
      if (used_scanners.contains(current_scanner.hashCode())) {
        return Deferred.fromResult(null);
      }
      used_scanners.add(current_scanner.hashCode());
      
      Pattern pattern = null;
      if (regex != null && !regex.isEmpty()) {
        try {
          Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
          e.printStackTrace();
        }
      }
      
      // return all matches
      ArrayList<ArrayList<KeyValue>> results = 
        new ArrayList<ArrayList<KeyValue>>();
      for (Map.Entry<String, HashMap<String, byte[]>> row : storage.entrySet()) {
        
        // if it's before the start row, after the end row or doesn't
        // match the given regex, continue on to the next row
        if (start != null && row.getKey().compareTo(start) < 0) {
          continue;
        }
        if (stop != null && row.getKey().compareTo(stop) > 0) {
          continue;
        }
        if (pattern != null && !pattern.matcher(row.getKey()).find()) {
          continue;
        }
        
        // loop on the columns
        final ArrayList<KeyValue> kvs = 
          new ArrayList<KeyValue>(row.getValue().size());
        for (Map.Entry<String, byte[]> entry : row.getValue().entrySet()) {
          
          // if the qualifier isn't in the set, continue
          if (scnr_qualifiers != null && 
              !scnr_qualifiers.contains(entry.getKey())) {
            continue;
          }
          
          KeyValue kv = mock(KeyValue.class);
          when(kv.key()).thenReturn(stringToBytes(row.getKey()));
          when(kv.value()).thenReturn(entry.getValue());
          when(kv.qualifier()).thenReturn(stringToBytes(entry.getKey()));
          kvs.add(kv);
        }
        
        if (!kvs.isEmpty()) {
          results.add(kvs);
        }
      }
      
      if (results.isEmpty()) {
        return Deferred.fromResult(null);
      }
      return Deferred.fromResult(results);
    }
  }
  
  /**
   * Creates or increments (possibly decremnts) a Long in the hash table at the
   * given location.
   */
  private class MockAtomicIncrement implements
    Answer<Deferred<Long>> {

    @Override
    public Deferred<Long> answer(InvocationOnMock invocation) throws Throwable {
      final Object[] args = invocation.getArguments();
      final AtomicIncrementRequest air = (AtomicIncrementRequest)args[0];
      final String key = bytesToString(air.key());
      final long amount = air.getAmount();
      final String qualifier = bytesToString(air.qualifier());
      
      HashMap<String, byte[]> column = storage.get(key);
      if (column == null) {
        column = new HashMap<String, byte[]>(1);
        storage.put(key, column);
      }
      
      if (!column.containsKey(qualifier)) {
        column.put(qualifier, Bytes.fromLong(amount));
        return Deferred.fromResult(amount);
      }
      
      long incremented_value = Bytes.getLong(column.get(qualifier));
      incremented_value += amount;
      column.put(qualifier, Bytes.fromLong(incremented_value));
      return Deferred.fromResult(incremented_value);
    }
    
  }
}
