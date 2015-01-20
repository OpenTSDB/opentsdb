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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
 * actual pretend data. The underlying data store is an incredibly ugly nesting
 * of ByteMaps from AsyncHbase so it stores and orders byte arrays similar to 
 * HBase. A MockBase instance represents a SINGLE table in HBase but it provides
 * support for column families and timestamped entries.
 * <p>
 * It's not a perfect mock but is useful for the majority of unit tests. Gets,
 * puts, cas, deletes and scans are currently supported. See notes for each
 * inner class below about what does and doesn't work.
 * <p>
 * Regarding timestamps, whenever you execute an RPC request, the 
 * {@code current_timestamp} will be incremented by one millisecond. By default
 * the timestamp starts at 1/1/2014 00:00:00 but you can set it to any value
 * at any time. If a PutRequest comes in with a specific time, that time will 
 * be stored and the timestamp will not be incremented.
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
  
  //      KEY           Column Family Qualifier     Timestamp     Value
  private Bytes.ByteMap<Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>>> 
  storage = new Bytes.ByteMap<Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>>>();
  private HashSet<MockScanner> scanners = new HashSet<MockScanner>(2);
  private byte[] default_family;
  
  /** Incremented every time a new value is stored (without a timestamp) */
  private long current_timestamp = 1388534400000L;
  
  /**
   * Setups up mock intercepts for all of the calls. Depending on the given
   * flags, some mocks may not be enabled, allowing local unit tests to setup
   * their own mocks.
   * @param tsdb A real TSDB (not mocked) that should have it's client set with
   * the given mock
   * @param client A mock client that may have been instantiated and should be
   * captured for use with MockBase
   * @param default_get Enable the default .get() mock
   * @param default_put Enable the default .put() and .compareAndSet() mocks
   * @param default_delete Enable the default .delete() mock
   * @param default_scan Enable the Scanner mock implementation
   */
  public MockBase(
      final TSDB tsdb, final HBaseClient client,
      final boolean default_get, 
      final boolean default_put,
      final boolean default_delete,
      final boolean default_scan) {
    this.tsdb = tsdb;
    
    default_family = "t".getBytes(ASCII);  // set a default
    
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
      // to facilitate unit tests where more than one scanner is used (i.e. in a
      // callback chain) we have to provide a new mock scanner for each new
      // scanner request. That's the way the mock scanner method knows when a
      // second call has been issued and it should return a null.
      when(client.newScanner((byte[]) any())).thenAnswer(new Answer<Scanner>() {

        @Override
        public Scanner answer(InvocationOnMock arg0) throws Throwable {
          final Scanner scanner = mock(Scanner.class);
          scanners.add(new MockScanner(scanner));
          return scanner;
        }
        
      });      

    }
    
    when(client.atomicIncrement((AtomicIncrementRequest)any()))
      .then(new MockAtomicIncrement());
    when(client.bufferAtomicIncrement((AtomicIncrementRequest)any()))
    .then(new MockAtomicIncrement());
  }

  /** @param family Sets the family for calls that need it */
  public void setFamily(final byte[] family) {
    this.default_family = family;
  }
  
  /** @param timestamp The timestamp to use for further storage increments */
  public void setCurrentTimestamp(final long timestamp) {
    this.current_timestamp = timestamp;
  }
  
  /** @return the incrementing timestamp */
  public long getCurrentTimestamp() {
    return current_timestamp;
  }
  
  /**
   * Add a column to the hash table using the default column family. 
   * The proper row will be created if it doesn't exist. If the column already 
   * exists, the original value will be overwritten with the new data
   * @param key The row key
   * @param qualifier The qualifier
   * @param value The value to store
   */
  public void addColumn(final byte[] key, final byte[] qualifier, 
      final byte[] value) {
    addColumn(key, default_family, qualifier, value, current_timestamp++);
  }
  
  /**
   * Add a column to the hash table 
   * The proper row will be created if it doesn't exist. If the column already 
   * exists, the original value will be overwritten with the new data
   * @param key The row key
   * @param family The column family to store the value in
   * @param qualifier The qualifier
   * @param value The value to store
   */
  public void addColumn(final byte[] key, final byte[] family, 
      final byte[] qualifier, final byte[] value) {
    addColumn(key, family, qualifier, value, current_timestamp++);
  }
  
  /**
   * Add a column to the hash table 
   * The proper row will be created if it doesn't exist. If the column already 
   * exists, the original value will be overwritten with the new data
   * @param key The row key
   * @param family The column family to store the value in
   * @param qualifier The qualifier
   * @param value The value to store
   * @param timestamp The timestamp to store
   */
  public void addColumn(final byte[] key, final byte[] family, 
      final byte[] qualifier, final byte[] value, final long timestamp) {
    // AsyncHBase will throw an NPE if the user tries to write a NULL value
    // so we better do the same. An empty value is ok though, i.e. new byte[] {}
    if (value == null) {
      throw new NullPointerException();
    }
    
    Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>> row = storage.get(key);
    if (row == null) {
      row = new Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>>();
      storage.put(key, row);
    }
    
    Bytes.ByteMap<TreeMap<Long, byte[]>> cf = row.get(family);
    if (cf == null) {
      cf = new Bytes.ByteMap<TreeMap<Long, byte[]>>();
      row.put(family, cf);
    }
    TreeMap<Long, byte[]> column = cf.get(qualifier);
    if (column == null) {
      // remember, most recent at the top!
      column = new TreeMap<Long, byte[]>(Collections.reverseOrder());
      cf.put(qualifier, column);
    }
    column.put(timestamp, value);
  }
  
  /** @return TTotal number of rows in the hash table */
  public int numRows() {
    return storage.size();
  }
  
  /**
   * Return the total number of column families for the row
   * @param key The row to search for
   * @return -1 if the row did not exist, otherwise the number of column families.
   */
  public int numColumnFamilies(final byte[] key) {
    final Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>> row = 
        storage.get(key);
    if (row == null) {
      return -1;
    }
    return row.size();
  }
  
  /**
   * Total number of columns in the given row across all column families
   * @param key The row to search for
   * @return -1 if the row did not exist, otherwise the number of columns.
   */
  public long numColumns(final byte[] key) {
    final Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>> row = 
        storage.get(key);
    if (row == null) {
      return -1;
    }
    long size = 0;
    for (Map.Entry<byte[], Bytes.ByteMap<TreeMap<Long, byte[]>>> entry : row) {
      size += entry.getValue().size();
    }
    return size;
  }
  
  /**
   * Return the total number of columns for a specific row and family
   * @param key The row to search for
   * @param family The column family to search for
   * @return -1 if the row did not exist, otherwise the number of columns.
   */
  public int numColumnsInFamily(final byte[] key, final byte[] family) {
    final Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>> row = 
        storage.get(key);
    if (row == null) {
      return -1;
    }
    final Bytes.ByteMap<TreeMap<Long, byte[]>> cf = row.get(family);
    if (cf == null) {
      return -1;
    }
    return cf.size();
  }

  /**
   * Retrieve the most recent contents of a single column with the default family
   * @param key The row key of the column
   * @param qualifier The column qualifier
   * @return The byte array of data or null if not found
   */
  public byte[] getColumn(final byte[] key, final byte[] qualifier) {
    return getColumn(key, default_family, qualifier);
  }
  
  /**
   * Retrieve the most recent contents of a single column
   * @param key The row key of the column
   * @param family The column family
   * @param qualifier The column qualifier
   * @return The byte array of data or null if not found
   */
  public byte[] getColumn(final byte[] key, final byte[] family, 
      final byte[] qualifier) {
    final Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>> row = 
        storage.get(key);
    if (row == null) {
      return null;
    }
    final Bytes.ByteMap<TreeMap<Long, byte[]>> cf = row.get(family);
    if (cf == null) {
      return null;
    }
    final TreeMap<Long, byte[]> column = cf.get(qualifier);
    if (column == null) {
      return null;
    }
    return column.firstEntry().getValue();
  }

  /**
   * Retrieve the full map of timestamps and values of a single column with 
   * the default family
   * @param key The row key of the column
   * @param qualifier The column qualifier
   * @return The byte array of data or null if not found
   */
  public TreeMap<Long, byte[]> getFullColumn(final byte[] key, 
      final byte[] qualifier) {
    return getFullColumn(key, default_family, qualifier);
  }
  
  /**
   * Retrieve the full map of timestamps and values of a single column
   * @param key The row key of the column
   * @param family The column family
   * @param qualifier The column qualifier
   * @return The tree map of timestamps and values or null if not found
   */
  public TreeMap<Long, byte[]> getFullColumn(final byte[] key, 
      final byte[] family, final byte[] qualifier) {
    final Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>> row = 
        storage.get(key);
    if (row == null) {
      return null;
    }
    final Bytes.ByteMap<TreeMap<Long, byte[]>> cf = row.get(family);
    if (cf == null) {
      return null;
    }
    final TreeMap<Long, byte[]> column = cf.get(qualifier);
    if (column == null) {
      return null;
    }
    return column;
  }
  
  /**
   * Returns the most recent value from all columns for a given column family
   * @param key The row key
   * @param family The column family ID
   * @return A map of columns if the CF was found, null if no such CF
   */
  public Bytes.ByteMap<byte[]> getColumnFamily(final byte[] key, 
      final byte[] family) {
    final Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>> row = 
        storage.get(key);
    if (row == null) {
      return null;
    }
    final Bytes.ByteMap<TreeMap<Long, byte[]>> cf = row.get(family);
    if (cf == null) {
      return null;
    }
    // convert to a <qualifier, value> byte map
    final Bytes.ByteMap<byte[]> columns = new Bytes.ByteMap<byte[]>();
    for (Map.Entry<byte[], TreeMap<Long, byte[]>> entry : cf.entrySet()) {
      // the <timestamp, value> map should never be null
      columns.put(entry.getKey(), entry.getValue().firstEntry().getValue());
    }
    return columns;
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
    storage.remove(key);
  }
  
  /**
   * Removes the entire column family from the hash table for ALL rows
   * @param family The family to remove
   */
  public void flushFamily(final byte[] family) {
    for (Map.Entry<byte[], Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>>> row : 
      storage.entrySet()) {
      row.getValue().remove(family);
    }
  }
  
  /**
   * Removes the given column from the hash map
   * @param key Row key
   * @param family Column family
   * @param qualifier Column qualifier
   */
  public void flushColumn(final byte[] key, final byte[] family, 
      final byte[] qualifier) {
    final Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>> row = 
        storage.get(key);
    if (row == null) {
      return;
    }
    final Bytes.ByteMap<TreeMap<Long, byte[]>> cf = row.get(family);
    if (cf == null) {
      return;
    }
    cf.remove(qualifier);
  }
  
  /**
   * Dumps the entire storage hash to stdout in a sort of tree style format with
   * all byte arrays hex encoded
   */
  public void dumpToSystemOut() {
    dumpToSystemOut(false);
  }
  
  /**
   * Dumps the entire storage hash to stdout in a sort of tree style format
   * @param ascii Whether or not the values should be converted to ascii
   */
  public void dumpToSystemOut(final boolean ascii) {
    if (storage.isEmpty()) {
      System.out.println("Storage is Empty");
      return;
    }
    
    for (Map.Entry<byte[], Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>>> row : 
      storage.entrySet()) {
      System.out.println("[Row] " + (ascii ? new String(row.getKey(), ASCII) : 
          bytesToString(row.getKey())));
      
      for (Map.Entry<byte[], Bytes.ByteMap<TreeMap<Long, byte[]>>> cf : 
        row.getValue().entrySet()) {
        
        final String family = ascii ? new String(cf.getKey(), ASCII) :
          bytesToString(cf.getKey());
        System.out.println("  [CF] " + family);
        
        for (Map.Entry<byte[], TreeMap<Long, byte[]>> column : cf.getValue().entrySet()) {
          System.out.println("    [Qual] " + (ascii ?
              "\"" + new String(column.getKey(), ASCII) + "\""
              : bytesToString(column.getKey())));
          for (Map.Entry<Long, byte[]> cell : column.getValue().entrySet()) {
            System.out.println("      [TS] " + cell.getKey() + "  [Value] " + 
                (ascii ?  new String(cell.getValue(), ASCII) 
                : bytesToString(cell.getValue())));
          }
        }
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
   * Concatenates byte arrays into one big array
   * @param arrays Any number of arrays to concatenate
   * @return The concatenated array
   */
  public static byte[] concatByteArrays(final byte[]... arrays) {
    int len = 0;
    for (final byte[] array : arrays) {
      len += array.length;
    }
    final byte[] result = new byte[len];
    len = 0;
    for (final byte[] array : arrays) {
      System.arraycopy(array, 0, result, len, array.length);
      len += array.length;
    }
    return result;
  }
  
  /**
   * Gets one or more columns from a row. If the row does not exist, a null is
   * returned. If no qualifiers are given, the entire row is returned. 
   * NOTE: all timestamp, value pairs are returned.
   */
  private class MockGet implements Answer<Deferred<ArrayList<KeyValue>>> {
    @Override
    public Deferred<ArrayList<KeyValue>> answer(InvocationOnMock invocation)
        throws Throwable {
      final Object[] args = invocation.getArguments();
      final GetRequest get = (GetRequest)args[0];
      
      final Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>> row = 
          storage.get(get.key());

      if (row == null) {
        return Deferred.fromResult((ArrayList<KeyValue>)null);
      } 
      
      final byte[] family = get.family();
      if (family != null && family.length > 0) {
        if (!row.containsKey(family)) {
          return Deferred.fromResult((ArrayList<KeyValue>)null);
        }
      }
      
      // compile a set of qualifiers to use as a filter if necessary
      Bytes.ByteMap<Object> qualifiers = new Bytes.ByteMap<Object>();
      if (get.qualifiers() != null && get.qualifiers().length > 0) { 
        for (byte[] q : get.qualifiers()) {
          qualifiers.put(q, null);
        }
      }
      
      final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(row.size());
      for (Map.Entry<byte[], Bytes.ByteMap<TreeMap<Long, byte[]>>> cf : 
        row.entrySet()) {
        
        // column family filter
        if (family != null && family.length > 0 && 
            !Bytes.equals(family, cf.getKey())) {
          continue;
        }
        
        for (Map.Entry<byte[], TreeMap<Long, byte[]>> column : 
          cf.getValue().entrySet()) {
          // qualifier filter
          if (!qualifiers.isEmpty() && !qualifiers.containsKey(column.getKey())) {
            continue;
          }
          
          // TODO - if we want to support multiple values, iterate over the 
          // tree map. Otherwise Get returns just the latest value.
          KeyValue kv = mock(KeyValue.class);
          when(kv.timestamp()).thenReturn(column.getValue().firstKey());
          when(kv.value()).thenReturn(column.getValue().firstEntry().getValue());
          when(kv.qualifier()).thenReturn(column.getKey());
          when(kv.key()).thenReturn(get.key());
          kvs.add(kv);
        }
      }
      return Deferred.fromResult(kvs);
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

      Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>> row = 
          storage.get(put.key());
      if (row == null) {
        row = new Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>>();
        storage.put(put.key(), row);
      }
      
      Bytes.ByteMap<TreeMap<Long, byte[]>> cf = row.get(put.family());
      if (cf == null) {
        cf = new Bytes.ByteMap<TreeMap<Long, byte[]>>();
        row.put(put.family(), cf);
      }
      
      for (int i = 0; i < put.qualifiers().length; i++) {
        TreeMap<Long, byte[]> column = cf.get(put.qualifiers()[i]);
        if (column == null) {
          column = new TreeMap<Long, byte[]>(Collections.reverseOrder());
          cf.put(put.qualifiers()[i], column);
        }
        
        column.put(put.timestamp() != Long.MAX_VALUE ? put.timestamp() : 
          current_timestamp++, put.values()[i]);
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
      
      Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>> row = 
          storage.get(put.key());
      if (row == null) {
        if (expected != null && expected.length > 0) {
          return Deferred.fromResult(false);
        }
        
        row = new Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>>();
        storage.put(put.key(), row);
      }
      
      Bytes.ByteMap<TreeMap<Long, byte[]>> cf = row.get(put.family());
      if (cf == null) {
        if (expected != null && expected.length > 0) {
          return Deferred.fromResult(false);
        }
        
        cf = new Bytes.ByteMap<TreeMap<Long, byte[]>>();
        row.put(put.family(), cf);
      }
      
      // CAS can only operate on one cell, so if the put request has more than 
      // one, we ignore any but the first
      TreeMap<Long, byte[]> column = cf.get(put.qualifiers()[0]);
      if (column == null && (expected != null && expected.length > 0)) {
        return Deferred.fromResult(false);
      }
      // if a timestamp was specified, maybe we're CASing against a specific 
      // cell. Otherwise we deal with the latest value
      final byte[] stored = column == null ? null : 
        put.timestamp() != Long.MAX_VALUE ? column.get(put.timestamp()) :
        column.firstEntry().getValue();
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
      if (column == null) {
        column = new TreeMap<Long, byte[]>(Collections.reverseOrder());
        cf.put(put.qualifiers()[0], column);
      }
      column.put(put.timestamp() != Long.MAX_VALUE ? put.timestamp() : 
        current_timestamp++, put.value());
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
      
      Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>> row = 
          storage.get(delete.key());
      if (row == null) {
        return Deferred.fromResult(null);
      }
      
      // if no qualifiers or family, then delete the row
      if ((delete.qualifiers() == null || delete.qualifiers().length < 1 || 
          delete.qualifiers()[0].length < 1) && (delete.family() == null || 
          delete.family().length < 1)) {
        storage.remove(delete.key());
        return Deferred.fromResult(new Object());
      }
      
      final byte[] family = delete.family();
      if (family != null && family.length > 0) {
        if (!row.containsKey(family)) {
          return Deferred.fromResult(null);
        }
      }
      
      // compile a set of qualifiers to use as a filter if necessary
      Bytes.ByteMap<Object> qualifiers = new Bytes.ByteMap<Object>();
      if (delete.qualifiers() != null || delete.qualifiers().length > 0) { 
        for (byte[] q : delete.qualifiers()) {
          qualifiers.put(q, null);
        }
      }
      
      // if the request only has a column family and no qualifiers, we delete
      // the entire family
      if (family != null && qualifiers.isEmpty()) {
        row.remove(family);
        if (row.isEmpty()) {
          storage.remove(delete.key());
        }
        return Deferred.fromResult(new Object());
      }
      
      List<byte[]> cf_removals = new ArrayList<byte[]>(row.entrySet().size());
      for (Map.Entry<byte[], Bytes.ByteMap<TreeMap<Long, byte[]>>> cf : 
        row.entrySet()) {
        
        // column family filter
        if (family != null && family.length > 0 && 
            !Bytes.equals(family, cf.getKey())) {
          continue;
        }
        
        for (byte[] qualifier : qualifiers.keySet()) {
          final TreeMap<Long, byte[]> column = cf.getValue().get(qualifier);
          if (column == null) {
            continue;
          }
          
          // with this flag we delete a single timestamp
          if (delete.deleteAtTimestampOnly()) {
            if (column != null) {
              column.remove(delete.timestamp());
              if (column.isEmpty()) {
                cf.getValue().remove(qualifier);
              }
            }
          } else {
            // otherwise we delete everything less than or equal to the 
            // delete timestamp
            List<Long> column_removals = new ArrayList<Long>(column.size());
            for (Map.Entry<Long, byte[]> cell : column.entrySet()) {
              if (cell.getKey() <= delete.timestamp()) {
                column_removals.add(cell.getKey());
              }
            }
            for (Long ts : column_removals) {
              column.remove(ts);
            }
            if (column.isEmpty()) {
              cf.getValue().remove(qualifier);
            }
          }
        }
        
        if (cf.getValue().isEmpty()) {
          cf_removals.add(cf.getKey());
        }
      }
      
      for (byte[] cf : cf_removals) {
        row.remove(cf);
      }
      
      if (row.isEmpty()) {
        storage.remove(delete.key());
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
   * The KeyRegexp can be set and it will run against the hex value of the 
   * row key. In testing it seems to work nicely even with byte patterns.
   */
  private class MockScanner implements 
    Answer<Deferred<ArrayList<ArrayList<KeyValue>>>> {
    
    private byte[] start = null;
    private byte[] stop = null;
    private HashSet<String> scnr_qualifiers = null;
    private byte[] family = null;
    private String regex = null;
    private boolean called;
    
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
          regex = (String)args[0];
          return null;
        }
      }).when(mock_scanner).setKeyRegexp(anyString(), (Charset)any());
      
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          start = (byte[])args[0];
          return null;
        }      
      }).when(mock_scanner).setStartKey((byte[])any());
      
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          stop = (byte[])args[0];
          return null;
        }      
      }).when(mock_scanner).setStopKey((byte[])any());
      
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          family = (byte[])args[0];
          return null;
        }      
      }).when(mock_scanner).setFamily((byte[])any());
      
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
      
      when(mock_scanner.nextRows()).thenAnswer(this);
      
    }
    
    @Override
    public Deferred<ArrayList<ArrayList<KeyValue>>> answer(
        final InvocationOnMock invocation) throws Throwable {
      
      // It's critical to see if this scanner has been processed before, 
      // otherwise the code under test will likely wind up in an infinite loop.
      // If the scanner has been seen before, we return null.
      if (called) {
        return Deferred.fromResult(null);
      }
      called = true;
      
      Pattern pattern = null;
      if (regex != null && !regex.isEmpty()) {
        try {
          pattern = Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
          e.printStackTrace();
        }
      }
      
      // return all matches
      ArrayList<ArrayList<KeyValue>> results = 
        new ArrayList<ArrayList<KeyValue>>();
      for (Map.Entry<byte[], Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>>> row : 
        storage.entrySet()) {
        
        // if it's before the start row, after the end row or doesn't
        // match the given regex, continue on to the next row
        if (start != null && Bytes.memcmp(row.getKey(), start) < 0) {
          continue;
        }
        if (stop != null && Bytes.memcmp(row.getKey(), stop) > 0) {
          continue;
        }
        if (pattern != null) {
          final String from_bytes = new String(row.getKey(), MockBase.ASCII);
          if (!pattern.matcher(from_bytes).find()) {
            continue;
          }
        }
        
        // loop on the column families
        final ArrayList<KeyValue> kvs = 
          new ArrayList<KeyValue>(row.getValue().size());
        for (Map.Entry<byte[], Bytes.ByteMap<TreeMap<Long, byte[]>>> cf : 
          row.getValue().entrySet()) {
          
          // column family filter
          if (family != null && family.length > 0 && 
              !Bytes.equals(family, cf.getKey())) {
            continue;
          }
        
          for (Map.Entry<byte[], TreeMap<Long, byte[]>> column : 
            cf.getValue().entrySet()) {
            
            // if the qualifier isn't in the set, continue
            if (scnr_qualifiers != null && 
                !scnr_qualifiers.contains(bytesToString(column.getKey()))) {
              continue;
            }
            
            KeyValue kv = mock(KeyValue.class);
            when(kv.key()).thenReturn(row.getKey());
            when(kv.value()).thenReturn(column.getValue().firstEntry().getValue());
            when(kv.qualifier()).thenReturn(column.getKey());
            when(kv.timestamp()).thenReturn(column.getValue().firstKey());
            when(kv.family()).thenReturn(cf.getKey());
            when(kv.toString()).thenReturn("[k '" + bytesToString(row.getKey()) + 
                "' q '" + bytesToString(column.getKey()) + "' v '" + 
                bytesToString(column.getValue().firstEntry().getValue()) + "']");
            kvs.add(kv);
          }
        
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
   * Creates or increments (possibly decrements) a Long in the hash table at the
   * given location.
   */
  private class MockAtomicIncrement implements
    Answer<Deferred<Long>> {

    @Override
    public Deferred<Long> answer(InvocationOnMock invocation) throws Throwable {
      final Object[] args = invocation.getArguments();
      final AtomicIncrementRequest air = (AtomicIncrementRequest)args[0];
      final long amount = air.getAmount();
      Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>> row = 
          storage.get(air.key());
      if (row == null) {
        row = new Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>>();
        storage.put(air.key(), row);
      }
      
      Bytes.ByteMap<TreeMap<Long, byte[]>> cf = row.get(air.family());
      if (cf == null) {
        cf = new Bytes.ByteMap<TreeMap<Long, byte[]>>();
        row.put(air.family(), cf);
      }
      
      TreeMap<Long, byte[]> column = cf.get(air.qualifier());
      if (column == null) {
        column = new TreeMap<Long, byte[]>(Collections.reverseOrder());
        cf.put(air.qualifier(), column);
        column.put(current_timestamp++, Bytes.fromLong(amount));
        return Deferred.fromResult(amount);
      }
      
      long incremented_value = Bytes.getLong(column.firstEntry().getValue());
      incremented_value += amount;
      column.put(column.firstKey(), Bytes.fromLong(incremented_value));
      return Deferred.fromResult(incremented_value);
    }
    
  }
}
