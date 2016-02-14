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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.bind.DatatypeConverter;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Pair;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.AppendRequest;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Ignore;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

/**
 * Mock HBase implementation useful in testing calls to and from storage with
 * actual pretend data. The underlying data store is an incredibly ugly nesting
 * of ByteMaps from AsyncHbase so it stores and orders byte arrays similar to 
 * HBase. It supports tables and column families along with timestamps but 
 * doesn't deal with TTLs or other features.
 * <p>
 * By default we configure the "'tsdb', {NAME => 't'}" and 
 * "'tsdb-uid', {NAME => 'id'}, {NAME => 'name'}" tables. If you need more, just
 * add em.
 * 
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
 * <li>AppendRequest</li>
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
  
  /** Gross huh? <table, <cf, <row, <qual, <ts, value>>>>>
   * Why is CF before row? Because we want to throw exceptions if a CF hasn't
   * been "configured"
   */
  private ByteMap<ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>>> 
  storage = new ByteMap<ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>>>();
  private HashSet<MockScanner> scanners = new HashSet<MockScanner>(2);
  
  /** The default family for shortcuts */
  private byte[] default_family;
  
  /** The default table for shortcuts */
  private byte[] default_table;
  
  /** Incremented every time a new value is stored (without a timestamp) */
  private long current_timestamp = 1388534400000L;
  
  /** A list of exceptions that can be thrown when working with a row key */
  private ByteMap<Pair<RuntimeException, Boolean>> exceptions;
  
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

    default_family = "t".getBytes(ASCII);
    default_table = "tsdb".getBytes(ASCII);
    setupDefaultTables();
    
    // replace the "real" field objects with mocks
    Whitebox.setInternalState(tsdb, "client", client);

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
          final byte[] table = (byte[])arg0.getArguments()[0];
          scanners.add(new MockScanner(scanner, table));
          return scanner;
        }
        
      });      

    }
    
    when(client.atomicIncrement((AtomicIncrementRequest)any()))
      .then(new MockAtomicIncrement());
    when(client.bufferAtomicIncrement((AtomicIncrementRequest)any()))
      .then(new MockAtomicIncrement());
    when(client.append((AppendRequest)any())).thenAnswer(new MockAppend());
  }

  /**
   * Add a table with families to the data store. If the table or family
   * exists, it's a no-op. Give real values as we don't check `em.
   * @param table The table to add
   * @param families A list of one or more famlies to add to the table
   */
  public void addTable(final byte[] table, final List<byte[]> families) {
    ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = storage.get(table);
    if (map == null) {
      map = new ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>>();
      storage.put(table, map);
    }
    for (final byte[] family : families) {
      if (!map.containsKey(family)) {
        map.put(family, new ByteMap<ByteMap<TreeMap<Long, byte[]>>>());
      }
    }
  }
  
  /**
   * Pops the table out of the map
   * @param table The table to pop
   * @return True if the table was there, false if it wasn't.
   */
  public boolean deleteTable(final byte[] table) {
    return storage.remove(table) != null;
  }
  
  /** @param family Sets the default family for calls that need it */
  public void setFamily(final byte[] family) {
    default_family = family;
  }
  
  /** @param table Sets the default table for calls that need it */
  public void setDefaultTable(final byte[] table) {
    default_table = table;
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
   * exists, the original value will be overwritten with the new data.
   * Uses the default table and family
   * @param key The row key
   * @param qualifier The qualifier
   * @param value The value to store
   */
  public void addColumn(final byte[] key, final byte[] qualifier, 
      final byte[] value) {
    addColumn(default_table, key, default_family, qualifier, value, 
        current_timestamp++);
  }
  
  /**
   * Add a column to the hash table 
   * The proper row will be created if it doesn't exist. If the column already 
   * exists, the original value will be overwritten with the new data.
   * Uses the default table.
   * @param key The row key
   * @param family The column family to store the value in
   * @param qualifier The qualifier
   * @param value The value to store
   */
  public void addColumn(final byte[] key, final byte[] family, 
      final byte[] qualifier, final byte[] value) {
    addColumn(default_table, key, family, qualifier, value, current_timestamp++);
  }
  
  /**
   * Add a column to the hash table 
   * The proper row will be created if it doesn't exist. If the column already 
   * exists, the original value will be overwritten with the new data
   * @param table The table
   * @param key The row key
   * @param family The column family to store the value in
   * @param qualifier The qualifier
   * @param value The value to store
   */
  public void addColumn(final byte[] table, final byte[] key, final byte[] family, 
      final byte[] qualifier, final byte[] value) {
    addColumn(table, key, family, qualifier, value, current_timestamp++);
  }
  
  /**
   * Add a column to the hash table 
   * The proper row will be created if it doesn't exist. If the column already 
   * exists, the original value will be overwritten with the new data
   * @param table The table
   * @param key The row key
   * @param family The column family to store the value in
   * @param qualifier The qualifier
   * @param value The value to store
   * @param timestamp The timestamp to store
   */
  public void addColumn(final byte[] table, final byte[] key, final byte[] family, 
      final byte[] qualifier, final byte[] value, final long timestamp) {
    // AsyncHBase will throw an NPE if the user tries to write a NULL value
    // so we better do the same. An empty value is ok though, i.e. new byte[] {}
    if (value == null) {
      throw new NullPointerException();
    }
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = storage.get(table);
    if (map == null) {
      throw new RuntimeException(
          "No such table " + Bytes.pretty(table));
    }
    final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(family);
    if (cf == null) {
      throw new RuntimeException(
          "No such CF " + Bytes.pretty(family));
    }
    
    ByteMap<TreeMap<Long, byte[]>> row = cf.get(key);
    if (row == null) {
      row = new ByteMap<TreeMap<Long, byte[]>>();
      cf.put(key, row);
    }

    TreeMap<Long, byte[]> column = row.get(qualifier);
    if (column == null) {
      // remember, most recent at the top!
      column = new TreeMap<Long, byte[]>(Collections.reverseOrder());
      row.put(qualifier, column);
    }
    column.put(timestamp, value);
  }
  
  /**
   * Stores an exception so that any operation on the given key will cause it
   * to be thrown.
   * @param key The key to go pear shaped on
   * @param exception The exception to throw
   */
  public void throwException(final byte[] key, final RuntimeException exception) {
    throwException(key, exception, true);
  }
  
  /**
   * Stores an exception so that any operation on the given key will cause it
   * to be thrown.
   * @param key The key to go pear shaped on
   * @param exception The exception to throw
   * @param as_result Whether or not to return the exception in the deferred
   * result or throw it outright.
   */
  public void throwException(final byte[] key, final RuntimeException exception, 
      final boolean as_result) {
    if (exceptions == null) {
      exceptions = new ByteMap<Pair<RuntimeException, Boolean>>();
    }
    exceptions.put(key, new Pair<RuntimeException, Boolean>(exception, as_result));
  }
  
  /** Removes all exceptions from the exception list */
  public void clearExceptions() {
    exceptions.clear();
  }
  
  /** @return Total number of unique rows in the default table. Returns 0 if the 
   * default table does not exist */
  public int numRows() {
    return numRows(default_table);
  }
  
  /**
   * Total number of rows in the given table. Returns 0 if the table does not exit.
   * @param table The table to scan
   * @return The number of rows
   */
  public int numRows(final byte[] table) {
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
        storage.get(table);
    if (map == null) {
      return 0;
    }
    final ByteMap<Void> unique_rows = new ByteMap<Void>(); 
    for (final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf : map.values()) {
      for (final byte[] key : cf.keySet()) {
        unique_rows.put(key, null);
      }
    }
    return unique_rows.size();
  }
  
  /**
   * Return the total number of column families for the row in the default table
   * @param key The row to search for
   * @return -1 if the table or row did not exist, otherwise the number of 
   * column families.
   */
  public int numColumnFamilies(final byte[] key) {
    return numColumnFamilies(default_table, key);
  }
  
  /**
   * Return the number of column families for the given row key in the given table.
   * @param table The table to iterate over
   * @param key The row to search for 
   * @return -1 if the table or row did not exist, otherwise the number of 
   * column families.
   */
  public int numColumnFamilies(final byte[] table, final byte[] key) {
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
        storage.get(table);
    if (map == null) {
      return -1;
    }
    int sum = 0;
    for (final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf : map.values()) {
      if (cf.containsKey(key)) {
        ++sum;
      }
    }
    return sum == 0 ? -1 : sum;
  }
  
  /**
   * Total number of columns in the given row across all column families in the
   * default table
   * @param key The row to search for
   * @return -1 if the row did not exist, otherwise the number of columns.
   */
  public long numColumns(final byte[] key) {
    return numColumns(default_table, key);
  }
  
  /**
   * Total number of columns in the given row across all column families in the
   * default table
   * @param table The table to iterate over
   * @param key The row to search for
   * @return -1 if the row did not exist, otherwise the number of columns.
   */
  public long numColumns(final byte[] table, final byte[] key) {
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
        storage.get(table);
    if (map == null) {
      return -1;
    }
    long size = 0;
    for (final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf : map.values()) {
      final ByteMap<TreeMap<Long, byte[]>> row = cf.get(key);
      if (row != null) {
        size += row.size();
      }
    }
    return size == 0 ? -1 : size;
  }
  
  /**
   * Return the total number of columns for a specific row and family in the
   * default table
   * @param key The row to search for
   * @param family The column family to search for
   * @return -1 if the row did not exist, otherwise the number of columns.
   */
  public int numColumnsInFamily(final byte[] key, final byte[] family) {
    return numColumnsInFamily(default_table, key, family);
  }
  
  /**
   * Return the total number of columns for a specific row and family
   * @param key The row to search for
   * @param family The column family to search for
   * @return -1 if the row did not exist, otherwise the number of columns.
   */
  public int numColumnsInFamily(final byte[] table, final byte[] key, 
      final byte[] family) {
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
        storage.get(table);
    if (map == null) {
      return -1;
    }
    final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(family);
    if (cf == null) {
      return -1;
    }
    return cf.size();
  }

  /**
   * Retrieve the most recent contents of a single column with the default family
   * and in the default table
   * @param key The row key of the column
   * @param qualifier The column qualifier
   * @return The byte array of data or null if not found
   */
  public byte[] getColumn(final byte[] key, final byte[] qualifier) {
    return getColumn(default_table, key, default_family, qualifier);
  }
  
  /**
   * Retrieve the most recent contents of a single column with the default table
   * @param key The row key of the column
   * @param family The column family
   * @param qualifier The column qualifier
   * @return The byte array of data or null if not found
   */
  public byte[] getColumn(final byte[] key, final byte[] family, 
      final byte[] qualifier) {
    return getColumn(default_table, key, family, qualifier);
  }
  
  /**
   * Retrieve the most recent contents of a single column
   * @param table The table to fetch from
   * @param key The row key of the column
   * @param family The column family
   * @param qualifier The column qualifier
   * @return The byte array of data or null if not found
   */
  public byte[] getColumn(final byte[] table, final byte[] key, 
      final byte[] family, final byte[] qualifier) {
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
        storage.get(table);
    if (map == null) {
      return null;
    }
    final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(family);
    if (cf == null) {
      return null;
    }
    final ByteMap<TreeMap<Long, byte[]>> row = cf.get(key);
    if (row == null) {
      return null;
    }
    final TreeMap<Long, byte[]> column = row.get(qualifier);
    if (column == null) {
      return null;
    }
    return column.firstEntry().getValue();
  }

  /**
   * Retrieve the full map of timestamps and values of a single column with 
   * the default family and default table
   * @param key The row key of the column
   * @param qualifier The column qualifier
   * @return The byte array of data or null if not found
   */
  public TreeMap<Long, byte[]> getFullColumn(final byte[] key, 
      final byte[] qualifier) {
    return getFullColumn(default_table, key, default_family, qualifier);
  }
  
  /**
   * Retrieve the full map of timestamps and values of a single column
   * @param table The table to fetch from
   * @param key The row key of the column
   * @param family The column family
   * @param qualifier The column qualifier
   * @return The tree map of timestamps and values or null if not found
   */
  public TreeMap<Long, byte[]> getFullColumn(final byte[] table, final byte[] key, 
      final byte[] family, final byte[] qualifier) {
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
        storage.get(table);
    if (map == null) {
      return null;
    }
    final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(family);
    if (cf == null) {
      return null;
    }
    final ByteMap<TreeMap<Long, byte[]>> row = cf.get(key);
    if (row == null) {
      return null;
    }
    return row.get(qualifier);
  }
  
  /**
   * Returns the most recent value from all columns for a given column family
   * in the default table
   * @param key The row key
   * @param family The column family ID
   * @return A map of columns if the CF was found, null if no such CF
   */
  public ByteMap<byte[]> getColumnFamily(final byte[] key, 
      final byte[] family) {
    return getColumnFamily(default_table, key , family);
  }
  
  /**
   * Returns the most recent value from all columns for a given column family
   * @param table The table to fetch from
   * @param key The row key
   * @param family The column family ID
   * @return A map of columns if the CF was found, null if no such CF
   */
  public ByteMap<byte[]> getColumnFamily(final byte[] table, final byte[] key, 
      final byte[] family) {
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
        storage.get(table);
    if (map == null) {
      return null;
    }
    final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(family);
    if (cf == null) {
      return null;
    }
    final ByteMap<TreeMap<Long, byte[]>> row = cf.get(key);
    if (row == null) {
      return null;
    }
    // convert to a <qualifier, value> byte map
    final ByteMap<byte[]> columns = new ByteMap<byte[]>();
    for (Entry<byte[], TreeMap<Long, byte[]>> entry : row.entrySet()) {
      // the <timestamp, value> map should never be null
      columns.put(entry.getKey(), entry.getValue().firstEntry().getValue());
    }
    return columns;
  }
  
  /** @return the list of keys stored in the default table for all CFs */
  public Set<byte[]> getKeys() {
    return getKeys(default_table);
  }
  
  /**
   * Return the list of unique keys in the given table for all CFs
   * @param table The table to pull from
   * @return A list of keys. May be null if the table doesn't exist
   */
  public Set<byte[]> getKeys(final byte[] table) {
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
        storage.get(table);
    if (map == null) {
      return null;
    }
    final ByteMap<Void> unique_rows = new ByteMap<Void>(); 
    for (final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf : map.values()) {
      for (final byte[] key : cf.keySet()) {
        unique_rows.put(key, null);
      }
    }
    return unique_rows.keySet();
  }
  
  /**
   * Return the mocked TSDB object to use for HBaseClient access
   * @return
   */
  public TSDB getTSDB() {
    return tsdb;
  }
  
  /**
   * Runs through all rows in the "tsdb" table and compacts them by making a 
   * call to the {@link TSDB.compact} method. It will delete any columns 
   * that were compacted and leave others untouched, just as the normal 
   * method does.
   * And only iterates over the 't' family.
   * @throws Exception if Whitebox couldn't access the compact method
   */
  public void tsdbCompactAllRows() throws Exception {
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
        storage.get("tsdb".getBytes(ASCII));
    if (map == null) {
      return;
    }
    final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get("t".getBytes(ASCII));
    if (cf == null) {
      return;
    }
    
    for (Entry<byte[], ByteMap<TreeMap<Long, byte[]>>> entry : cf.entrySet()) {
      final byte[] key = entry.getKey();
      
      final ByteMap<TreeMap<Long, byte[]>> row = entry.getValue();
      ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(row.size());
      final Set<byte[]> deletes = new HashSet<byte[]>();
      for (Map.Entry<byte[], TreeMap<Long, byte[]>> column : row.entrySet()) {
        if (column.getKey().length % 2 == 0) {
          kvs.add(new KeyValue(key, default_family, column.getKey(), 
              column.getValue().firstKey(),
              column.getValue().firstEntry().getValue()));
          deletes.add(column.getKey());
        }
      }
      if (kvs.size() > 0) {
        for (final byte[] k : deletes) {
          row.remove(k);
        }
        final KeyValue compacted = 
            Whitebox.invokeMethod(tsdb, "compact", kvs, Collections.EMPTY_LIST);
        final TreeMap<Long, byte[]> compacted_value = new TreeMap<Long, byte[]>();
        compacted_value.put(current_timestamp++, compacted.value());
        row.put(compacted.qualifier(), compacted_value);
      }
    }
  }
  
  /**
   * Clears out all rows from storage but doesn't delete the tables or families.
   */
  public void flushStorage() {
    for (final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> table : 
      storage.values()) {
      for (final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf : table.values()) {
        cf.clear();
      }
    }
  }

  /**
   * Clears out all rows for a given table
   * @param table The table to empty out
   */
  public void flushStorage(final byte[] table) {
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = storage.get(table);
    if (map == null) {
      return;
    }
    for (final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf : map.values()) {
      cf.clear();
    }
  }
  
  /**
   * Removes the entire row from the default table for all column families
   * @param key The row to remove
   */
  public void flushRow(final byte[] key) {
    flushRow(default_table, key);
  }
  
  /**
   * Removes the entire row from the table for all column families
   * @param table The table to purge
   * @param key The row to remove
   */
  public void flushRow(final byte[] table, final byte[] key) {
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = storage.get(table);
    if (map == null) {
      return;
    }
    for (final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf : map.values()) {
      cf.remove(key);
    }
  }
  
  /**
   * Removes all rows from the default table for the given column family
   * @param family The family to remove
   */
  public void flushFamily(final byte[] family) {
    flushFamily(default_table, family);
  }
  
  /**
   * Removes all rows from the default table for the given column family
   * @param table The table to purge from
   * @param family The family to remove
   */
  public void flushFamily(final byte[] table, final byte[] family) {
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = storage.get(table);
    if (map == null) {
      return;
    }
    final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(family);
    if (cf != null) {
      cf.clear();
    }
  }
  
  /**
   * Removes the given column from the default table 
   * @param key Row key
   * @param family Column family
   * @param qualifier Column qualifier
   */
  public void flushColumn(final byte[] key, final byte[] family, 
      final byte[] qualifier) {
    flushColumn(default_table, key, family, qualifier);
  }
  
  /**
   * Removes the given column from the table
   * @param table The table to purge from
   * @param key Row key
   * @param family Column family
   * @param qualifier Column qualifier
   */
  public void flushColumn(final byte[] table, final byte[] key, 
      final byte[] family, final byte[] qualifier) {
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = storage.get(table);
    if (map == null) {
      return;
    }
    final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(family);
    if (cf == null) {
      return;
    }
    final ByteMap<TreeMap<Long, byte[]>> row = cf.get(key);
    if (row == null) {
      return;
    }
    row.remove(qualifier);
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
    
    for (Entry<byte[], ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>>> table : 
      storage.entrySet()) {
      System.out.println("[Table] " + new String(table.getKey(), ASCII));
      
      for (Entry<byte[], ByteMap<ByteMap<TreeMap<Long, byte[]>>>> cf : 
        table.getValue().entrySet()) {
        System.out.println("  [CF] " + new String(cf.getKey(), ASCII));

        for (Entry<byte[], ByteMap<TreeMap<Long, byte[]>>> row : 
          cf.getValue().entrySet()) {
          System.out.println("    [Row] " + (ascii ? 
              new String(row.getKey(), ASCII) : bytesToString(row.getKey())));
          
          for (Map.Entry<byte[], TreeMap<Long, byte[]>> column : row.getValue().entrySet()) {
            System.out.println("      [Qual] " + (ascii ?
                "\"" + new String(column.getKey(), ASCII) + "\""
                : bytesToString(column.getKey())));
            for (Map.Entry<Long, byte[]> cell : column.getValue().entrySet()) {
              System.out.println("        [TS] " + cell.getKey() + "  [Value] " + 
                  (ascii ?  new String(cell.getValue(), ASCII) 
                  : bytesToString(cell.getValue())));
            }
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
  
  /** Creates the TSDB and UID tables */
  private void setupDefaultTables() {
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> tsdb =
        new ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>>();
    tsdb.put("t".getBytes(ASCII), new ByteMap<ByteMap<TreeMap<Long, byte[]>>>());
    storage.put("tsdb".getBytes(ASCII), tsdb);
    
    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> tsdb_uid =
        new ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>>();
    tsdb_uid.put("name".getBytes(ASCII), 
        new ByteMap<ByteMap<TreeMap<Long, byte[]>>>());
    tsdb_uid.put("id".getBytes(ASCII), 
        new ByteMap<ByteMap<TreeMap<Long, byte[]>>>());
    storage.put("tsdb-uid".getBytes(ASCII), tsdb_uid);
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
      
      if (exceptions != null) {
        final Pair<RuntimeException, Boolean> ex = exceptions.get(get.key());
        if (ex != null) {
          if (ex.getValue()) {
            return Deferred.fromError(ex.getKey());
          } else {
            throw ex.getKey();
          }
        }
      }
      
      final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
          storage.get(get.table());
      if (map == null) {
        return Deferred.fromError(new RuntimeException(
          "No such table " + Bytes.pretty(get.table())));
      }
      
      // compile a set of qualifiers to use as a filter if necessary
      final ByteMap<Object> qualifiers = new ByteMap<Object>();
      if (get.qualifiers() != null && get.qualifiers().length > 0) { 
        for (byte[] q : get.qualifiers()) {
          qualifiers.put(q, null);
        }
      }
      
      final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>();
      for (final Entry<byte[], ByteMap<ByteMap<TreeMap<Long, byte[]>>>> cf : 
          map.entrySet()) {
        if (get.family() != null && Bytes.memcmp(get.family(), cf.getKey()) != 0) {
          continue;
        }
        
        final ByteMap<TreeMap<Long, byte[]>> row = cf.getValue().get(get.key());
        if (row == null) {
          continue;
        }
        
        for (Entry<byte[], TreeMap<Long, byte[]>> column : row.entrySet()) {
          if (!qualifiers.isEmpty() && !qualifiers.containsKey(column.getKey())) {
            continue;
          }
          
          // TODO - if we want to support multiple values, iterate over the 
          // tree map. Otherwise Get returns just the latest value.
          kvs.add(new KeyValue(get.key(), cf.getKey(), column.getKey(),
              column.getValue().firstKey(),
              column.getValue().firstEntry().getValue()));
        }
      }
      if (kvs.isEmpty()) {
        return Deferred.fromResult(null);
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
      
      if (exceptions != null) {
        final Pair<RuntimeException, Boolean> ex = exceptions.get(put.key());
        if (ex != null) {
          if (ex.getValue()) {
            return Deferred.fromError(ex.getKey());
          } else {
            throw ex.getKey();
          }
        }
      }
      
      final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
          storage.get(put.table());
      if (map == null) {
        return Deferred.fromError(new RuntimeException(
          "No such table " + Bytes.pretty(put.table())));
      }
      
      final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(put.family());
      if (cf == null) {
        return Deferred.fromError(new RuntimeException(
            "No such CF " + Bytes.pretty(put.table()))); 
      }
      
      ByteMap<TreeMap<Long, byte[]>> row = cf.get(put.key());
      if (row == null) {
        row = new ByteMap<TreeMap<Long, byte[]>>();
        cf.put(put.key(), row);
      }

      for (int i = 0; i < put.qualifiers().length; i++) {
        TreeMap<Long, byte[]> column = row.get(put.qualifiers()[i]);
        if (column == null) {
          column = new TreeMap<Long, byte[]>(Collections.reverseOrder());
          row.put(put.qualifiers()[i], column);
        }
        
        column.put(put.timestamp() != Long.MAX_VALUE ? put.timestamp() : 
          current_timestamp++, put.values()[i]);
      }
      
      return Deferred.fromResult(true);
    }
  }
  
  /**
   * Stores one or more columns in a row. If the row does not exist, it's
   * created.
   */
  private class MockAppend implements Answer<Deferred<Boolean>> {
    @Override
    public Deferred<Boolean> answer(final InvocationOnMock invocation) 
      throws Throwable {
      final Object[] args = invocation.getArguments();
      final AppendRequest append = (AppendRequest)args[0];

      if (exceptions != null) {
        final Pair<RuntimeException, Boolean> ex = exceptions.get(append.key());
        if (ex != null) {
          if (ex.getValue()) {
            return Deferred.fromError(ex.getKey());
          } else {
            throw ex.getKey();
          }
        }
      }
      
      final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
          storage.get(append.table());
      if (map == null) {
        return Deferred.fromError(new RuntimeException(
          "No such table " + Bytes.pretty(append.table())));
      }
      
      final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(append.family());
      if (cf == null) {
        return Deferred.fromError(new RuntimeException(
            "No such CF " + Bytes.pretty(append.table()))); 
      }
      
      ByteMap<TreeMap<Long, byte[]>> row = cf.get(append.key());
      if (row == null) {
        row = new ByteMap<TreeMap<Long, byte[]>>();
        cf.put(append.key(), row);
      }
      
      for (int i = 0; i < append.qualifiers().length; i++) {
        TreeMap<Long, byte[]> column = row.get(append.qualifiers()[i]);
        if (column == null) {
          column = new TreeMap<Long, byte[]>(Collections.reverseOrder());
          row.put(append.qualifiers()[i], column);
        }
        
        final byte[] values;
        long column_timestamp = 0;
        if (append.timestamp() != Long.MAX_VALUE) {
          values = column.get(append.timestamp());
          column_timestamp = append.timestamp();
        } else {
          if (column.isEmpty()) {
            values = null;
          } else {
            values = column.firstEntry().getValue();
            column_timestamp = column.firstKey();
          }
        }
        if (column_timestamp == 0) {
          column_timestamp = current_timestamp++;
        }

        final int current_len = values != null ? values.length : 0;
        final byte[] append_value = new byte[current_len + append.values()[i].length];
        if (current_len > 0) {
          System.arraycopy(values, 0, append_value, 0, values.length);
        }
        
        System.arraycopy(append.value(), 0, append_value, current_len, 
            append.values()[i].length);
        column.put(column_timestamp, append_value);
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
      
      if (exceptions != null) {
        final Pair<RuntimeException, Boolean> ex = exceptions.get(put.key());
        if (ex != null) {
          if (ex.getValue()) {
            return Deferred.fromError(ex.getKey());
          } else {
            throw ex.getKey();
          }
        }
      }
      
      final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
          storage.get(put.table());
      if (map == null) {
        return Deferred.fromError(new RuntimeException(
          "No such table " + Bytes.pretty(put.table())));
      }
      
      final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(put.family());
      if (cf == null) {
        return Deferred.fromError(new RuntimeException(
            "No such CF " + Bytes.pretty(put.table()))); 
      }
      
      ByteMap<TreeMap<Long, byte[]>> row = cf.get(put.key());
      if (row == null) {
        if (expected != null && expected.length > 0) {
          return Deferred.fromResult(false);
        }
        row = new ByteMap<TreeMap<Long, byte[]>>();
        cf.put(put.key(), row);
      }
      
      // CAS can only operate on one cell, so if the put request has more than 
      // one, we ignore any but the first
      TreeMap<Long, byte[]> column = row.get(put.qualifiers()[0]);
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
        row.put(put.qualifiers()[0], column);
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
      
      if (exceptions != null) {
        final Pair<RuntimeException, Boolean> ex = exceptions.get(delete.key());
        if (ex != null) {
          if (ex.getValue()) {
            return Deferred.fromError(ex.getKey());
          } else {
            throw ex.getKey();
          }
        }
      }
      
      final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
          storage.get(delete.table());
      if (map == null) {
        return Deferred.fromError(new RuntimeException(
          "No such table " + Bytes.pretty(delete.table())));
      }
      
      // if no qualifiers or family, then delete the row from all families
      if ((delete.qualifiers() == null || delete.qualifiers().length < 1 || 
          delete.qualifiers()[0].length < 1) && (delete.family() == null || 
          delete.family().length < 1)) {
        for (final Entry<byte[], ByteMap<ByteMap<TreeMap<Long, byte[]>>>> cf : 
          map.entrySet()) {
          cf.getValue().remove(delete.key());
        }
        return Deferred.fromResult(new Object());
      }
      
      final byte[] family = delete.family();
      if (family != null && family.length > 0) {
        if (!map.containsKey(family)) {
          return Deferred.fromError(new RuntimeException(
              "No such CF " + Bytes.pretty(family))); 
        }
      }
      
      // compile a set of qualifiers
      ByteMap<Object> qualifiers = new ByteMap<Object>();
      if (delete.qualifiers() != null || delete.qualifiers().length > 0) { 
        for (byte[] q : delete.qualifiers()) {
          qualifiers.put(q, null);
        }
      }
      
      // TODO - validate the assumption that a delete with a row key and qual
      // but without a family would delete the columns in ALL families
      
      // if the request only has a column family and no qualifiers, we delete
      // the row from the entire family
      if (family != null && qualifiers.isEmpty()) {
        final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(delete.family());
        // cf != null validated above
        cf.remove(delete.key());
        return Deferred.fromResult(new Object());
      }
      
      for (final Entry<byte[], ByteMap<ByteMap<TreeMap<Long, byte[]>>>> cf : 
        map.entrySet()) {
        
        // column family filter
        if (family != null && family.length > 0 && 
            !Bytes.equals(family, cf.getKey())) {
          continue;
        }
        
        ByteMap<TreeMap<Long, byte[]>> row = cf.getValue().get(delete.key());
        if (row == null) {
          continue;
        }
        
        for (byte[] qualifier : qualifiers.keySet()) {
          final TreeMap<Long, byte[]> column = row.get(qualifier);
          if (column == null) {
            continue;
          }
          
          // with this flag we delete a single timestamp
          if (delete.deleteAtTimestampOnly()) {
            if (column != null) {
              column.remove(delete.timestamp());
              if (column.isEmpty()) {
                row.remove(qualifier);
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
              row.remove(qualifier);
            }
          }
        }
        
        if (row.isEmpty()) {
          cf.getValue().remove(delete.key());
        }
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

    private final byte[] table;
    private byte[] start = null;
    private byte[] stop = null;
    private HashSet<String> scnr_qualifiers = null;
    private byte[] family = null;
    private String regex = null;
    private int max_num_rows = Scanner.DEFAULT_MAX_NUM_ROWS;
    private ByteMap<Iterator<Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>>> 
      cursors;
    private ByteMap<Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>> cf_rows;
    private byte[] last_row;
    
    /**
     * Default ctor
     * @param mock_scanner The scanner we're using
     * @param table The table (confirmed to exist)
     */
    public MockScanner(final Scanner mock_scanner, final byte[] table) {
      this.table = table;

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
      
      if (cursors == null) {
        final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
            storage.get(table);
        if (map == null) {
          return Deferred.fromError( new RuntimeException(
              "No such table " + Bytes.pretty(table)));
        }
        
        cursors = new ByteMap<Iterator<Entry<byte[], 
            ByteMap<TreeMap<Long, byte[]>>>>>();
        cf_rows = new ByteMap<Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>>();
        
        if (family == null || family.length < 1) {
          for (final Entry<byte[], ByteMap<ByteMap<TreeMap<Long, byte[]>>>> cf : map) {
            final Iterator<Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>> 
            cursor = cf.getValue().iterator();
            cursors.put(cf.getKey(), cursor);
            cf_rows.put(cf.getKey(), null);
          }
        } else {
          final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(family);
          if (cf == null) {
            return Deferred.fromError(new RuntimeException(
            "No such CF " + Bytes.pretty(family)));
          }
          final Iterator<Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>> 
            cursor = cf.iterator();
          cursors.put(family, cursor);
          cf_rows.put(family, null);
        }
      }

      // If we're out of rows to scan, then you HAVE to return null as the 
      // HBase client does.
      if (!hasNext()) {
        return Deferred.fromResult(null);
      }
      
      Pattern pattern = null;
      if (regex != null && !regex.isEmpty()) {
        try {
          pattern = Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
          e.printStackTrace();
        }
      }
      
      // return all matches
      final ArrayList<ArrayList<KeyValue>> results = 
        new ArrayList<ArrayList<KeyValue>>();
      int rows_read = 0;
      while (hasNext()) {
        advance();
        
        // if it's before the start row, after the end row or doesn't
        // match the given regex, continue on to the next row
        if (start != null && Bytes.memcmp(last_row, start) < 0) {
          continue;
        }
        // asynchbase Scanner's logic:
        // - start_key is inclusive, stop key is exclusive
        // - when start key is equal to the stop key, 
        //   include the key in scan result
        // - if stop key is empty, scan till the end
        if (stop != null && stop.length > 0 && 
            Bytes.memcmp(last_row, stop) >= 0 && 
            Bytes.memcmp(start, stop) != 0) {
          continue;
        }
        if (pattern != null) {
          final String from_bytes = new String(last_row, MockBase.ASCII);
          if (!pattern.matcher(from_bytes).find()) {
            continue;
          }
        }
        
        // throws AFTER we match on a row key
        if (exceptions != null) {
          final Pair<RuntimeException, Boolean> ex = exceptions.get(last_row);
          if (ex != null) {
            if (ex.getValue()) {
              return Deferred.fromError(ex.getKey());
            } else {
              throw ex.getKey();
            }
          }
        }

        // loop over the column family rows to see if they match
        final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>();
        for (final Entry<byte[], Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>> row : 
          cf_rows.entrySet()) {
          if (row.getValue() == null || 
              Bytes.memcmp(last_row, row.getValue().getKey()) != 0) {
            continue;
          }
          
          for (final Entry<byte[], TreeMap<Long, byte[]>> column : 
            row.getValue().getValue().entrySet()) {
            // if the qualifier isn't in the set, continue
            if (scnr_qualifiers != null && 
                !scnr_qualifiers.contains(bytesToString(column.getKey()))) {
              continue;
            }
            
            kvs.add(new KeyValue(row.getValue().getKey(), row.getKey(), 
                column.getKey(), column.getValue().firstKey(),
                column.getValue().firstEntry().getValue()));
          }
        }
        
        if (!kvs.isEmpty()) {
          results.add(kvs);
        }
        rows_read++;
        
        if (rows_read >= max_num_rows) {
          Thread.sleep(10); // this is here for time based unit tests
          break;
        }
      }
      
      if (results.isEmpty()) {
        return Deferred.fromResult(null);
      }
      return Deferred.fromResult(results);
    }
    
    /** @return Returns true if any of the CF iterators have another value */
    private boolean hasNext() {
      for (final Iterator<Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>> cursor : 
          cursors.values()) {
        if (cursor.hasNext()) {
          return true;
        }
      }
      return false;
    }
  
    /** Insanely inefficient and ugly way of advancing the cursors */
    private void advance() {
      // first time to get the ceiling
      if (last_row == null) {
        for (final Entry<byte[], 
            Iterator<Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>>> iterator : 
          cursors.entrySet()) {
          final Entry<byte[], ByteMap<TreeMap<Long, byte[]>>> row = 
              iterator.getValue().hasNext() ? iterator.getValue().next() : null;
          cf_rows.put(iterator.getKey(), row);
          if (last_row == null) {
            last_row = row.getKey();
          } else {
            if (Bytes.memcmp(last_row, row.getKey()) < 0) {
              last_row = row.getKey();
            }
          }
        }
        return;
      }
      
      for (final Entry<byte[], Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>> cf : 
        cf_rows.entrySet()) {
        final Entry<byte[], ByteMap<TreeMap<Long, byte[]>>> row = cf.getValue();
        if (row == null) {
          continue;
        }
        
        if (Bytes.memcmp(last_row, row.getKey()) == 0) {
          if (!cursors.get(cf.getKey()).hasNext()) {
            cf_rows.put(cf.getKey(), null); // EX?
          } else {
            cf_rows.put(cf.getKey(), cursors.get(cf.getKey()).next());
          }
        }
      }
      
      last_row = null;
      for (final Entry<byte[], ByteMap<TreeMap<Long, byte[]>>> row : 
        cf_rows.values()) {
        if (row == null) {
          continue;
        }
        
        if (last_row == null) {
          last_row = row.getKey();
        } else {
          if (Bytes.memcmp(last_row, row.getKey()) < 0) {
            last_row = row.getKey();
          }
        }
      }
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
      
      if (exceptions != null) {
        final Pair<RuntimeException, Boolean> ex = exceptions.get(air.key());
        if (ex != null) {
          if (ex.getValue()) {
            return Deferred.fromError(ex.getKey());
          } else {
            throw ex.getKey();
          }
        }
      }
      
      final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map = 
          storage.get(air.table());
      if (map == null) {
        return Deferred.fromError(new RuntimeException(
          "No such table " + Bytes.pretty(air.table())));
      }
      
      final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(air.family());
      if (cf == null) {
        return Deferred.fromError(new RuntimeException(
            "No such CF " + Bytes.pretty(air.table()))); 
      }
      
      ByteMap<TreeMap<Long, byte[]>> row = cf.get(air.key());
      if (row == null) {
        row = new ByteMap<TreeMap<Long, byte[]>>();
        cf.put(air.key(), row);
      }
      
      TreeMap<Long, byte[]> column = row.get(air.qualifier());
      if (column == null) {
        column = new TreeMap<Long, byte[]>(Collections.reverseOrder());
        row.put(air.qualifier(), column);
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
