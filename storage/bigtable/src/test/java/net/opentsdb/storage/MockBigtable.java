// This file is part of OpenTSDB.
// Copyright (C) 2013-2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.storage;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.bind.DatatypeConverter;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Bytes.ByteMap;
import net.opentsdb.utils.Pair;

import org.junit.Ignore;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.reflect.Whitebox;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.ReadModifyWriteRule;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

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
public final class MockBigtable {
  public static final byte[] DATA_TABLE = 
      "projects/MyProject/instances/MyInstance/tables/tsdb"
        .getBytes(Const.ISO_8859_CHARSET);
  public static final byte[] UID_TABLE = 
      "projects/MyProject/instances/MyInstance/tables/tsdb-uid"
        .getBytes(Const.ISO_8859_CHARSET);
  
  
  /** The TSD mock */
  private TSDB tsdb;

  /** Gross huh? <table, <cf, <row, <qual, <ts, value>>>>>
   * Why is CF before row? Because we want to throw exceptions if a CF hasn't
   * been "configured"
   */
  private ByteMap<ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>>>
  storage = new ByteMap<ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>>>();

  /** The scanners caught by this instance. */
  private List<MockScanner> scanners = Lists.newArrayList();
  
  /** The multi-gets caught by this instance. */
  private List<ReadRowsRequest> multi_gets = Lists.newArrayList();

  /** The default family for shortcuts */
  private byte[] default_family;

  /** The default table for shortcuts */
  private byte[] default_table;

  /** Incremented every time a new value is stored (without a timestamp) */
  private long current_timestamp = 1388534400000L;

  /** A list of exceptions that can be thrown when working with a row key */
  private ByteMap<Pair<RuntimeException, Boolean>> exceptions;

  public MockBigtable(final BigtableSession session,
                      final AsyncExecutor executor,
                      final BigtableDataClient client,
                      final BulkMutation bulk_mutator) {

    default_family = "t".getBytes(Const.ASCII_US_CHARSET);
    default_table = DATA_TABLE;
    setupDefaultTables();

    try {
      when(executor.readRowsAsync(any(ReadRowsRequest.class)))
        .thenAnswer(new Answer<ListenableFuture<List<Row>>>() {
          @Override
          public ListenableFuture<List<Row>> answer(InvocationOnMock invocation)
              throws Throwable {
            return new MockGet((ReadRowsRequest) invocation.getArguments()[0]);
          }
        });
    } catch (InterruptedException e1) {
      throw new RuntimeException("WTF?", e1);
    }
    
    // Default put answer will store the given values in the proper location.
    when(bulk_mutator.add(any(MutateRowRequest.class)))
      .thenAnswer(new Answer<ListenableFuture<MutateRowResponse>>() {
        @Override
        public ListenableFuture<MutateRowResponse> answer(
            InvocationOnMock invocation) throws Throwable {
          return new MockPut((MutateRowRequest) invocation.getArguments()[0]);
        }
      });

//    if (default_delete) {
//      when(client.delete((DeleteRequest)any())).thenAnswer(new MockDelete());
//    }
    when(client.readFlatRows(any(ReadRowsRequest.class)))
      .thenAnswer(new Answer<ResultScanner>() {
          @Override
          public ResultScanner answer(final InvocationOnMock invocation)
              throws Throwable {
            return spy(new MockScanner((ReadRowsRequest) 
                invocation.getArguments()[0]));
          }
      });
    
    try {
      when(executor.readModifyWriteRowAsync(any(ReadModifyWriteRowRequest.class)))
        .thenAnswer(new Answer<ListenableFuture<ReadModifyWriteRowResponse>>() {
          @Override
          public ListenableFuture<ReadModifyWriteRowResponse> answer(
              InvocationOnMock invocation) throws Throwable {
            return new MockAppendAndIncrement(
                (ReadModifyWriteRowRequest) invocation.getArguments()[0]);
          }
        });
    } catch (InterruptedException e) {
      throw new RuntimeException("WTF?", e);
    }
    
    try {
      when(executor.checkAndMutateRowAsync(any(CheckAndMutateRowRequest.class)))
        .thenAnswer(new Answer<ListenableFuture<CheckAndMutateRowResponse>>() {
          @Override
          public ListenableFuture<CheckAndMutateRowResponse> answer(
              InvocationOnMock invocation) throws Throwable {
            return new MockCAS((CheckAndMutateRowRequest) 
                invocation.getArguments()[0]);
          }
        });
    } catch (InterruptedException e) {
      throw new RuntimeException("WTF?", e);
    }
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
   * @param timestamp The timestamp of cell
   */
  public void addColumn(final byte[] key, final byte[] qualifier,
      final byte[] value, final long timestamp) {
    addColumn(default_table, key, default_family, qualifier, value,
        timestamp);
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
    if (column == null || column.isEmpty()) {
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

  /** @return The set of scanners configured by the caller */
  public List<MockScanner> getScanners() {
    return scanners;
  }
  
  /** @return The last scanner created. */
  public MockScanner getLastScanner() {
    return scanners.isEmpty() ? null : scanners.get(scanners.size() - 1);
  }

  public void clearScanners() {
    scanners.clear();
  }
  
  /** @return The list of multi-gets sent to this mock client. */
  public List<ReadRowsRequest> getMultiGets() {
    return multi_gets;
  }
  
  /** @return The last set of get requests sent to this mock client. */
  public ReadRowsRequest getLastMultiGets() {
    return multi_gets.get(multi_gets.size() - 1);
  }
  
  public void clearMultiGets() {
    multi_gets.clear();
  }
  
  /**
   * Return the mocked TSDB object to use for HBaseClient access
   * @return
   */
  public TSDB getTSDB() {
    return tsdb;
  }

//  /**
//   * Runs through all rows in the "tsdb" table and compacts them by making a
//   * call to the {@link TSDB.compact} method. It will delete any columns
//   * that were compacted and leave others untouched, just as the normal
//   * method does.
//   * And only iterates over the 't' family.
//   * @throws Exception if Whitebox couldn't access the compact method
//   */
//  public void tsdbCompactAllRows() throws Exception {
//    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map =
//        storage.get("tsdb".getBytes(Const.ASCII_US_CHARSET));
//    if (map == null) {
//      return;
//    }
//    final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get("t".getBytes(Const.ASCII_US_CHARSET));
//    if (cf == null) {
//      return;
//    }
//
//    for (Entry<byte[], ByteMap<TreeMap<Long, byte[]>>> entry : cf.entrySet()) {
//      final byte[] key = entry.getKey();
//
//      final ByteMap<TreeMap<Long, byte[]>> row = entry.getValue();
//      ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(row.size());
//      final Set<byte[]> deletes = new HashSet<byte[]>();
//      for (Map.Entry<byte[], TreeMap<Long, byte[]>> column : row.entrySet()) {
//        if (column.getKey().length % 2 == 0) {
//          kvs.add(new KeyValue(key, default_family, column.getKey(),
//              column.getValue().firstKey(),
//              column.getValue().firstEntry().getValue()));
//          deletes.add(column.getKey());
//        }
//      }
//      if (kvs.size() > 0) {
//        for (final byte[] k : deletes) {
//          row.remove(k);
//        }
//        final KeyValue compacted =
//            Whitebox.invokeMethod(tsdb, "compact", kvs, Collections.EMPTY_LIST,
//                Collections.EMPTY_LIST);
//        final TreeMap<Long, byte[]> compacted_value = new TreeMap<Long, byte[]>();
//        compacted_value.put(current_timestamp++, compacted.value());
//        row.put(compacted.qualifier(), compacted_value);
//      }
//    }
//  }

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
    dumpToSystemOut(null, false);
  }

  /**
   * Dumps the entire storage hash to stdout in a sort of tree style format
   * @param table An optional table, if null, print them all.
   * @param ascii Whether or not the values should be converted to ascii
   */
  public void dumpToSystemOut(final byte[] table, final boolean ascii) {
    if (storage.isEmpty()) {
      System.out.println("Storage is Empty");
      return;
    }

    for (Entry<byte[], ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>>> db_table :
      storage.entrySet()) {
      if (table != null && Bytes.memcmp(table, db_table.getKey()) != 0) {
        continue;
      }
      
      System.out.println("[Table] " + new String(db_table.getKey(), Const.ASCII_US_CHARSET));

      for (Entry<byte[], ByteMap<ByteMap<TreeMap<Long, byte[]>>>> cf :
        db_table.getValue().entrySet()) {
        System.out.println("  [CF] " + new String(cf.getKey(), Const.ASCII_US_CHARSET));

        for (Entry<byte[], ByteMap<TreeMap<Long, byte[]>>> row :
          cf.getValue().entrySet()) {
          System.out.println("    [Row] " + (ascii ?
              new String(row.getKey(), Const.ASCII_US_CHARSET) : bytesToString(row.getKey())));

          for (Map.Entry<byte[], TreeMap<Long, byte[]>> column : row.getValue().entrySet()) {
            System.out.println("      [Qual] " + (ascii ?
                "\"" + new String(column.getKey(), Const.ASCII_US_CHARSET) + "\""
                : bytesToString(column.getKey())));
            for (Map.Entry<Long, byte[]> cell : column.getValue().entrySet()) {
              System.out.println("        [TS] " + cell.getKey() + "  [Value] " +
                  (ascii ?  new String(cell.getValue(), Const.ASCII_US_CHARSET)
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
    tsdb.put("t".getBytes(Const.ASCII_US_CHARSET), 
        new ByteMap<ByteMap<TreeMap<Long, byte[]>>>());
    storage.put(DATA_TABLE, tsdb);

    final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> tsdb_uid =
        new ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>>();
    tsdb_uid.put("name".getBytes(Const.ASCII_US_CHARSET),
        new ByteMap<ByteMap<TreeMap<Long, byte[]>>>());
    tsdb_uid.put("id".getBytes(Const.ASCII_US_CHARSET),
        new ByteMap<ByteMap<TreeMap<Long, byte[]>>>());
    storage.put(UID_TABLE, tsdb_uid);
  }

  /**
   * Gets one or more columns from a row. If the row does not exist, a null is
   * returned. If no qualifiers are given, the entire row is returned.
   * NOTE: all timestamp, value pairs are returned.
   */
  private class MockGet implements ListenableFuture<List<Row>> {
    Exception exception;
    List<Row> response;
    
    MockGet(final ReadRowsRequest request) {
      multi_gets.add(request);
      response = Lists.newArrayList();
      
      // TODO - for now we're just comparing literal bytes since TSD isn't
      // using regex in gets.
      byte[] family_regex = null;
      byte[] qualifier_regex = null;
      
      if (request.hasFilter()) {
        if (request.getFilter().hasChain()) {
          for (final RowFilter filter : request.getFilter().getChain().getFiltersList()) {
            if (!filter.getFamilyNameRegexFilterBytes().isEmpty()) {
              family_regex = filter.getFamilyNameRegexFilterBytes().toByteArray();
            } else if (!filter.getColumnQualifierRegexFilter().isEmpty()) {
              qualifier_regex = filter.getColumnQualifierRegexFilter().toByteArray();
            }
          }
        }
      }
      
      for (final ByteString key : request.getRows().getRowKeysList()) {
        if (exceptions != null) {
          final Pair<RuntimeException, Boolean> ex = exceptions.get(key.toByteArray());
          if (ex != null) {
            if (ex.getValue()) {
              exception = ex.getKey();
              return;
            } else {
              throw ex.getKey();
            }
          }
        }
  
        final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map =
            storage.get(request.getTableNameBytes().toByteArray());
        if (map == null) {
          exception = new RuntimeException(
            "No such table " + Bytes.pretty(request.getTableNameBytes().toByteArray()));
          return;
        }
        
        Family.Builder family_builder = Family.newBuilder();
        int cells = 0;
        for (final Entry<byte[], ByteMap<ByteMap<TreeMap<Long, byte[]>>>> cf :
            map.entrySet()) {
          if (family_regex != null && Bytes.memcmp(family_regex, cf.getKey()) != 0) {
            continue;
          }
  
          final ByteMap<TreeMap<Long, byte[]>> row = cf.getValue().get(key.toByteArray());
          if (row == null) {
            continue;
          }
  
          for (Entry<byte[], TreeMap<Long, byte[]>> column : row.entrySet()) {
            if (qualifier_regex != null && 
                Bytes.memcmp(qualifier_regex, column.getKey()) != 0) {
              continue;
            }
  
            // TODO - if we want to support multiple values, iterate over the
            // tree map. Otherwise Get returns just the latest value.
            family_builder.addColumns(Column.newBuilder()
                .setQualifier(ByteStringer.wrap(column.getKey()))
                .addCells(Cell.newBuilder()
                    .setTimestampMicros(column.getValue().firstKey())
                    .setValue(ByteStringer.wrap(
                        column.getValue().firstEntry().getValue()))));
            cells++;
          }
        }
        
        if (cells > 0) {
          response.add(Row.newBuilder()
              .setKey(key)
              .addFamilies(family_builder)
              .build());
        }
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public List<Row> get() throws InterruptedException, ExecutionException {
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return response;
    }

    @Override
    public List<Row> get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return response;
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
      // TODO - super mega ugly reflection because Futures$CallbackListener
      // is a private static class.
      final FutureCallback<List<Row>> callback = 
          (FutureCallback<List<Row>>) 
            Whitebox.getInternalState(listener, "callback");
      if (exception != null) {
        callback.onFailure(new ExecutionException(exception));
      } else {
        callback.onSuccess(response);
      }
    }
  }

//  /**
//   * Handles a multi-get call by routing individual requests to the MockGet
//   */
//  private class MockMultiGet implements 
//      Answer<Deferred<List<GetResultOrException>>> {
//    final HBaseClient client;
//    public MockMultiGet(final HBaseClient client) {
//      this.client = client;
//    }
//    
//    @Override
//    public Deferred<List<GetResultOrException>> answer(
//        final InvocationOnMock invocation) throws Throwable {
//      final Object[] args = invocation.getArguments();
//      @SuppressWarnings("unchecked")
//      final List<GetRequest> gets = (List<GetRequest>) args[0];
//      multi_gets.add(gets);
//      final List<GetResultOrException> results = 
//          Lists.newArrayListWithCapacity(gets.size());
//      for (final GetRequest get : gets) {
//        try {
//          // just reuse the logic above.
//          results.add(new GetResultOrException(client.get(get).join()));
//        } catch (Exception e) {
//          results.add(new GetResultOrException(e));
//        }
//      }
//      return Deferred.fromResult(results);
//    }
//  }
//  
  /**
   * Stores one or more columns in a row. If the row does not exist, it's
   * created.
   */
  private class MockPut implements ListenableFuture<MutateRowResponse> {
    final MutateRowRequest request; 
    Exception exception;
    MutateRowResponse response;
    
    MockPut(final MutateRowRequest request) {
      this.request = request;
      if (exceptions != null) {
        final Pair<RuntimeException, Boolean> ex = 
            exceptions.get(request.getRowKey().toByteArray());
        if (ex != null) {
          if (ex.getValue()) {
            exception = ex.getKey();
            return;
          } else {
            throw ex.getKey();
          }
        }
      }

      final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map =
          storage.get(request.getTableNameBytes().toByteArray());
      if (map == null) {
        exception = new RuntimeException("No such table " 
            + Bytes.pretty(request.getTableNameBytes().toByteArray()));
        return;
      }

      for (final Mutation mutation : request.getMutationsList()) {
        final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = 
            map.get(mutation.getSetCell().getFamilyNameBytes().toByteArray());
        if (cf == null) {
          exception = new RuntimeException(
              "No such CF " + Bytes.pretty(
                  mutation.getSetCell().getFamilyNameBytes().toByteArray()));
          return;
        }
  
        ByteMap<TreeMap<Long, byte[]>> row = cf.get(
            request.getRowKey().toByteArray());
        if (row == null) {
          row = new ByteMap<TreeMap<Long, byte[]>>();
          cf.put(request.getRowKey().toByteArray(), row);
        }
  
        TreeMap<Long, byte[]> column = row.get(
            mutation.getSetCell().getColumnQualifier().toByteArray());
        if (column == null) {
          column = new TreeMap<Long, byte[]>(Collections.reverseOrder());
          row.put(mutation.getSetCell().getColumnQualifier().toByteArray(), column);
        } else {
          long storedTs = column.firstKey();
    		  if (mutation.getSetCell().getTimestampMicros() / 1000 >= storedTs) {
    		    column.clear();
    		  } else {
    		    continue;
    		  }
        }
        
        column.put(mutation.getSetCell().getTimestampMicros() / 1000 
            != Long.MAX_VALUE ? mutation.getSetCell().getTimestampMicros() / 1000 :
          current_timestamp++, mutation.getSetCell().getValue().toByteArray());
        assert column.size() == 1 : "Since max versions allowed is 1, there can "
            + "never be two entries at similar timestamp. To resolve change the "
            + "code to only keep the entry with higher timestamp";
      }
      
      response = MutateRowResponse.newBuilder().build();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public MutateRowResponse get()
        throws InterruptedException, ExecutionException {
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return response;
    }

    @Override
    public MutateRowResponse get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return response;
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
      // TODO - super mega ugly reflection because Futures$CallbackListener
      // is a private static class.
      final FutureCallback<MutateRowResponse> callback = 
          (FutureCallback<MutateRowResponse>) 
            Whitebox.getInternalState(listener, "callback");
      if (exception != null) {
        callback.onFailure(new ExecutionException(exception));
      } else {
        callback.onSuccess(response);
      }
    }
  }

  /**
   * Stores one or more columns in a row. If the row does not exist, it's
   * created.
   */
  private class MockAppendAndIncrement implements ListenableFuture<ReadModifyWriteRowResponse> {
    ReadModifyWriteRowResponse response;
    Exception exception;
    
    final ReadModifyWriteRowRequest request;
    
    MockAppendAndIncrement(final ReadModifyWriteRowRequest request) {
      this.request = request;
      if (exceptions != null) {
        final Pair<RuntimeException, Boolean> ex = exceptions.get(
            request.getRowKey().toByteArray());
        if (ex != null) {
          if (ex.getValue()) {
            exception = ex.getKey();
            return;
          } else {
            throw ex.getKey();
          }
        }
      }

      final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map =
          storage.get(request.getTableNameBytes().toByteArray());
      if (map == null) {
        exception = new RuntimeException(
          "No such table " + Bytes.pretty(request.getTableNameBytes().toByteArray()));
        return;
      }

      Family.Builder family_builder = Family.newBuilder();
      for (final ReadModifyWriteRule rule : request.getRulesList()) {
        final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = 
            map.get(rule.getFamilyNameBytes().toByteArray());
        if (cf == null) {
          exception = new RuntimeException(
              "No such CF " + Bytes.pretty(rule.getFamilyNameBytes().toByteArray()));
          return;
        }
  
        ByteMap<TreeMap<Long, byte[]>> row = cf.get(request.getRowKey().toByteArray());
        if (row == null) {
          row = new ByteMap<TreeMap<Long, byte[]>>();
          cf.put(request.getRowKey().toByteArray(), row);
        }
  
        TreeMap<Long, byte[]> column = row.get(rule.getColumnQualifier().toByteArray());
        if (column == null) {
          column = new TreeMap<Long, byte[]>(Collections.reverseOrder());
          row.put(rule.getColumnQualifier().toByteArray(), column);
        }
  
        final byte[] values;
        long column_timestamp = 0;
        /*
         * If there is no Map for Timestamp -> Value, then we create one, add the value with current timestamp
         * else we fetch the top most KeyValue from the column map and append the value in the request with the value
         * already present. The timestamp is also updated. The larger of either current time or the first timestamp from
         * the map is incremented by 1 and used for the updated KeyValue.
         */
  
        if (column.isEmpty()) {
          values = null;
          column_timestamp = current_timestamp++;
        } else {
          values = column.firstEntry().getValue();
          column_timestamp = current_timestamp > column.firstKey() ? current_timestamp++ : column.firstKey() + 1;
        }
  
        // increment or append
        if (rule.getAppendValue().isEmpty()) {
          long extant = Bytes.getLong(values);
          extant += rule.getIncrementAmount();
          column.clear();
          column.put(column_timestamp, Bytes.fromLong(extant));
          assert column.size() == 1 : "MockBase is designed to store "
              + "only a single version of cell since OpenTSDB table "
              + "schema for HBase is designed in that manner";
          
          family_builder.addColumns(Column.newBuilder()
              .setQualifier(rule.getColumnQualifier())
              .addCells(Cell.newBuilder()
                  .setTimestampMicros(column_timestamp)
                  .setValue(ByteStringer.wrap(Bytes.fromLong(extant)))));
        } else {
          final int current_len = values != null ? values.length : 0;
          final byte[] append_value = new byte[current_len + 
                                               rule.getAppendValue().toByteArray().length];
          if (current_len > 0) {
            System.arraycopy(values, 0, append_value, 0, values.length);
          }
    
          System.arraycopy(rule.getAppendValue().toByteArray(), 0, append_value, current_len,
              rule.getAppendValue().toByteArray().length);
          
          // TODO - Bigtable will keep the old columns around.
          // Remove all the old KeyValues, if any, since we need to have only 1 version of data
          // Column_timestamp will hold the new timestamp and append_value holds the corresponding new data
          column.clear();
          column.put(column_timestamp, append_value);
          assert column.size() == 1 : "MockBase is designed to store "
              + "only a single version of cell since OpenTSDB table "
              + "schema for HBase is designed in that manner";
          
          family_builder.addColumns(Column.newBuilder()
              .setQualifier(rule.getColumnQualifier())
              .addCells(Cell.newBuilder()
                  .setTimestampMicros(column_timestamp)
                  .setValue(ByteStringer.wrap(append_value))));
        }
      }
      
      response = ReadModifyWriteRowResponse.newBuilder()
          .setRow(Row.newBuilder()
              .setKey(request.getRowKey())
              .addFamilies(family_builder))
          .build();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public ReadModifyWriteRowResponse get()
        throws InterruptedException, ExecutionException {
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return response;
    }

    @Override
    public ReadModifyWriteRowResponse get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return response;
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
      // TODO - super mega ugly reflection because Futures$CallbackListener
      // is a private static class.
      final FutureCallback<ReadModifyWriteRowResponse> callback = 
          (FutureCallback<ReadModifyWriteRowResponse>) 
            Whitebox.getInternalState(listener, "callback");
      if (exception != null) {
        callback.onFailure(new ExecutionException(exception));
      } else {
        callback.onSuccess(response);
      }
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
  private class MockCAS implements ListenableFuture<CheckAndMutateRowResponse> {
    final CheckAndMutateRowRequest request;
    Exception exception;
    CheckAndMutateRowResponse response;
    
    MockCAS(final CheckAndMutateRowRequest request) {
      this.request = request;
      
      if (exceptions != null) {
        final Pair<RuntimeException, Boolean> ex = exceptions.get(
            request.getRowKey().toByteArray());
        if (ex != null) {
          if (ex.getValue()) {
            exception = ex.getKey();
            return;
          } else {
            throw ex.getKey();
          }
        }
      }

      final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map =
          storage.get(request.getTableNameBytes().toByteArray());
      if (map == null) {
        exception = new RuntimeException("No such table " 
            + Bytes.pretty(request.getTableNameBytes().toByteArray()));
        return;
      }

      // TODO - handle non-null mutations
      Mutation mutation = request.getFalseMutations(0);
      final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(
          mutation.getSetCell().getFamilyNameBytes().toByteArray());
      if (cf == null) {
        exception = new RuntimeException(
            "No such CF " + Bytes.pretty(
                mutation.getSetCell().getFamilyNameBytes().toByteArray()));
        return;
      }

      ByteMap<TreeMap<Long, byte[]>> row = cf.get(request.getRowKey().toByteArray());
      if (row == null) {
//        if (expected != null && expected.length > 0) {
//          return Deferred.fromResult(false);
//        }
        row = new ByteMap<TreeMap<Long, byte[]>>();
        cf.put(request.getRowKey().toByteArray(), row);
      }

      // CAS can only operate on one cell, so if the put request has more than
      // one, we ignore any but the first
      TreeMap<Long, byte[]> column = row.get(
          mutation.getSetCell().getColumnQualifier().toByteArray());
//      if (column == null && (expected != null && expected.length > 0)) {
//        return Deferred.fromResult(false);
//      }

      // HBase CAS doesn't use Timestamps for comparison
      // Since OpenTSDB uses an HBase Table with single version, the final TreeMap 'column' should always
      // contain a single entry, the one with higher timestamp

      final byte[] stored = column == null ? null : column.firstEntry().getValue();
      if (stored != null) {
        response = CheckAndMutateRowResponse.newBuilder()
            .setPredicateMatched(true)
            .build();
        return;
      }
//      if (stored == null && (expected != null && expected.length > 0)) {
//        return Deferred.fromResult(false);
//      }
//      if (stored != null && (expected == null || expected.length < 1)) {
//        return Deferred.fromResult(false);
//      }
//      if (stored != null && expected != null &&
//          Bytes.memcmp(stored, expected) != 0) {
//        return Deferred.fromResult(false);
//      }

      // passed CAS!
      if (column == null) {
        column = new TreeMap<Long, byte[]>(Collections.reverseOrder());
        row.put(mutation.getSetCell().getColumnQualifier().toByteArray(), column);
      } else {
//        long storedTs = column.firstKey();
//        if (put.timestamp() >= storedTs) {
//            column.clear();
//      	} else {
//      	  return Deferred.fromResult(true);
//      	}
      }

      column.put(mutation.getSetCell().getTimestampMicros() != -1 ? 
          mutation.getSetCell().getTimestampMicros() :
        current_timestamp++, mutation.getSetCell().getValue().toByteArray());

      assert column.size() == 1 : "MockBase is designed to store only a single version of cell since OpenTSDB table schema for HBase is designed in that manner";
      response = CheckAndMutateRowResponse.newBuilder()
          .setPredicateMatched(false)
          .build();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public CheckAndMutateRowResponse get()
        throws InterruptedException, ExecutionException {
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return response;
    }

    @Override
    public CheckAndMutateRowResponse get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return response;
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
      // TODO - super mega ugly reflection because Futures$CallbackListener
      // is a private static class.
      final FutureCallback<CheckAndMutateRowResponse> callback = 
          (FutureCallback<CheckAndMutateRowResponse>) 
            Whitebox.getInternalState(listener, "callback");
      if (exception != null) {
        callback.onFailure(new ExecutionException(exception));
      } else {
        callback.onSuccess(response);
      }
    }

  }
//
//  /**
//   * Deletes one or more columns. If a row no longer has any valid columns, the
//   * entire row will be removed.
//   */
//  private class MockDelete implements Answer<Deferred<Object>> {
//
//    @Override
//    public Deferred<Object> answer(InvocationOnMock invocation)
//        throws Throwable {
//      final Object[] args = invocation.getArguments();
//      final DeleteRequest delete = (DeleteRequest)args[0];
//
//      if (exceptions != null) {
//        final Pair<RuntimeException, Boolean> ex = exceptions.get(delete.key());
//        if (ex != null) {
//          if (ex.getValue()) {
//            return Deferred.fromError(ex.getKey());
//          } else {
//            throw ex.getKey();
//          }
//        }
//      }
//
//      final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map =
//          storage.get(delete.table());
//      if (map == null) {
//        return Deferred.fromError(new RuntimeException(
//          "No such table " + Bytes.pretty(delete.table())));
//      }
//
//      // if no qualifiers or family, then delete the row from all families
//      if ((delete.qualifiers() == null || delete.qualifiers().length < 1 ||
//          delete.qualifiers()[0].length < 1) && (delete.family() == null ||
//          delete.family().length < 1)) {
//        for (final Entry<byte[], ByteMap<ByteMap<TreeMap<Long, byte[]>>>> cf :
//          map.entrySet()) {
//          cf.getValue().remove(delete.key());
//        }
//        return Deferred.fromResult(new Object());
//      }
//
//      final byte[] family = delete.family();
//      if (family != null && family.length > 0) {
//        if (!map.containsKey(family)) {
//          return Deferred.fromError(new RuntimeException(
//              "No such CF " + Bytes.pretty(family)));
//        }
//      }
//
//      // compile a set of qualifiers
//      ByteMap<Object> qualifiers = new ByteMap<Object>();
//      if (delete.qualifiers() != null || delete.qualifiers().length > 0) {
//        for (byte[] q : delete.qualifiers()) {
//          qualifiers.put(q, null);
//        }
//      }
//
//      // TODO - validate the assumption that a delete with a row key and qual
//      // but without a family would delete the columns in ALL families
//
//      // if the request only has a column family and no qualifiers, we delete
//      // the row from the entire family
//      if (family != null && qualifiers.isEmpty()) {
//        final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(delete.family());
//        // cf != null validated above
//        cf.remove(delete.key());
//        return Deferred.fromResult(new Object());
//      }
//
//      for (final Entry<byte[], ByteMap<ByteMap<TreeMap<Long, byte[]>>>> cf :
//        map.entrySet()) {
//
//        // column family filter
//        if (family != null && family.length > 0 &&
//            !Bytes.equals(family, cf.getKey())) {
//          continue;
//        }
//
//        ByteMap<TreeMap<Long, byte[]>> row = cf.getValue().get(delete.key());
//        if (row == null) {
//          continue;
//        }
//
//        for (byte[] qualifier : qualifiers.keySet()) {
//          final TreeMap<Long, byte[]> column = row.get(qualifier);
//          if (column == null) {
//            continue;
//          }
//
//          // with this flag we delete a single timestamp
//          if (delete.deleteAtTimestampOnly()) {
//            if (column != null) {
//              column.remove(delete.timestamp());
//              if (column.isEmpty()) {
//                row.remove(qualifier);
//              }
//            }
//          } else {
//            // otherwise we delete everything less than or equal to the
//            // delete timestamp
//            List<Long> column_removals = new ArrayList<Long>(column.size());
//            for (Map.Entry<Long, byte[]> cell : column.entrySet()) {
//              if (cell.getKey() <= delete.timestamp()) {
//                column_removals.add(cell.getKey());
//              }
//            }
//            for (Long ts : column_removals) {
//              column.remove(ts);
//            }
//            if (column.isEmpty()) {
//              row.remove(qualifier);
//            }
//          }
//        }
//
//        if (row.isEmpty()) {
//          cf.getValue().remove(delete.key());
//        }
//      }
//      return Deferred.fromResult(new Object());
//    }
//
//  }

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
  public class MockScanner implements ResultScanner<FlatRow> {

    private final ReadRowsRequest request;
    private byte[] start = null;
    private byte[] stop = null;
    private HashSet<String> scnr_qualifiers = null;
    private byte[] family = null;
    private ByteMap<Iterator<Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>>>
      cursors;
    private ByteMap<Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>> cf_rows;
    private byte[] last_row;
    private boolean is_reversed;
    private boolean has_filter;
    private byte[] row_key_regexp;

    /**
     * Default ctor
     * @param mock_scanner The scanner we're using
     * @param table The table (confirmed to exist)
     */
    public MockScanner(final ReadRowsRequest request) {
      this.request = request;
      System.out.println("SCANNER INIT: " + request);
      // parse out the request
      start = request.getRows().getRowRanges(0).getStartKeyClosed().toByteArray();
      stop = request.getRows().getRowRanges(0).getEndKeyOpen().toByteArray();
      
      // get the column family and 
      if (request.hasFilter()) {
        System.out.println("FILTER: " + request.getFilter());
        if (request.getFilter().hasChain()) {
          int filters = 0;
          for (final RowFilter filter : request.getFilter().getChain().getFiltersList()) {
            if (!filter.getFamilyNameRegexFilterBytes().isEmpty()) {
              family = filter.getFamilyNameRegexFilterBytes().toByteArray();
              continue;
            }
            
            if (!filter.getRowKeyRegexFilter().isEmpty()) {
              row_key_regexp = filter.getRowKeyRegexFilter().toByteArray();
              continue;
            }
            
            // probably qualifier filters then.
            filters++;
          }
          
          if (filters > 0) {
            has_filter = true;
          }
          
        } else if (!request.getFilter().getFamilyNameRegexFilterBytes().isEmpty()) {
          family = request.getFilter().getFamilyNameRegexFilterBytes().toByteArray();
        } else if (!request.getFilter().getRowKeyRegexFilter().isEmpty()) {
          row_key_regexp = request.getFilter().getRowKeyRegexFilter().toByteArray();
        } else {
          // probably qualifier filters then.
          has_filter = true;
        }
      }
      scanners.add(this);
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
    
    @Override
    public void close() throws IOException { }

    @Override
    public FlatRow next() throws IOException {
      final FlatRow[] results = next(1);
      return results.length > 0 ? results[0] : null;
    }

    @Override
    public FlatRow[] next(final int count) throws IOException {
      if (cursors == null) {
        System.out.println("INITIALIZING SCANNER..........");
        final ByteMap<ByteMap<ByteMap<TreeMap<Long, byte[]>>>> map =
            storage.get(request.getTableNameBytes().toByteArray());
        if (map == null) {
         throw new RuntimeException("No such table " 
             + Bytes.pretty(request.getTableNameBytes().toByteArray()));
        }

        cursors = new ByteMap<Iterator<Entry<byte[],
            ByteMap<TreeMap<Long, byte[]>>>>>();
        cf_rows = new ByteMap<Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>>();

        if (family == null || family.length < 1) {
          for (final Entry<byte[], ByteMap<ByteMap<TreeMap<Long, byte[]>>>> cf : map) {
            final Iterator<Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>> cursor;
            if (is_reversed) {
              cursor = cf.getValue().descendingMap().entrySet().iterator();
            } else {
              cursor = cf.getValue().iterator();
            }
            cursors.put(cf.getKey(), cursor);
            cf_rows.put(cf.getKey(), null);
          }
        } else {
          final ByteMap<ByteMap<TreeMap<Long, byte[]>>> cf = map.get(family);
          if (cf == null) {
            throw new RuntimeException("No such CF " + Bytes.pretty(family));
          }
          final Iterator<Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>>
            cursor = cf.iterator();
          cursors.put(family, cursor);
          cf_rows.put(family, null);
        }
      }
      System.out.println("HASNEXT: " + hasNext());

      // If we're out of rows to scan, then we return an empty array like
      // Bigtable does.
      if (!hasNext()) {
        return new FlatRow[0];
      }
      
      // TODO - fix the regex comparator
      Pattern pattern = null;
      Charset regex_charset = Const.ISO_8859_CHARSET;
      if (row_key_regexp != null) {
        try {
          // key regex filter uses Bytes.UTF8(<string>)
          pattern = Pattern.compile(new String(row_key_regexp, 
              Const.ISO_8859_CHARSET));
        } catch (PatternSyntaxException e) {
          e.printStackTrace();
          throw e;
        }
      }

      // return all matches
      final List<FlatRow> results = Lists.newArrayList();
      int rows_read = 0;
      while (hasNext()) {
        advance();
        // if it's before the start row, after the end row or doesn't
        // match the given regex, continue on to the next row
        if (start != null && Bytes.memcmp(last_row, start) < 0) {
          continue;
        }
        
        // TODO ??? Scanner's logic:
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
          final String from_bytes = new String(last_row, regex_charset);
          if (!pattern.matcher(from_bytes).find()) {
            continue;
          }
        }

        // throws AFTER we match on a row key
        if (exceptions != null) {
          final Pair<RuntimeException, Boolean> ex = exceptions.get(last_row);
          if (ex != null) {
            if (ex.getValue()) {
              throw ex.getKey();
            } else {
              throw ex.getKey();
            }
          }
        }

        // loop over the column family rows to see if they match
        for (final Entry<byte[], Entry<byte[], ByteMap<TreeMap<Long, byte[]>>>> row :
          cf_rows.entrySet()) {
          if (row.getValue() == null ||
              Bytes.memcmp(last_row, row.getValue().getKey()) != 0) {
            continue;
          }

          FlatRow.Builder row_builder = FlatRow.newBuilder()
              .withRowKey(ByteStringer.wrap(row.getValue().getKey()));
          int cells = 0;
          for (final Entry<byte[], TreeMap<Long, byte[]>> column :
            row.getValue().getValue().entrySet()) {
            // if the qualifier isn't in the set, continue
            if (scnr_qualifiers != null &&
                !scnr_qualifiers.contains(bytesToString(column.getKey()))) {
              continue;
            }

            // handle qualifier filters. 
            if (has_filter) {
              if (!matchOtherFilters(column.getKey())) {
                continue;
              }
            }
            
            row_builder.addCell(new String(row.getKey()), 
                ByteStringer.wrap(column.getKey()),
                column.getValue().firstKey(),
                ByteStringer.wrap(column.getValue().firstEntry().getValue()));
            cells++;
          }
          
          if (cells > 0) {
            results.add(row_builder.build());
          }
        }
        rows_read++;
        
        if (rows_read >= count) {
          final FlatRow[] result_array = new FlatRow[count];
          results.toArray(result_array);
          return result_array;
        }
      }

      if (results.isEmpty()) {
        return new FlatRow[0];
      }
      
      final FlatRow[] result_array = new FlatRow[results.size()];
      results.toArray(result_array);
      return result_array;
    }

    @Override
    public int available() {
      // TODO - yeah I know
      return hasNext() ? 1 : 0;
    }
    
    public ReadRowsRequest request() {
      return request;
    }
    
    boolean matchOtherFilters(final byte[] qualifier) {
      Interleave interleave = null;
      if (request.getFilter().hasChain()) {
        for (final RowFilter filter : request.getFilter().getChain().getFiltersList()) {
          if (filter.hasInterleave()) {
            interleave = filter.getInterleave();
            break;
          }
        }
      } else if (request.getFilter().hasInterleave()) {
        interleave = request.getFilter().getInterleave();
      }
      
      if (interleave == null) {
        return false;
      }
      
      for (final RowFilter filter : interleave.getFiltersList()) {
        if (!filter.getColumnQualifierRegexFilter().isEmpty()) {
          Pattern pattern = Pattern.compile(new String(
              filter.getColumnQualifierRegexFilter().toByteArray(), 
              Const.ASCII_US_CHARSET));
          final String from_bytes = new String(last_row, Const.ASCII_US_CHARSET);
          if (pattern.matcher(from_bytes).find()) {
            return true;
          }
        }
      }
      
      return false;
    }
  }
//
//  /**
//   * Creates or increments (possibly decrements) a Long in the hash table at the

}
