// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.Const;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Pair;
import org.hbase.async.*;
import org.hbase.async.Scanner;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import net.opentsdb.uid.UniqueIdType;

import static com.google.common.collect.Maps.newHashMap;
import static net.opentsdb.uid.UniqueId.uidToString;

/**
 * TsdbStore implementation useful in testing calls to and from
 * storage with actual pretend data. The underlying data store is an
 * incredibly ugly nesting of ByteMaps from AsyncHbase so it stores and
 * orders byte arrays similar to HBase. A MemoryStore instance represents a
 * SINGLE table in HBase but it provides support for column families and
 * timestamped entries.
 * <p>
 * It's not a perfect implementation but is useful for the majority of unit
 * tests. Gets, puts, cas, deletes and scans are currently supported. See
 * notes for each method below about what does and doesn't work.
 * <p>
 * Regarding timestamps, whenever you execute an RPC request, the
 * {@code current_timestamp} will be incremented by one millisecond. By default
 * the timestamp starts at 1/1/2014 00:00:00 but you can set it to any value
 * at any time. If a PutRequest comes in with a specific time, that time will
 * be stored and the timestamp will not be incremented.
 * <p>
 * @since 2.0
 */
public class MemoryStore implements TsdbStore {
  private static final Charset ASCII = Charsets.ISO_8859_1;

  //      KEY           Column Family Qualifier     Timestamp     Value
  private Bytes.ByteMap<Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>>>
    storage = new Bytes.ByteMap<Bytes.ByteMap<Bytes.ByteMap<TreeMap<Long, byte[]>>>>();

  private int TREE_MAX_ID = 1;

  private final Map<Integer, Tree> tree_table;
                    //ID       display_name
  private final Table<Integer, String, Branch> branch_table;
                   //branch ID  tsuid
  private final Table<Pair<Integer, String>, String, Leaf> leaf_table;
  private final Table<String, String, byte[]> data_table;
  private final Table<Long, String, byte[]> uid_table;
  private final Table<String, Long, Annotation> annotation_table;

  private final Map<UniqueIdType, AtomicLong> uid_max = ImmutableMap.of(
          UniqueIdType.METRIC, new AtomicLong(1),
          UniqueIdType.TAGK, new AtomicLong(1),
          UniqueIdType.TAGV, new AtomicLong(1));
  private final Table<String, UniqueIdType, byte[]> uid_forward_mapping;
  private final Table<String, UniqueIdType, String> uid_reverse_mapping;

  private HashSet<MockScanner> scanners = new HashSet<MockScanner>(2);
  private byte[] default_family = "t".getBytes(ASCII);

  /** Incremented every time a new value is stored (without a timestamp) */
  private long current_timestamp = 1388534400000L;

  public MemoryStore() {
    tree_table = newHashMap();
    branch_table = HashBasedTable.create();
    leaf_table = HashBasedTable.create();
    data_table = HashBasedTable.create();
    uid_table = HashBasedTable.create();
    annotation_table = HashBasedTable.create();
    uid_forward_mapping = HashBasedTable.create();
    uid_reverse_mapping = HashBasedTable.create();

  }

  /**
   * Creates or increments (possibly decrements) a Long in the hash table at the
   * given location.
   */
  @Override
  public Deferred<Long> atomicIncrement(AtomicIncrementRequest air) {
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

  @Override
  public Deferred<Long> bufferAtomicIncrement(AtomicIncrementRequest request) {
    return atomicIncrement(request);
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
  @Override
  public Deferred<Boolean> compareAndSet(PutRequest put, byte[] expected) {
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

  /**
   * Deletes one or more columns. If a row no longer has any valid columns, the
   * entire row will be removed.
   */
  @Override
  public Deferred<Object> delete(DeleteRequest delete) {
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

  @Override
  public Deferred<ArrayList<Object>> checkNecessaryTablesExist() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Object> flush() {
    return Deferred.fromResult(null);
  }

  /**
   * Gets one or more columns from a row. If the row does not exist, a null is
   * returned. If no qualifiers are given, the entire row is returned.
   * NOTE: all timestamp, value pairs are returned.
   */
  @Override
  public Deferred<ArrayList<KeyValue>> get(GetRequest get) {
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
        KeyValue kv = PowerMockito.mock(KeyValue.class);
        Mockito.when(kv.timestamp()).thenReturn(column.getValue().firstKey());
        Mockito.when(kv.value()).thenReturn(column.getValue().firstEntry().getValue());
        Mockito.when(kv.qualifier()).thenReturn(column.getKey());
        Mockito.when(kv.key()).thenReturn(get.key());
        kvs.add(kv);
      }
    }
    return Deferred.fromResult(kvs);
  }

  @Override
  public long getFlushInterval() {
    return 0;
  }

  @Override
  public Scanner newScanner(byte[] table) {
    final Scanner scanner = PowerMockito.mock(Scanner.class);
    scanners.add(new MockScanner(scanner));
    return scanner;
  }

  /**
   * Stores one or more columns in a row. If the row does not exist, it's
   * created.
   */
  @Override
  public Deferred<Object> put(PutRequest put) {
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

    return Deferred.fromResult(null);
  }

  @Override
  public void setFlushInterval(short aShort) {
  }

  @Override
  public Deferred<Object> addPoint(byte[] row, byte[] qualifier, byte[] value) {
    final String s_row = new String(row, ASCII);
    final String s_qual = new String(qualifier, ASCII);
    data_table.put(s_row, s_qual, value);
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void recordStats(StatsCollector collector) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<com.google.common.base.Optional<byte[]>> getId(String name, UniqueIdType type) {
    byte[] id = uid_forward_mapping.get(name, type);

    if (id != null && id.length != type.width) {
      throw new IllegalStateException("Found id.length = " + id.length
              + " which is != " + type.width
              + " required for '" + type.qualifier + '\'');
    }

    return Deferred.fromResult(Optional.fromNullable(id));
  }

  @Override
  public Deferred<com.google.common.base.Optional<String>> getName(byte[] id, UniqueIdType type) {
    if (id.length != type.width) {
      throw new IllegalArgumentException("Wrong id.length = " + id.length
              + " which is != " + type.width
              + " required for '" + type.qualifier + '\'');
    }

    String str_uid = new String(id, Const.CHARSET_ASCII);

    final String name = uid_reverse_mapping.get(str_uid, type);
    return Deferred.fromResult(Optional.fromNullable(name));
  }

  @Override
  public Deferred<Object> add(final UIDMeta meta) {
    uid_table.put(
            UniqueId.uidToLong(meta.getUID()),
            meta.getType().toString().toLowerCase() + "_meta",
            meta.getStorageJSON());

    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> delete(final UIDMeta meta) {
    uid_table.remove(
            UniqueId.uidToLong(meta.getUID()),
            meta.getType().toString().toLowerCase() + "_meta");

    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<UIDMeta> getMeta(final byte[] uid,
                                   final String name,
                                   final UniqueIdType type) {
    final String qualifier = type.toString().toLowerCase() + "_meta";
    final long s_uid = UniqueId.uidToLong(uid);

    byte[] json_value = uid_table.get(s_uid, qualifier);

    if (json_value == null) {
      // return the default
      return Deferred.fromResult(new UIDMeta(type, uid, name, false));
    }

    UniqueIdType effective_type = type;
    if (effective_type == null) {
      effective_type = UniqueIdType.fromString(qualifier.substring(0,
              qualifier.indexOf("_meta")));
    }

    UIDMeta return_meta = UIDMeta.buildFromJSON(
            json_value, effective_type, uid, name);

    return Deferred.fromResult(return_meta);
  }

  private Deferred<UIDMeta> getMeta(final byte[] uid,
                                    final UniqueIdType
          type) {
    final String qualifier = type.toString().toLowerCase() + "_meta";
    byte[] json_value = uid_table.get(UniqueId.uidToLong(uid), qualifier);

    if (json_value == null)
      return Deferred.fromResult(null);

    UIDMeta meta = JSON.parseToObject(json_value, UIDMeta.class);
    meta.resetChangedMap();

    return Deferred.fromResult(meta);
  }

  @Override
  public Deferred<Boolean> updateMeta(final UIDMeta meta,
                                      final boolean overwrite) {
    return getMeta(meta.getUID(), meta.getType()).addCallbackDeferring(
            new Callback<Deferred<Boolean>, UIDMeta>() {
              @Override
              public Deferred<Boolean> call(UIDMeta meta2) throws Exception {
                if (null != meta2) {
                  meta.syncMeta(meta2, overwrite);
                  add(meta);
                }

                // The MemoryStore is not expected to run in multiple threads
                // and because of this there won't be multiple clients that
                // can change the value between #updateMeta's initial
                // #getMeta and the subsequent #add(meta) above. Because of
                // this we can safely always return TRUE here.
                return Deferred.fromResult(Boolean.TRUE);
              }
            });
  }

  @Override
  public Deferred<Object> deleteUID(byte[] name, byte[] kind) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<byte[]> allocateUID(final String name,
                                      final UniqueIdType type) {
    final byte[] row = Bytes.fromLong(uid_max.get(type).getAndIncrement());

    // row.length should actually be 8.
    if (row.length < type.width) {
      throw new IllegalStateException("row.length = " + row.length
              + " which is less than " + type.width
              + " for id=" + uid_max
              + " row=" + Arrays.toString(row));
    }

    // Verify that the indices in the row array that won't be used in the
    // uid with the current length are zero so we haven't reached the upper
    // limits.
    for (int i = 0; i < row.length - type.width; i++) {
      if (row[i] != 0) {
        throw new IllegalArgumentException("All Unique IDs for " + type.qualifier
                + " on " + type.width + " bytes are already assigned!");
      }
    }

    // Shrink the ID on the requested number of bytes.
    final byte[] uid = Arrays.copyOfRange(row, row.length - type.width,
            row.length);

    return allocateUID(name, uid, type);
  }

  @Override
  public Deferred<byte[]> allocateUID(final String name,
                                      final byte[] uid,
                                      final UniqueIdType type) {
    uid_max.get(type).set(Math.max(uid_max.get(type).get(),
            (UniqueId.uidToLong(uid, (short) uid.length) + 1 )));

    String str_uid = new String(uid, Const.CHARSET_ASCII);

    if (uid_reverse_mapping.contains(str_uid, type)) {
      throw new IllegalArgumentException("A UID with " + str_uid + " already exists");
    }

    uid_reverse_mapping.put(str_uid, type, name);

    if (uid_forward_mapping.contains(name, type)) {
      return Deferred.fromResult(uid_forward_mapping.get(name,type));
    }

    uid_forward_mapping.put(name, type, uid);

    return Deferred.fromResult(uid);
  }

  @Override
  public void scheduleForCompaction(byte[] row) {
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
   * Total number of columns in the given row on the data table.
   * @param row The row to search for
   * @return -1 if the row did not exist, otherwise the number of columns.
   */
  public long numColumnsDataTable(final byte[] row) {
    final String s_row = new String(row, ASCII);

    if (!data_table.containsRow(s_row))
      return -1;

    return data_table.row(s_row).size();
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
   * Retrieve a column from the data table.
   * @param key The row key of the column
   * @param qualifier The column qualifier
   * @return The byte array of data or null if not found
   */
  public byte[] getColumnDataTable(final byte[] key,
                          final byte[] qualifier) {
    final String s_key = new String(key, ASCII);
    final String s_qual = new String(qualifier, ASCII);
    return data_table.get(s_key, s_qual);
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
   * Attempts to mark an Annotation object for deletion. Note that if the
   * annoation does not exist in storage, this delete call will not throw an
   * error.
   *
   * @param annotation@return A meaningless Deferred for the caller to wait on until the call is
   * complete. The value may be null.
   */
  @Override
  public Deferred<Object> delete(Annotation annotation) {

    final String tsuid = annotation.getTSUID() != null && !annotation.getTSUID().isEmpty() ?
            annotation.getTSUID() : "";

    final long start = annotation.getStartTime() % 1000 == 0 ?
            annotation.getStartTime() / 1000 : annotation.getStartTime();
    if (start < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    return Deferred.fromResult(
            (Object) annotation_table.remove(tsuid, start));
  }

  @Override
  public Deferred<Boolean> updateAnnotation(Annotation original, Annotation annotation) {

    String tsuid = !Strings.isNullOrEmpty(annotation.getTSUID()) ?
            annotation.getTSUID() : "";

    final long start = annotation.getStartTime() % 1000 == 0 ?
            annotation.getStartTime() / 1000 : annotation.getStartTime();
    if (start < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    Annotation note = annotation_table.get(tsuid, start);

    if ((null == note && null == original) ||
            (null != note && null != original && note.equals(original))) {
      annotation_table.remove(tsuid, start);
      annotation_table.put(tsuid, start, annotation);
      return Deferred.fromResult(true);
    }
    return Deferred.fromResult(false);
  }

  @Override
  public Deferred<List<Annotation>> getGlobalAnnotations(final long start_time,
                                                         final long end_time) {
    //some sanity check should happen before this is called actually.

    if (start_time < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    List<Annotation> annotations = new ArrayList<Annotation>();
    Collection<Annotation> globals = annotation_table.row("").values();

    for (Annotation global: globals) {
      if (start_time <= global.getStartTime() && global.getStartTime() <= end_time) {
        annotations.add(global);
      }
    }
    return Deferred.fromResult(annotations);
  }

  @Override
  public Deferred<Integer> deleteAnnotationRange(final byte[] tsuid,
                                                 final long start_time,
                                                 final long end_time) {

    //some sanity check should happen before this is called actually.
    //however we need to modify the start_time and end_time

    if (start_time < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    final long start = start_time % 1000 == 0 ? start_time / 1000 : start_time;
    final long end = end_time % 1000 == 0 ? end_time / 1000 : end_time;
    String key = "";

    ArrayList<Annotation> del_list = new ArrayList<Annotation>();
    if (tsuid != null) {
      //convert byte array to string, sloppy but seems to be the best way
      key = uidToString(tsuid);
    }

    Collection<Annotation> globals = annotation_table.row(key).values();
    for (Annotation global: globals) {
      if (start <= global.getStartTime() && global.getStartTime() <= end) {
        del_list.add(global);
      }
    }

    for (Annotation a : del_list) {
      delete(a);
    }
    return Deferred.fromResult(del_list.size());

  }

  @Override
  public Deferred<Tree> fetchTree(int tree_id) {
    return Deferred.fromResult(tree_table.get(tree_id));
  }

  @Override
  public Deferred<Boolean> storeTree(Tree tree, boolean overwrite) {
    // The overwrite matters only for the HBaseStore implementation.
    // Therefore in this case this value should not matter or be tested.
    // It can happen in-flight for a live instance
    // (Should be tested in the HBaseStore test class)

    Tree stored_tree = tree_table.get(tree.getTreeId());

    if (stored_tree == null) {
      stored_tree = tree;
    } else {
      stored_tree.copyChanges(tree, overwrite);
    }
    Tree res = tree_table.put(stored_tree.getTreeId(), stored_tree);
    // In the real implementation it does a compare and set.
    // So since this is a serial implementation right now no other process
    // should have accessed and it should always return true.
    return Deferred.fromResult(true);
  }

  @Override
  public Deferred<Integer> createNewTree(Tree tree) {
    tree.setTreeId(TREE_MAX_ID++);
    if (tree.getTreeId() > Const.MAX_TREE_ID_INCLUSIVE) {
      throw new IllegalStateException("Exhausted all Tree IDs");
    }
    tree_table.put(tree.getTreeId(), tree);
    return Deferred.fromResult(tree.getTreeId());
  }

  @Override
  public Deferred<List<Tree>> fetchAllTrees() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Boolean> deleteTree(int tree_id, boolean delete_definition) {
    Tree res = tree_table.remove(tree_id);
    if (res == null)
      return Deferred.fromResult(false);
    return Deferred.fromResult(true);
  }

  @Override
  public Deferred<Map<String, String>> fetchCollisions(int tree_id, List<String> tsuids) {
    /*
    * This method in lack of something better will return an empty map.
    * Later this must be solved.
    */
    final Map<String, String> returnValue = new HashMap<String, String>();
    return Deferred.fromResult(returnValue);
  }

  @Override
  public Deferred<Map<String, String>> fetchNotMatched(int tree_id, List<String> tsuids) {
    final Map<String, String> returnValue = new HashMap<String, String>();
    return Deferred.fromResult(returnValue);
  }

  @Override
  public Deferred<Boolean> flushTreeCollisions(Tree tree) {
    /*
     * Return a meaningless deferred but should always be true.
     */
    return Deferred.fromResult(true);
  }

  @Override
  public Deferred<Boolean> flushTreeNotMatched(Tree tree) {
    return Deferred.fromResult(true); //A meaningless deferred
  }

  @Override
  public Deferred<Boolean> storeLeaf(final Leaf leaf, final Branch branch,
                                     final Tree tree) {


      //tsuid   object
    final Iterator<Map.Entry<String, Leaf>> iterator = leaf_table.row(
            new Pair<Integer, String>(branch.getTreeId(),
                    branch.getDisplayName())).entrySet().iterator();

    Leaf existing_leaf = null;
    boolean found = false;

    while(iterator.hasNext()) {
      Map.Entry<String, Leaf> entry = iterator.next();
      //found the leaf matching our display name
      if (entry.getValue().getDisplayName().equals(leaf.getDisplayName())) {
        existing_leaf = entry.getValue();
        found = true;
        break;
      }
    }
    if(!found) {
      leaf_table.put(
              new Pair<Integer, String>(
              branch.getTreeId(),branch.getDisplayName()),
              leaf.getTsuid(), leaf);
      return Deferred.fromResult(true);
    }
    if (existing_leaf == null) {
      // this should never happen in this implementation
      // stored data may be corrupt
      // the likelyhood of getting corrupt data is very low.
      return Deferred.fromResult(false);
    }
    if (existing_leaf.getTsuid().equals(leaf.getTsuid())) {
      return Deferred.fromResult(true);
    }
    tree.addCollision(leaf.getTsuid(), existing_leaf.getTsuid());
    return Deferred.fromResult(false);
  }

  @Override
  public Deferred<ArrayList<Boolean>> storeBranch(final Tree tree,
                                                  final Branch branch,
                                                  final boolean store_leaves) {

    final ArrayList<Deferred<Boolean>> storage_results =
            new ArrayList<Deferred<Boolean>>(branch.getLeaves() != null
                    ? branch.getLeaves().size() + 1 : 1);
    if (branch_table.contains(branch.getTreeId(), branch.getDisplayName())) {
      storage_results.add(Deferred.fromResult(false));
    } else {
      branch_table.put(branch.getTreeId(), branch.getDisplayName(), branch);
      storage_results.add(Deferred.fromResult(true));
    }
    if (store_leaves && branch.getLeaves()!= null) {
      for (final Leaf leaf : branch.getLeaves()) {
        storage_results.add(storeLeaf(leaf, branch, tree));
      }
    }
    return Deferred.group(storage_results);
  }

  @Override
  public Deferred<Branch> fetchBranchOnly(byte[] branch_id) {
    final int ID = Tree.bytesToId(branch_id);

    Map<String, Branch> branches =  branch_table.row(ID);
    Branch branch = new Branch();
    boolean found = false;
    for (Map.Entry<String, Branch> branch_entry : branches.entrySet()) {
      Branch temp = branch_entry.getValue();

      if (Arrays.equals(temp.compileBranchId(), branch_id)) {
        found = true;
        branch.setPath(temp.getPath());
        branch.setDisplayName(temp.getDisplayName());
        branch.setTreeId(ID);
        break;
      }
    }
    if (found)
      return Deferred.fromResult(branch);
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Branch> fetchBranch(final byte[] branch_id,
                                      final boolean load_leaf_uids,
                                      final TSDB tsdb) {

    final int ID = Tree.bytesToId(branch_id);

    Map<String, Branch> branches =  branch_table.row(ID);
    Branch branch = new Branch();
    boolean found = false;

    for (Map.Entry<String, Branch> branch_entry : branches.entrySet()) {
      Branch temp = branch_entry.getValue();

      if (Arrays.equals(temp.compileBranchId(), branch_id)) {
        found = true;
        branch.setPath(temp.getPath());
        branch.setDisplayName(temp.getDisplayName());
        branch.setTreeId(ID);
      } else {
        branch.addChild(branch_entry.getValue());
      }
    }
    if (found) {
      Pair<Integer, String> key =
              new Pair<Integer, String>(ID, branch.getDisplayName());
      for (Leaf leaf : leaf_table.row(key).values()) {
        if (!load_leaf_uids) //if the tsuid should not be loaded we remove them
          leaf.setTsuid("");
        branch.addLeaf(leaf);
      }
      return Deferred.fromResult(branch);
    }

    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<TreeRule> fetchTreeRule(int tree_id, int level, int order) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> deleteTreeRule(int tree_id, int level, int order) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> deleteAllTreeRule(int tree_id) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Boolean> syncTreeRuleToStorage(TreeRule rule, boolean overwrite) {
    // Return true if the CAS call succeeded, false if the stored data was
    // modified in flight.
    return Deferred.fromResult(true);
  }

  /**
   * Attempts to fetch a global or local annotation from storage
   * @param tsuid The TSUID as a byte array. May be null if retrieving a global
   * annotation
   * @param start_time The start time as a Unix epoch timestamp
   * @return A valid annotation object if found, null if not
   */
  public Deferred<Annotation> getAnnotation(final byte[] tsuid, final long start_time) {

    if (start_time < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    long time = start_time % 1000 == 0 ? start_time / 1000 : start_time;

    String key = "";
    if (tsuid != null) {
      //convert byte array to proper string
      key = uidToString(tsuid);
    }
    return Deferred.fromResult(annotation_table.get(key, time));
  }

  /**
   * Finds all the {@link net.opentsdb.core.Span}s that match this query.
   * This is what actually scans the HBase table and loads the data into
   * {@link net.opentsdb.core.Span}s.
   * @return A map from HBase row key to the {@link net.opentsdb.core.Span} for that row key.
   * Since a {@link net.opentsdb.core.Span} actually contains multiple HBase rows, the row key
   * stored in the map has its timestamp zero'ed out.
   * @throws org.hbase.async.HBaseException if there was a problem communicating with HBase to
   * perform the search.
   * @throws IllegalArgumentException if bad data was retreived from HBase.
   * @param query
   */
  @Override
  public Deferred<ImmutableList<DataPoints>> executeQuery(final Query query) {
    throw new UnsupportedOperationException("Not implemented yet");
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
      Mockito.doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          regex = (String) args[0];
          return null;
        }
      }).when(mock_scanner).setKeyRegexp(Matchers.anyString());

      Mockito.doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          regex = (String) args[0];
          return null;
        }
      }).when(mock_scanner).setKeyRegexp(Matchers.anyString(), (Charset) Matchers.any());

      Mockito.doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          start = (byte[]) args[0];
          return null;
        }
      }).when(mock_scanner).setStartKey((byte[]) Matchers.any());

      Mockito.doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          stop = (byte[]) args[0];
          return null;
        }
      }).when(mock_scanner).setStopKey((byte[]) Matchers.any());

      Mockito.doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          family = (byte[]) args[0];
          return null;
        }
      }).when(mock_scanner).setFamily((byte[]) Matchers.any());

      Mockito.doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          scnr_qualifiers = new HashSet<String>(1);
          scnr_qualifiers.add(bytesToString((byte[]) args[0]));
          return null;
        }
      }).when(mock_scanner).setQualifier((byte[]) Matchers.any());

      Mockito.doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          final byte[][] qualifiers = (byte[][]) args[0];
          scnr_qualifiers = new HashSet<String>(qualifiers.length);
          for (byte[] qualifier : qualifiers) {
            scnr_qualifiers.add(bytesToString(qualifier));
          }
          return null;
        }
      }).when(mock_scanner).setQualifiers((byte[][]) Matchers.any());

      Mockito.when(mock_scanner.nextRows()).thenAnswer(this);

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
          final String from_bytes = new String(row.getKey(), ASCII);
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

            KeyValue kv = PowerMockito.mock(KeyValue.class);
            Mockito.when(kv.key()).thenReturn(row.getKey());
            Mockito.when(kv.value()).thenReturn(column.getValue().firstEntry().getValue());
            Mockito.when(kv.qualifier()).thenReturn(column.getKey());
            Mockito.when(kv.timestamp()).thenReturn(column.getValue().firstKey());
            Mockito.when(kv.family()).thenReturn(cf.getKey());
            Mockito.when(kv.toString()).thenReturn("[k '" + bytesToString(row.getKey()) +
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
}
