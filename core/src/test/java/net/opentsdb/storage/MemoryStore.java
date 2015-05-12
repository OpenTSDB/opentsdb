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
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdUtils;
import net.opentsdb.uid.IdentifierDecorator;
import net.opentsdb.uid.TimeseriesId;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import net.opentsdb.uid.UniqueIdType;
import org.hbase.async.Bytes;

import static net.opentsdb.core.StringCoder.fromBytes;
import static net.opentsdb.uid.IdUtils.uidToString;

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
    storage = new Bytes.ByteMap<>();

  private final Table<Long, String, LabelMeta> uid_table;
  private final Table<String, Long, Annotation> annotation_table;

  private final Map<TimeseriesId, NavigableMap<Long, Number>> datapoints;

  private final Map<UniqueIdType, AtomicLong> uid_max = ImmutableMap.of(
          UniqueIdType.METRIC, new AtomicLong(1),
          UniqueIdType.TAGK, new AtomicLong(1),
          UniqueIdType.TAGV, new AtomicLong(1));
  private final Table<String, UniqueIdType, byte[]> uid_forward_mapping;
  private final Table<String, UniqueIdType, String> uid_reverse_mapping;

  private byte[] default_family = "t".getBytes(ASCII);

  /** Incremented every time a new value is stored (without a timestamp) */
  private long current_timestamp = 1388534400000L;

  public MemoryStore() {
    uid_table = HashBasedTable.create();
    annotation_table = HashBasedTable.create();
    uid_forward_mapping = HashBasedTable.create();
    uid_reverse_mapping = HashBasedTable.create();
    datapoints = Maps.newHashMap();
  }

  @Override
  public Deferred<Object> addPoint(final TimeseriesId tsuid,
                                   final long timestamp,
                                   final float value) {
    return addPoint(tsuid, value, timestamp);
  }

  @Override
  public Deferred<Object> addPoint(final TimeseriesId tsuid,
                                   final long timestamp,
                                   final double value) {
    return addPoint(tsuid, value, timestamp);
  }

  @Override
  public Deferred<Object> addPoint(final TimeseriesId tsuid,
                                   final long timestamp,
                                   final long value) {
    return addPoint(tsuid, (Number) value, timestamp);
  }

  private Deferred<Object> addPoint(final TimeseriesId tsuid,
                                    final Number value,
                                    final long timestamp) {
    /*
     * TODO(luuse): tsuid neither implements #equals, #hashCode or Comparable.
     * Should implement a custom TimeseriesId for MemoryStore that implements all
     * of these.
     */
    NavigableMap<Long, Number> tsuidDps = datapoints.get(tsuid);

    if (tsuidDps == null) {
      tsuidDps = Maps.newTreeMap();
      datapoints.put(tsuid, tsuidDps);
    }

    tsuidDps.put(timestamp, value);

    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<com.google.common.base.Optional<byte[]>> getId(String name, UniqueIdType type) {
    byte[] id = uid_forward_mapping.get(name, type);

    if (id != null && id.length != type.width) {
      throw new IllegalStateException("Found id.length = " + id.length
              + " which is != " + type.width
              + " required for '" + type + '\'');
    }

    return Deferred.fromResult(Optional.fromNullable(id));
  }

  @Override
  public Deferred<com.google.common.base.Optional<String>> getName(byte[] id, UniqueIdType type) {
    if (id.length != type.width) {
      throw new IllegalArgumentException("Wrong id.length = " + id.length
              + " which is != " + type.width
              + " required for '" + type + '\'');
    }

    String str_uid = fromBytes(id);

    final String name = uid_reverse_mapping.get(str_uid, type);
    return Deferred.fromResult(Optional.fromNullable(name));
  }

  @Override
  public Deferred<LabelMeta> getMeta(final byte[] uid,
                                     final UniqueIdType type) {
    final String qualifier = type.toString().toLowerCase() + "_meta";
    final long s_uid = IdUtils.uidToLong(uid);

    final LabelMeta meta = uid_table.get(s_uid, qualifier);

    return Deferred.fromResult(meta);
  }

  @Override
  public Deferred<Boolean> updateMeta(final LabelMeta meta) {
    uid_table.put(
        IdUtils.uidToLong(meta.identifier()),
        meta.type().toString().toLowerCase() + "_meta",
        meta);

    return Deferred.fromResult(Boolean.TRUE);
  }

  @Override
  public Deferred<Object> deleteUID(byte[] name, UniqueIdType type) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<List<byte[]>> executeTimeSeriesQuery(final ResolvedSearchQuery query) {
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
        throw new IllegalArgumentException("All Unique IDs for " + type
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
            (IdUtils.uidToLong(uid, (short) uid.length) + 1)));

    String str_uid = fromBytes(uid);

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
      row = new Bytes.ByteMap<>();
      storage.put(key, row);
    }

    Bytes.ByteMap<TreeMap<Long, byte[]>> cf = row.get(family);
    if (cf == null) {
      cf = new Bytes.ByteMap<>();
      row.put(family, cf);
    }
    TreeMap<Long, byte[]> column = cf.get(qualifier);
    if (column == null) {
      // remember, most recent at the top!
      column = new TreeMap<>(Collections.reverseOrder());
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
    return 1;
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
    return new byte[] {};
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

    List<Annotation> annotations = new ArrayList<>();
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

    ArrayList<Annotation> del_list = new ArrayList<>();
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
  public Deferred<Object> delete(TSMeta tsMeta) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Boolean> create(TSMeta tsMeta) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<TSMeta> getTSMeta(byte[] tsuid) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Boolean> syncToStorage(TSMeta tsMeta, Deferred<ArrayList<Object>> uid_group, boolean overwrite) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<List<TSMeta>> executeTimeseriesMetaQuery(final ResolvedSearchQuery query) {
    throw new UnsupportedOperationException("Not implemented yet!");
  }

  @Override
  public Deferred<Boolean> TSMetaExists(String tsuid) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

    /**
   * Attempts to fetch a global or local annotation from storage
   * @param tsuid The TSUID as a byte array. May be null if retrieving a global
   * annotation
   * @param start_time The start time as a Unix epoch timestamp
   * @return A valid annotation object if found, null if not
   */
  @Override
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
   * @throws IllegalArgumentException if bad data was retreived from HBase.
   * @param query
   */
  @Override
  public Deferred<ImmutableList<DataPoints>> executeQuery(final Query query) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<List<IdentifierDecorator>> executeIdQuery(final IdQuery query) {
    Function<UniqueIdType, Boolean> typeMatchFunction = new Function<UniqueIdType, Boolean>() {
      @Override
      public Boolean apply(final UniqueIdType input) {
        return query.getType() == null || query.getType() == input;
      }
    };

    Function<String, Boolean> nameMatchFunction = new Function<String, Boolean>() {
      @Override
      public Boolean apply(final String name) {
        return query.getQuery() == null || name.startsWith(query.getQuery());
      }
    };

    final List<IdentifierDecorator> result = new ArrayList<>();

    for (final Table.Cell<String, UniqueIdType, byte[]> cell : uid_forward_mapping.cellSet()) {
      if (typeMatchFunction.apply(cell.getColumnKey()) &&
          nameMatchFunction.apply(cell.getRowKey())) {
        result.add(new IdentifierDecorator() {
          @Override
          public byte[] getId() {
            return cell.getValue();
          }

          @Override
          public UniqueIdType getType() {
            return cell.getColumnKey();
          }

          @Override
          public String getName() {
            return cell.getRowKey();
          }
        });
      }
    }

    return Deferred.fromResult(result);
  }
}
