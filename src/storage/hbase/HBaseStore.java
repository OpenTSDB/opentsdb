// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.storage.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Const;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Internal;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.Query;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.JSONException;
import org.hbase.async.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.opentsdb.core.StringCoder.fromBytes;
import static net.opentsdb.core.StringCoder.toBytes;
import static net.opentsdb.uid.UniqueId.UniqueIdType;
import static net.opentsdb.storage.hbase.HBaseConst.CHARSET;

/**
 * The HBaseStore that implements the client interface required by TSDB.
 */
public class HBaseStore implements TsdbStore {
  /** Byte used for the qualifier prefix to indicate this is an annotation */
  public static final byte ANNOTATION_QUAL_PREFIX = 0x01;

  private static final Logger LOG = LoggerFactory.getLogger(HBaseStore.class);

  /** Row key of the special row used to track the max ID already assigned. */
  private static final byte[] MAXID_ROW = { 0 };

  private static final byte[] TS_FAMILY = { 't' };

  /**
   * The single column family used by this class.
   */
  private static final byte[] ID_FAMILY = toBytes("id");
  /**
   * The single column family used by this class.
   */
  private static final byte[] NAME_FAMILY = toBytes("name");

  /** The single column family used by this class. */
  private static final byte[] UID_FAMILY = toBytes("name");
  /** Name of the CF where trees and branches are stored. */
  private static final byte[] TREE_FAMILY = toBytes("t");
  /** The tree qualifier */
  private static final byte[] TREE_QUALIFIER = toBytes("tree");
  /** Width of tree IDs in bytes */
  private static final short TREE_ID_WIDTH = 2;
  /** Byte prefix for collision columns */
  private static byte[] COLLISION_PREFIX = toBytes("tree_collision:");
  /** Byte prefix for not matched columns */
  private static byte[] NOT_MATCHED_PREFIX = toBytes("tree_not_matched:");
  /** Byte suffix for collision rows, appended after the tree ID */
  private static byte COLLISION_ROW_SUFFIX = 0x01;
  /** Byte suffix for not matched rows, appended after the tree ID */
  private static byte NOT_MATCHED_ROW_SUFFIX = 0x02;

  final org.hbase.async.HBaseClient client;

  /**
   * Row keys that need to be compacted.
   * Whenever we write a new data point to a row, we add the row key to this
   * set.  Every once in a while, the compaction thread will go through old
   * row keys and will read re-compact them.
   */
  private final CompactionQueue compactionq;

  private final boolean enable_realtime_ts;
  private final boolean enable_realtime_uid;
  private final boolean enable_tsuid_incrementing;
  private final boolean enable_tree_processing;
  private final boolean enable_compactions;

  private final byte[] data_table_name;
  private final byte[] uid_table_name;
  private final byte[] tree_table_name;
  private final byte[] meta_table_name;

  public HBaseStore(final HBaseClient client, final Config config) {
    this.client = checkNotNull(client);
    checkNotNull(config);

    enable_tree_processing = config.enable_tree_processing();
    enable_realtime_ts = config.enable_realtime_ts();
    enable_realtime_uid = config.enable_realtime_uid();
    enable_tsuid_incrementing = config.enable_tsuid_incrementing();
    enable_compactions = config.enable_compactions();

    data_table_name = config.getString("tsd.storage.hbase.data_table").getBytes(HBaseConst.CHARSET);
    uid_table_name = config.getString("tsd.storage.hbase.uid_table").getBytes(HBaseConst.CHARSET);
    tree_table_name = config.getString("tsd.storage.hbase.tree_table").getBytes(HBaseConst.CHARSET);
    meta_table_name = config.getString("tsd.storage.hbase.meta_table").getBytes(HBaseConst.CHARSET);

    client.setFlushInterval(config.getShort("tsd.storage.flush_interval"));

    compactionq = new CompactionQueue(this, config, data_table_name, TS_FAMILY);
  }

  /**
   * Calculate the base time based on a timestamp to be used in a row key.
   * TODO This should be made into an instance method
   */
  public static long buildBaseTime(final long timestamp) {
    if ((timestamp & Const.SECOND_MASK) != 0) {
      // drop the ms timestamp to seconds to calculate the base timestamp
      return ((timestamp / 1000) - ((timestamp / 1000) % Const.MAX_TIMESPAN));
    } else {
      return (timestamp - (timestamp % Const.MAX_TIMESPAN));
    }
  }

  /**
   * Calculate the row key based on a TSUID and the base time.
   * TODO This should be made into an instance method
   */
  public static byte[] buildRowKeyFromTSUID(byte[] tsuid, int base_time) {
    // otherwise we need to build the row key from the TSUID and start time
    final byte[] row = new byte[Const.TIMESTAMP_BYTES + tsuid.length];
    System.arraycopy(tsuid, 0, row, 0, Const.METRICS_WIDTH);
    Bytes.setInt(row, base_time, Const.METRICS_WIDTH);
    System.arraycopy(tsuid, Const.METRICS_WIDTH, row, Const.METRICS_WIDTH +
        Const.TIMESTAMP_BYTES, (tsuid.length - Const.METRICS_WIDTH));
    return row;
  }

  /**
   * Calculates the row key based on the TSUID and the start time. If the TSUID
   * is empty, the row key is a 0 filled byte array {@code TSDB.metrics_width()}
   * wide plus the normalized start timestamp without any tag bytes.
   * @param start_time The start time as a Unix epoch timestamp
   * @param tsuid An optional TSUID if storing a local annotation
   * @return The row key as a byte array
   */
  public static byte[] getAnnotationRowKey(final long start_time, final byte[] tsuid) {
    if (start_time < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    final long base_time = buildBaseTime(start_time);

    // if the TSUID is empty, then we're a global annotation. The row key will
    // just be an empty byte array of metric width plus the timestamp
    if (tsuid == null || tsuid.length < 1) {
      final byte[] row = new byte[Const.METRICS_WIDTH + Const.TIMESTAMP_BYTES];
      Bytes.setInt(row, (int) base_time, Const.METRICS_WIDTH);
      return row;
    }

    return buildRowKeyFromTSUID(tsuid, (int) base_time);
  }

  /**
   * Calculates and returns the column qualifier. The qualifier is the offset
   * of the {@code #start_time} from the row key's base time stamp in seconds
   * with a prefix of {@code #ANNOTATION_QUAL_PREFIX}. Thus if the offset is 0 and the prefix is
   * 1 and the timestamp is in seconds, the qualifier would be [1, 0, 0].
   * Millisecond timestamps will have a 5 byte qualifier
   * @return The column qualifier as a byte array
   * @throws IllegalArgumentException if the start_time has not been set
   */
  public static byte[] getAnnotationQualifier(final long start_time) {
    if (start_time < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    final long base_time;
    final byte[] qualifier;
    long timestamp = start_time;
    // downsample to seconds to save space AND prevent duplicates if the time
    // is on a second boundary (e.g. if someone posts at 1328140800 with value A
    // and 1328140800000L with value B)
    if (timestamp % 1000 == 0) {
      timestamp = timestamp / 1000;
    }

    if ((timestamp & Const.SECOND_MASK) != 0) {
      // drop the ms timestamp to seconds to calculate the base timestamp
      base_time = ((timestamp / 1000) -
          ((timestamp / 1000) % Const.MAX_TIMESPAN));
      qualifier = new byte[5];
      final int offset = (int) (timestamp - (base_time * 1000));
      System.arraycopy(Bytes.fromInt(offset), 0, qualifier, 1, 4);
    } else {
      base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
      qualifier = new byte[3];
      final short offset = (short) (timestamp - base_time);
      System.arraycopy(Bytes.fromShort(offset), 0, qualifier, 1, 2);
    }
    qualifier[0] = ANNOTATION_QUAL_PREFIX;
    return qualifier;
  }

  /**
   * Returns a timestamp after parsing an annotation qualifier.
   * @param qualifier The full qualifier (including prefix) on either 3 or 5 bytes
   * @param base_time The base time from the row in seconds
   * @return A timestamp in milliseconds
   * @since 2.1
   */
  private static long timeFromQualifier(final byte[] qualifier, final long base_time) {
    final long offset;
    if (qualifier.length == 3) {
      offset = Bytes.getUnsignedShort(qualifier, 1);
      return (base_time + offset) * 1000;
    } else {
      offset = Bytes.getUnsignedInt(qualifier, 1);
      return (base_time * 1000) + offset;
    }
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
    /**
     * Called after executing the GetRequest to parse the meta data.
     */
    final class GetCB implements Callback<Deferred<Annotation>,
      ArrayList<KeyValue>> {

      /**
       * @return Null if the meta did not exist or a valid Annotation object if
       * it did.
       */
      @Override
      public Deferred<Annotation> call(final ArrayList<KeyValue> row)
        throws Exception {
        if (row == null || row.isEmpty()) {
          return Deferred.fromResult(null);
        }

        Annotation note = JSON.parseToObject(row.get(0).value(),
            Annotation.class);
        return Deferred.fromResult(note);
      }
    }

    final GetRequest get = new GetRequest(data_table_name, getAnnotationRowKey(start_time, tsuid))
            .family(TS_FAMILY)
            .qualifier(getAnnotationQualifier(start_time));

    return client.get(get).addCallbackDeferring(new GetCB());
  }

  /**
   * Attempts to mark an Annotation object for deletion. Note that if the
   * annoation does not exist in storage, this delete call will not throw an
   * error.
   *
   * @param annotation
   * @return A meaningless Deferred for the caller to wait on until the call is
   * complete. The value may be null.
   */
  @Override
  public Deferred<Object> delete(Annotation annotation) {
    final byte[] tsuid_byte = annotation.getTSUID() != null && !annotation.getTSUID().isEmpty() ?
            UniqueId.stringToUid(annotation.getTSUID()) : null;
    final DeleteRequest delete = new DeleteRequest(data_table_name,
            getAnnotationRowKey(annotation.getStartTime(), tsuid_byte), TS_FAMILY,
            getAnnotationQualifier(annotation.getStartTime()));
    return delete(delete);
  }

  @Override
  public Deferred<Boolean> updateAnnotation(Annotation original, Annotation annotation) {
    final byte[] original_note = original == null ? new byte[0] :
            original.getStorageJSON();

    final byte[] tsuid_byte = !Strings.isNullOrEmpty(annotation.getTSUID()) ?
            UniqueId.stringToUid(annotation.getTSUID()) : null;

    final PutRequest put = new PutRequest(data_table_name,
            getAnnotationRowKey(annotation.getStartTime(), tsuid_byte), TS_FAMILY,
            getAnnotationQualifier(annotation.getStartTime()),
            annotation.getStorageJSON());

    return client.compareAndSet(put, original_note);
  }

  @Override
  public Deferred<Long> bufferAtomicIncrement(AtomicIncrementRequest request) {
    return this.client.bufferAtomicIncrement(request);
  }

  @Override
  public Deferred<Boolean> compareAndSet(PutRequest edit, byte[] expected) {
    return this.client.compareAndSet(edit, expected);
  }

  @Override
  public Deferred<Object> delete(DeleteRequest request) {
    return this.client.delete(request);
  }

  @Override
  public Deferred<ArrayList<Object>> checkNecessaryTablesExist() {
    final ArrayList<Deferred<Object>> checks = new ArrayList<Deferred<Object>>(4);
    checks.add(client.ensureTableExists(data_table_name));
    checks.add(client.ensureTableExists(uid_table_name));

    if (enable_tree_processing) {
      checks.add(client.ensureTableExists(tree_table_name));
    }
    if (enable_realtime_ts ||
        enable_realtime_uid ||
        enable_tsuid_incrementing) {
      checks.add(client.ensureTableExists(meta_table_name));
    }

    return Deferred.group(checks);
  }

  @Override
  public Deferred<Object> flush() {
    final class HClientFlush implements Callback<Object, ArrayList<Object>> {
      public Object call(final ArrayList<Object> args) {
        return client.flush();
      }
      public String toString() {
        return "flush TsdbStore";
      }
    }

    return enable_compactions && compactionq != null
            ? compactionq.flush().addCallback(new HClientFlush())
            : client.flush();
  }

  @Override
  public Deferred<ArrayList<KeyValue>> get(GetRequest request) {
    return this.client.get(request);
  }

  @Override
  public Scanner newScanner(byte[] table) {
    return this.client.newScanner(table);
  }

  @Override
  public Deferred<Object> put(PutRequest request) {
    return this.client.put(request);
  }

  @Override
  public Deferred<Object> addPoint(byte[] row, byte[] qualifier, byte[] value) {
    final PutRequest point = new PutRequest(data_table_name, row, TS_FAMILY,
            qualifier,
            value);

    scheduleForCompaction(row);

    return client.put(point);
  }

  @Override
  public Deferred<Object> shutdown() {
    final class CompactCB implements Callback<Object, ArrayList<Object>> {
      public Object call(ArrayList<Object> compactions) throws Exception {
        return client.shutdown();
      }
    }

    final class CompactEB implements Callback<Object, Exception> {
      public Object call(final Exception e) {
        LOG.error("Failed to shutdown. Received an error when " +
                "flushing the compaction queue", e);
        return client.shutdown();
      }
    }

    if (enable_compactions) {
      LOG.info("Flushing compaction queue");
      return compactionq.flush().addCallbacks(new CompactCB(), new CompactEB());
    } else {
      return client.shutdown();
    }
  }

  @Override
  public void recordStats(StatsCollector col) {
    compactionq.collectStats(col);

    final ClientStats stats = client.stats();

    col.record("hbase.root_lookups", stats.rootLookups());
    col.record("hbase.meta_lookups", stats.uncontendedMetaLookups(), "type=uncontended");
    col.record("hbase.meta_lookups", stats.contendedMetaLookups(), "type=contended");
    col.record("hbase.rpcs", stats.atomicIncrements(), "type=increment");
    col.record("hbase.rpcs", stats.deletes(), "type=delete");
    col.record("hbase.rpcs", stats.gets(), "type=get");
    col.record("hbase.rpcs", stats.puts(), "type=put");
    col.record("hbase.rpcs", stats.rowLocks(), "type=rowLock");
    col.record("hbase.rpcs", stats.scannersOpened(), "type=openScanner");
    col.record("hbase.rpcs", stats.scans(), "type=scan");
    col.record("hbase.rpcs.batched", stats.numBatchedRpcSent());
    col.record("hbase.flushes", stats.flushes());
    col.record("hbase.connections.created", stats.connectionsCreated());
    col.record("hbase.nsre", stats.noSuchRegionExceptions());
    col.record("hbase.nsre.rpcs_delayed", stats.numRpcDelayedDueToNSRE());
  }

  @Override
  public void setFlushInterval(short aShort) {
    this.client.setFlushInterval(aShort);
  }

  @Override
  public long getFlushInterval() {
    return this.client.getFlushInterval();
  }

  @Override
  public Deferred<Long> atomicIncrement(AtomicIncrementRequest air) {
    return this.client.atomicIncrement(air);
  }

  @Override
  public Deferred<Optional<String>> getName(final byte[] id, final UniqueIdType type) {
    if (id.length != type.width) {
      throw new IllegalArgumentException("Wrong id.length = " + id.length
              + " which is != " + type.width
              + " required for '" + type.qualifier + '\'');
    }

    class GetCB implements Callback<Optional<String>, Optional<KeyValue>> {
      public Optional<String> call(final Optional<KeyValue> cell) {
        if (cell.isPresent()) {
          return Optional.of(fromBytes(cell.get().value()));
        }

        return Optional.absent();
      }
    }

    final GetRequest request = new GetRequest(uid_table_name, id)
            .family(NAME_FAMILY)
            .qualifier(type.qualifier.getBytes(HBaseConst.CHARSET));

    return client.get(request)
            .addCallback(new GetCellNotEmptyCB())
            .addCallback(new GetCB());
  }

  @Override
  public Deferred<Optional<byte[]>> getId(final String name, final UniqueIdType type) {
    return getId(toBytes(name), type);
  }

  private Deferred<Optional<byte[]>> getId(final byte[] name, final UniqueIdType type) {
    final byte[] qualifier = type.qualifier.getBytes(HBaseConst.CHARSET);

    final GetRequest get = new GetRequest(uid_table_name, name)
            .family(ID_FAMILY)
            .qualifier(qualifier);

    class GetCB implements Callback<Optional<byte[]>, Optional<KeyValue>> {
      public Optional<byte[]> call(final Optional<KeyValue> cell) {
        if (cell.isPresent()) {
          byte[] id = cell.get().value();

          if (id.length != type.width) {
            throw new IllegalStateException("Found id.length = " + id.length
                    + " which is != " + type.width
                    + " required for '" + type.qualifier + '\'');
          }

          return Optional.of(id);
        }

        return Optional.absent();
      }
    }

    return client.get(get)
            .addCallback(new GetCellNotEmptyCB())
            .addCallback(new GetCB());
  }

  /**
   * Attempts to store a blank, new UID meta object in the proper location.
   * <b>Warning:</b> This should not be called by user accessible methods as it
   * will overwrite any data already in the column. This method does not use
   * a CAS, instead it uses a PUT to overwrite anything in the column.
   *
   * @param meta The meta object to store
   * @return A deferred without meaning. The response may be null and should
   * only be used to track completion.
   * @throws org.hbase.async.HBaseException   if there was an issue writing to storage
   * @throws IllegalArgumentException         if data was missing
   * @throws net.opentsdb.utils.JSONException if the object could not be serialized
   */
  @Override
  public Deferred<Object> add(final UIDMeta meta) {
    final PutRequest put = new PutRequest(uid_table_name,
      UniqueId.stringToUid(meta.getUID()), UID_FAMILY,
      (meta.getType().toString().toLowerCase() + "_meta").getBytes(HBaseConst.CHARSET),
      meta.getStorageJSON());

    return client.put(put);
  }

  /**
   * Attempts to delete the meta object from storage
   *
   * @param meta The meta object to delete
   * @return A deferred without meaning. The response may be null and should
   * only be used to track completion.
   * @throws org.hbase.async.HBaseException if there was an issue
   * @throws IllegalArgumentException       if data was missing (uid and type)
   */
  @Override
  public Deferred<Object> delete(final UIDMeta meta) {
    final DeleteRequest delete = new DeleteRequest(uid_table_name,
      UniqueId.stringToUid(meta.getUID()), UID_FAMILY,
      (meta.getType().toString().toLowerCase() + "_meta").getBytes(HBaseConst.CHARSET));
    return client.delete(delete);
  }

  @Override
  public Deferred<Boolean> updateMeta(final UIDMeta meta, final boolean overwrite) {

    return getMeta(meta.getUID().getBytes(HBaseConst.CHARSET),
      meta.getType()).addCallbackDeferring(

      /**
       *  Nested callback used to merge and store the meta data after verifying
       *  that the UID mapping exists. It has to access the {@code local_meta}
       *  object so that's why it's nested within the NameCB class
       */
      new Callback<Deferred<Boolean>, Optional<KeyValue>>() {
        /**
         * Executes the CompareAndSet after merging changes
         * @return True if the CAS was successful, false if the stored data
         * was modified during flight.
         */

      @Override
      public Deferred<Boolean> call(Optional<KeyValue> cell) throws Exception {
        final UIDMeta stored_meta;
        final byte[] original_meta;

        if (cell.isPresent()) {
          original_meta = cell.get().value();
          stored_meta = JSON.parseToObject(original_meta, UIDMeta.class);
          stored_meta.resetChangedMap();
        } else {
          original_meta = new byte[0];
          stored_meta = null;
        }

        if (stored_meta != null) {
          meta.syncMeta(stored_meta, overwrite);
        }

        final PutRequest put = new PutRequest(uid_table_name,
                UniqueId.stringToUid(meta.getUID()),
                UID_FAMILY,
                toBytes(meta.getType().toString().toLowerCase() + "_meta"),
                meta.getStorageJSON());
        return client.compareAndSet(put, original_meta);
      }
    });
  }


  private Deferred<Optional<KeyValue>> getMeta(byte[] uid, final UniqueIdType type) {
    final byte[] qual = toBytes(type.toString().toLowerCase() + "_meta");

    final GetRequest request = new GetRequest(uid_table_name, uid)
            .family(UID_FAMILY)
            .qualifier(qual);

    return client.get(request).addCallback(new GetCellNotEmptyCB());
  }

  @Override
  public Deferred<UIDMeta> getMeta(final byte[] uid, final String name,
                                   final UniqueIdType type) {
    /**
     * Inner class called to retrieve the meta data after verifying that the
     * name mapping exists. It requires the name to set the default, hence
     * the reason it's nested.
     */
    class FetchMetaCB implements Callback<UIDMeta, Optional<KeyValue>> {
      /**
       * Called to parse the response of our storage GET call after
       * verification
       * @return The stored UIDMeta or a default object if the meta data
       * did not exist
       */
      @Override
      public UIDMeta call(Optional<KeyValue> cell)
        throws Exception {
        if (!cell.isPresent()) {
          // return the default
          return new UIDMeta(type,
            uid, name, false);
        }

        UniqueIdType effective_type = type;
        if (effective_type == null) {
          final String qualifier =
            new String(cell.get().qualifier(), HBaseConst.CHARSET);
          effective_type = UniqueId.stringToUniqueIdType(qualifier.substring(0,
            qualifier.indexOf("_meta")));
        }

        UIDMeta return_meta = UIDMeta.buildFromJSON(cell.get().value(),
                effective_type, uid, name);

        return return_meta;
      }
    }

    return getMeta(uid, type).addCallback(new FetchMetaCB());
  }

  // ------------------ //
  // Compaction helpers //
  // ------------------ //
  /**
   * Schedules the given row key for later re-compaction.
   * Once this row key has become "old enough", we'll read back all the data
   * points in that row, write them back to TsdbStore in a more compact fashion,
   * and delete the individual data points.
   * @param row The row key to re-compact later.  Will not be modified.
   */
  @Override
  public final void scheduleForCompaction(final byte[] row) {
    if (enable_compactions) {
      compactionq.add(row);
    }
  }

  /**
   * Delete the UID with the key specified by name with the qualifier kind.
   * This only removes the forward mapping. The reverse mapping will not be
   * removed.
   * @param name The UID key to remove
   * @param kind The qualifier of the UID to remove
   * @return A deferred that indicated the completion of the request. The
   * contained object has no special meaning and may be null.
   */
  @Override
  public Deferred<Object> deleteUID(final byte[] name, final byte[] kind) {
    try {
      final DeleteRequest request = new DeleteRequest(
              uid_table_name, name, ID_FAMILY, kind);
      return client.delete(request);
    } catch (HBaseException e) {
      LOG.error("When deleting(\"{}\", on {}: Failed to remove the mapping" +
              "for (key, qualifier) = ({}, {}). ", name, this, name, kind, e);
      throw e;
    }
  }

  @Override
  public Deferred<byte[]> allocateUID(final String name,
                                      final UniqueIdType type) {
    class IdCB implements Callback<Deferred<byte[]>, Long> {
      @Override
      public Deferred<byte[]> call(Long id) throws Exception {
        if (id <= 0) {
          throw new IllegalStateException("Got a negative ID from HBase: " + id);
        }

        LOG.info("Got ID={} for kind='{}' name='{}'", id, type.qualifier, name);

        final byte[] row = Bytes.fromLong(id);

        // row.length should actually be 8.
        if (row.length < type.width) {
          throw new IllegalStateException("row.length = " + row.length
                  + " which is less than " + type.width
                  + " for id=" + id
                  + " row=" + Arrays.toString(row));
        }

        // Verify that the indices in the row array that won't be used in the
        // uid with the current length are zero so we haven't reached the upper
        // limits.
        for (int i = 0; i < row.length - type.width; i++) {
          if (row[i] != 0) {
            throw new IllegalStateException("All Unique IDs for " + type.qualifier
                    + " on " + type.width + " bytes are already assigned!");
          }
        }

        // Shrink the ID on the requested number of bytes.
        final byte[] uid = Arrays.copyOfRange(row, row.length - type.width,
                row.length);

        return allocateUID(name, uid, type);
      }
    }

    final byte[] qualifier = type.qualifier.getBytes(HBaseConst.CHARSET);
    Deferred<Long> new_id_d = client.atomicIncrement(
            new AtomicIncrementRequest(
                    uid_table_name,
                    MAXID_ROW,
                    ID_FAMILY,
                    qualifier));

    return new_id_d.addCallbackDeferring(new IdCB());
  }

  /**
   * Allocate a new UID with name and uid for the UID type kind. This will
   * create a reverse and forward mapping in HBase using two {@link org.hbase
   * .async.PutRequest}s.
   * @param name The name of the new UID
   * @param uid The UID to asign to the name
   * @param type The type of the UID.
   */
  @Override
  public Deferred<byte[]> allocateUID(final String name,
                                      final byte[] uid,
                                      final UniqueIdType type) {
    // Create the reverse mapping first, so that if we die before updating
    // the forward mapping we don't run the risk of "publishing" a
    // partially assigned ID.  The reverse mapping on its own is harmless
    // but the forward mapping without reverse mapping is bad.
    //
    // We are CAS'ing the KV into existence -- the second argument is how
    // we tell HBase we want to atomically create the KV, so that if there
    // is already a KV in this cell, we'll fail.  Technically we could do
    // just a `put' here, as we have a freshly allocated UID, so there is
    // not reason why a KV should already exist for this UID, but just to
    // err on the safe side and catch really weird corruption cases, we do
    // a CAS instead to create the KV.
    class ReverseCB implements Callback<Deferred<Boolean>, Boolean> {
      private final PutRequest reverse_request;
      private final PutRequest forward_request;

      ReverseCB(PutRequest reverse_request, PutRequest forward_request) {
        this.reverse_request = reverse_request;
        this.forward_request = forward_request;
      }

      @Override
      public Deferred<Boolean> call(Boolean created) throws Exception {
        if (created) {
          return client.compareAndSet(forward_request, HBaseClient.EMPTY_ARRAY);
        } else {
          throw new IllegalStateException("CAS to create mapping when " +
                  "allocating UID with request " + reverse_request + " failed. " +
                  "You should probably run a FSCK against the UID table.");
        }
      }
    }

    class ForwardCB implements Callback<Deferred<byte[]>, Boolean> {
      private final PutRequest request;
      private final byte[] uid;

      public ForwardCB(final PutRequest request, byte[] uid) {
        this.request = request;
        this.uid = uid;
      }

      @Override
      public Deferred<byte[]> call(Boolean created) throws Exception {
        if (created) {
          return Deferred.fromResult(uid);
        } else {
          // If two TSDs attempted to allocate a UID for the same name at the
          // same time, they would both have allocated a UID, and created a
          // reverse mapping, and upon getting here, only one of them would
          // manage to CAS this KV into existence.  The one that loses the
          // race will retry and discover the UID assigned by the winner TSD,
          // and a UID will have been wasted in the process.  No big deal.
          LOG.warn("Race condition on CAS to create forward mapping for uid " +
                  "{} with request {}. Another TSDB instance must have " +
                  "allocated this uid concurrently.", uid, request);

          return getId(name, type).addCallback(new Callback<byte[], Optional<byte[]>>() {
            @Override
            public byte[] call(Optional<byte[]> id) throws Exception {
              // Calling #get() here is safe since the failed CAS above
              // indicates that the id should exist.
              return id.get();
            }
          });
        }
      }
    }

    final byte[] b_name = toBytes(name);
    final byte[] qualifier = type.qualifier.getBytes(HBaseConst.CHARSET);
    final PutRequest reverse_mapping = new PutRequest(uid_table_name, uid, NAME_FAMILY, qualifier, b_name);
    final PutRequest forward_mapping = new PutRequest(uid_table_name, b_name, ID_FAMILY, qualifier, uid);

    return client.compareAndSet(reverse_mapping, HBaseClient.EMPTY_ARRAY)
            .addCallbackDeferring(new ReverseCB(reverse_mapping, forward_mapping))
            .addCallbackDeferring(new ForwardCB(forward_mapping, uid));
  }

  @Override
  public Deferred<List<Annotation>> getGlobalAnnotations(final long start_time, final long end_time) {
    /**
     * Scanner that loops through the [0, 0, 0, timestamp] rows looking for
     * global annotations. Returns a list of parsed annotation objects.
     * The list may be empty.
     */
    final class ScannerCB implements Callback<Deferred<List<Annotation>>,
      ArrayList<ArrayList<KeyValue>>> {
      final Scanner scanner;
      final List<Annotation> annotations = new ArrayList<Annotation>();

      /**
       * Initializes the scanner
       */
      public ScannerCB() {
        final byte[] start = new byte[Const.METRICS_WIDTH +
                                      Const.TIMESTAMP_BYTES];
        final byte[] end = new byte[Const.METRICS_WIDTH +
                                    Const.TIMESTAMP_BYTES];

        final long normalized_start = (start_time -
            (start_time % Const.MAX_TIMESPAN));
        final long normalized_end = (end_time -
            (end_time % Const.MAX_TIMESPAN));

        Bytes.setInt(start, (int) normalized_start, Const.METRICS_WIDTH);
        Bytes.setInt(end, (int) normalized_end, Const.METRICS_WIDTH);

        scanner = client.newScanner(data_table_name);
        scanner.setStartKey(start);
        scanner.setStopKey(end);
        scanner.setFamily(TS_FAMILY);
      }

      public Deferred<List<Annotation>> scan() {
        return scanner.nextRows().addCallbackDeferring(this);
      }

      @Override
      public Deferred<List<Annotation>> call (
          final ArrayList<ArrayList<KeyValue>> rows) throws Exception {
        if (rows == null || rows.isEmpty()) {
          return Deferred.fromResult(annotations);
        }

        for (final ArrayList<KeyValue> row : rows) {
          for (KeyValue column : row) {
            if ((column.qualifier().length == 3 || column.qualifier().length == 5)
                && column.qualifier()[0] == ANNOTATION_QUAL_PREFIX) {
              Annotation note = JSON.parseToObject(row.get(0).value(),
                      Annotation.class);
              if (note.getStartTime() < start_time || note.getEndTime() > end_time) {
                continue;
              }
              annotations.add(note);
            }
          }
        }

        return scan();
      }

    }

    return new ScannerCB().scan();
  }

  @Override
  public Deferred<Integer> deleteAnnotationRange(final byte[] tsuid,
                                                 final long start_time,
                                                 final long end_time) {
    final List<Deferred<Object>> delete_requests = new ArrayList<Deferred<Object>>();
    int width = tsuid != null ? tsuid.length + Const.TIMESTAMP_BYTES :
      Const.METRICS_WIDTH + Const.TIMESTAMP_BYTES;
    final byte[] start_row = new byte[width];
    final byte[] end_row = new byte[width];

    // downsample to seconds for the row keys
    final long start = start_time / 1000;
    final long end = end_time / 1000;
    final long normalized_start = (start - (start % Const.MAX_TIMESPAN));
    final long normalized_end = (end - (end % Const.MAX_TIMESPAN));
    Bytes.setInt(start_row, (int) normalized_start, Const.METRICS_WIDTH);
    Bytes.setInt(end_row, (int) normalized_end, Const.METRICS_WIDTH);

    if (tsuid != null) {
      // first copy the metric UID then the tags
      System.arraycopy(tsuid, 0, start_row, 0, Const.METRICS_WIDTH);
      System.arraycopy(tsuid, 0, end_row, 0, Const.METRICS_WIDTH);
      width = Const.METRICS_WIDTH + Const.TIMESTAMP_BYTES;
      final int remainder = tsuid.length - Const.METRICS_WIDTH;
      System.arraycopy(tsuid, Const.METRICS_WIDTH, start_row, width, remainder);
      System.arraycopy(tsuid, Const.METRICS_WIDTH, end_row, width, remainder);
    }

    /**
     * Iterates through the scanner results in an asynchronous manner, returning
     * once the scanner returns a null result set.
     */
    final class ScannerCB implements Callback<Deferred<List<Deferred<Object>>>,
        ArrayList<ArrayList<KeyValue>>> {
      final Scanner scanner;

      public ScannerCB() {
        scanner = client.newScanner(data_table_name);
        scanner.setStartKey(start_row);
        scanner.setStopKey(end_row);
        scanner.setFamily(TS_FAMILY);
        if (tsuid != null) {
          final List<String> tsuids = new ArrayList<String>(1);
          tsuids.add(UniqueId.uidToString(tsuid));
          Internal.createAndSetTSUIDFilter(scanner, tsuids);
        }
      }

      public Deferred<List<Deferred<Object>>> scan() {
        return scanner.nextRows().addCallbackDeferring(this);
      }

      @Override
      public Deferred<List<Deferred<Object>>> call (
          final ArrayList<ArrayList<KeyValue>> rows) throws Exception {
        if (rows == null || rows.isEmpty()) {
          return Deferred.fromResult(delete_requests);
        }

        for (final ArrayList<KeyValue> row : rows) {
          final long base_time = RowKey.baseTime(row.get(0).key());
          for (KeyValue column : row) {
            if ((column.qualifier().length == 3 || column.qualifier().length == 5)
                && column.qualifier()[0] == ANNOTATION_QUAL_PREFIX) {
              final long timestamp = timeFromQualifier(column.qualifier(),
                      base_time);
              if (timestamp < start_time || timestamp > end_time) {
                continue;
              }
              final DeleteRequest delete = new DeleteRequest(data_table_name,
                  column.key(), TS_FAMILY, column.qualifier());
              delete_requests.add(client.delete(delete));
            }
          }
        }
        return scan();
      }
    }

    /** Called when the scanner is done. Delete requests may still be pending */
    final class ScannerDoneCB implements Callback<Deferred<ArrayList<Object>>,
      List<Deferred<Object>>> {
      @Override
      public Deferred<ArrayList<Object>> call(final List<Deferred<Object>> deletes)
          throws Exception {
        return Deferred.group(delete_requests);
      }
    }

    /** Waits on the group of deferreds to complete before returning the count */
    final class GroupCB implements Callback<Deferred<Integer>, ArrayList<Object>> {
      @Override
      public Deferred<Integer> call(final ArrayList<Object> deletes)
          throws Exception {
        return Deferred.fromResult(deletes.size());
      }
    }

    Deferred<ArrayList<Object>> scanner_done = new ScannerCB().scan()
        .addCallbackDeferring(new ScannerDoneCB());
    return scanner_done.addCallbackDeferring(new GroupCB());
  }

  /**
   * Finds all the {@link net.opentsdb.core.DataPoints} that match this query.
   * This is what actually scans the HBase table and loads the data into
   * {@link net.opentsdb.core.DataPoints}.
   * @throws org.hbase.async.HBaseException if there was a problem communicating with HBase to
   * perform the search.
   * @throws IllegalArgumentException if bad data was retreived from HBase.
   * @param query The query object that specifies the filters
   */
  @Override
  public Deferred<ImmutableList<DataPoints>> executeQuery(final Query query) {
    class QueryCB implements Callback<ImmutableList<DataPoints>, ImmutableList<CompactedRow>> {
      @Override
      public ImmutableList<DataPoints> call(final ImmutableList<CompactedRow>row_parts) {
        return ImmutableList.<DataPoints>copyOf(row_parts);
      }
    }

    QueryRunner r = new QueryRunner(
            query,
            client,
            compactionq,
            data_table_name,
            TS_FAMILY);

    return r.run().addCallback(new QueryCB());
  }

  /**
   * Callback that makes sure that the returned result is not empty or null
   * and then returns the first cell.
   */
  private static class GetCellNotEmptyCB implements
          Callback<Optional<KeyValue>,
          ArrayList<KeyValue>> {
    @Override
    public Optional<KeyValue> call(ArrayList<KeyValue> cells)throws Exception {
      if (cells == null || cells.isEmpty()) {
        return Optional.absent();
      } else {
        return Optional.of(cells.get(0));
      }
    }
  }

   //TREE_STUFF TODO remove just here so all tree stuff will be grouped

  /**
   * Attempts to fetch the given tree from storage, loading the rule set at
   * the same time.
   * Do not use this methods directly but call by using TSDB.
   *
   * @param tree_id The Tree id to fetch
   * @return A tree object if found, null if the tree did not exist
   * @throws IllegalArgumentException if the tree ID was invalid
   * @throws HBaseException if a storage exception occurred
   * @throws JSONException if the object could not be deserialized
   */
  @Override
  public Deferred<Tree> fetchTree(int tree_id) {

    /**
     * Called from the GetRequest with results from storage. Loops through the
     * columns and loads the tree definition and rules
     */
    final class FetchTreeCB implements Callback<Deferred<Tree>,
            ArrayList<KeyValue>> {

      @Override
      public Deferred<Tree> call(ArrayList<KeyValue> row) throws Exception {
        if (row == null || row.isEmpty()) {
          return Deferred.fromResult(null);
        }

        final Tree tree = new Tree();

        // WARNING: Since the JSON in storage doesn't store the tree ID, we need
        // to loadi t from the row key.
        tree.setTreeId(Tree.bytesToId(row.get(0).key()));

        for (KeyValue column : row) {
          if (Bytes.memcmp(TREE_QUALIFIER, column.qualifier()) == 0) {
            // it's *this* tree. We deserialize to a new object and copy
            // since the columns could be in any order and we may get a rule
            // before the tree object
            final Tree local_tree = JSON.parseToObject(column.value(), Tree.class);
            tree.setCreated(local_tree.getCreated());
            tree.setDescription(local_tree.getDescription());
            tree.setName(local_tree.getName());
            tree.setNotes(local_tree.getNotes());
            tree.setStrictMatch(local_tree.getStrictMatch());
            tree.setEnabled(local_tree.getEnabled());
            tree.setStoreFailures(local_tree.getStoreFailures());
            // Tree rule
          } else if (Bytes.memcmp(TreeRule.RULE_PREFIX(), column.qualifier(), 0,
                  TreeRule.RULE_PREFIX().length) == 0) {
            final TreeRule rule = TreeRule.parseFromStorage(column);
            tree.addRule(rule);
          }
        }

        return Deferred.fromResult(tree);
      }

    }
    // fetch the whole row
    final GetRequest get = new GetRequest(tree_table_name, Tree.idToBytes(tree_id));
    get.family(TREE_FAMILY);

    // issue the get request
    return client.get(get).addCallbackDeferring(new FetchTreeCB());
  }

  /**
   * Attempts to store the tree definition via a CompareAndSet call.
   * Do not use this methods directly but call by using TSDB.
   *
   * @param tree The Tree to be stored.
   * @param overwrite Whether or not tree data should be overwritten
   * @return True if the write was successful, false if an error occurred
   * @throws IllegalArgumentException if the tree ID is missing or invalid
   * @throws HBaseException if a storage exception occurred
   */
  @Override
  public Deferred<Boolean> storeTree(final Tree tree, final boolean overwrite) {

    /**
     * Callback executed after loading a tree from storage so that we can
     * synchronize changes to the meta data and write them back to storage.
     */
    final class StoreTreeCB implements Callback<Deferred<Boolean>, Tree> {

      final private Tree local_tree;

      public StoreTreeCB(final Tree local_tree) {
        this.local_tree = local_tree;
      }
      /**
       * Synchronizes the stored tree object (if found) with the local tree
       * and issues a CAS call to write the update to storage.
       * @return True if the CAS was successful, false if something changed
       * in flight
       */
      @Override
      public Deferred<Boolean> call(final Tree fetched_tree) throws Exception {

        Tree stored_tree = fetched_tree;
        final byte[] original_tree = stored_tree == null ? new byte[0] :
                stored_tree.toStorageJson();
        // now copy changes
        if (stored_tree == null) {
          stored_tree = local_tree;
        } else {
          stored_tree.copyChanges(local_tree, overwrite);
        }
        // reset the change map so we don't keep writing
        tree.initializeChangedMap();

        final PutRequest put = new PutRequest(tree_table_name,
                Tree.idToBytes(tree.getTreeId()), TREE_FAMILY, TREE_QUALIFIER,
                stored_tree.toStorageJson());
        return client.compareAndSet(put, original_tree);
      }
    }
    return fetchTree(tree.getTreeId()).addCallbackDeferring(new StoreTreeCB(tree));
  }
  /**
   * Attempts to store the local tree in a new row, automatically assigning a
   * new tree ID and returning the value.
   * This method will scan the UID table for the maximum tree ID, increment it,
   * store the new tree, and return the new ID. If no trees have been created,
   * the returned ID will be "1". If we have reached the limit of trees for the
   * system, as determined by {@link #TREE_ID_WIDTH}, we will throw an exception.
   * @param tree The Tree to store.
   * @return A positive ID, greater than 0 if successful, 0 if there was
   * an error
   */
  @Override
  public Deferred<Integer> createNewTree(final Tree tree) {


    /**
     * Called after a successful CAS to store the new tree with the new ID.
     * Returns the new ID if successful, 0 if there was an error
     */
    final class CreatedCB implements Callback<Deferred<Integer>, Boolean> {

      @Override
      public Deferred<Integer> call(final Boolean cas_success)
              throws Exception {
        return Deferred.fromResult(tree.getTreeId());
      }

    }

    /**
     * Called after fetching all trees. Loops through the tree definitions and
     * determines the max ID so we can increment and write a new one
     */
    final class CreateNewCB implements Callback<Deferred<Integer>, List<Tree>> {

      private final Tree local_tree;

      public CreateNewCB(Tree tree) {
        local_tree = tree;
      }

      @Override
      public Deferred<Integer> call(List<Tree> trees) throws Exception {
        int max_id = 0;
        if (trees != null) {
          for (Tree tree : trees) {
            if (tree.getTreeId() > max_id) {
              max_id = tree.getTreeId();
            }
          }
        }

        local_tree.setTreeId(max_id + 1);
        if (local_tree.getTreeId() > Const.MAX_TREE_ID_EXCLUSIVE) {
          throw new IllegalStateException("Exhausted all Tree IDs");
        }

        return storeTree(local_tree, true).addCallbackDeferring(new CreatedCB());
      }

    }

    // starts the process by fetching all tree definitions from storage
    return fetchAllTrees().addCallbackDeferring(new CreateNewCB(tree));
  }

  /**
   * Attempts to retrieve all trees from the UID table, including their rules.
   * If no trees were found, the result will be an empty list
   * @return A list of tree objects. May be empty if none were found
   */
  @Override
  public Deferred<List<Tree>> fetchAllTrees() {

    final Deferred<List<Tree>> result = new Deferred<List<Tree>>();

    /**
     * Scanner callback that recursively calls itself to load the next set of
     * rows from storage. When the scanner returns a null, the callback will
     * return with the list of trees discovered.
     */
    final class AllTreeScanner implements Callback<Object,
            ArrayList<ArrayList<KeyValue>>> {

      private final List<Tree> trees = new ArrayList<Tree>();
      private final Scanner scanner;

      public AllTreeScanner() {
        scanner = setupAllTreeScanner();
      }

      /**
       * Fetches the next set of results from the scanner and adds this class
       * as a callback.
       * @return A list of trees if the scanner has reached the end
       */
      public Object fetchTrees() {
        return scanner.nextRows().addCallback(this);
      }

      @Override
      public Object call(ArrayList<ArrayList<KeyValue>> rows)
              throws Exception {
        if (rows == null) {
          result.callback(trees);
          return null;
        }

        for (ArrayList<KeyValue> row : rows) {
          final Tree tree = new Tree();
          for (KeyValue column : row) {
            if (column.qualifier().length >= TREE_QUALIFIER.length &&
                    Bytes.memcmp(TREE_QUALIFIER, column.qualifier()) == 0) {
              // it's *this* tree. We deserialize to a new object and copy
              // since the columns could be in any order and we may get a rule
              // before the tree object
              final Tree local_tree = JSON.parseToObject(column.value(),
                      Tree.class);

              tree.setCreated(local_tree.getCreated());
              tree.setDescription(local_tree.getDescription());
              tree.setName(local_tree.getName());
              tree.setNotes(local_tree.getNotes());
              tree.setStrictMatch(local_tree.getStrictMatch());
              tree.setEnabled(local_tree.getEnabled());
              tree.setStoreFailures(local_tree.getStoreFailures());

              // WARNING: Since the JSON data in storage doesn't contain the tree
              // ID, we need to parse it from the row key
              tree.setTreeId(Tree.bytesToId(row.get(0).key()));

              // tree rule
            } else if (column.qualifier().length > TreeRule.RULE_PREFIX().length &&
                    Bytes.memcmp(TreeRule.RULE_PREFIX(), column.qualifier(),
                            0, TreeRule.RULE_PREFIX().length) == 0) {
              final TreeRule rule = TreeRule.parseFromStorage(column);
              tree.addRule(rule);
            }
          }

          // only add the tree if we parsed a valid ID
          if (tree.getTreeId() > 0) {
            trees.add(tree);
          }
        }

        // recurse to get the next set of rows from the scanner
        return fetchTrees();
      }

    }

    // start the scanning process
    new AllTreeScanner().fetchTrees();
    return result;
  }

  @Override
  public Deferred<Boolean> deleteTree(final int tree_id,
                                      final boolean delete_definition) {
    // scan all of the rows starting with the tree ID. We can't just delete the
    // rows as there may be other types of data. Thus we have to check the
    // qualifiers of every column to see if it's safe to delete
    final byte[] start = Tree.idToBytes(tree_id);
    final byte[] end = Tree.idToBytes(tree_id + 1);
    final Scanner scanner = newScanner(tree_table_name);
    scanner.setStartKey(start);
    scanner.setStopKey(end);
    scanner.setFamily(TREE_FAMILY);

    final Deferred<Boolean> completed = new Deferred<Boolean>();

    /**
     * Scanner callback that loops through all rows between tree id and
     * tree id++ searching for tree related columns to delete.
     */
    final class DeleteTreeScanner implements Callback<Deferred<Boolean>,
            ArrayList<ArrayList<KeyValue>>> {

      // list where we'll store delete requests for waiting on
      private final ArrayList<Deferred<Object>> delete_deferreds =
              new ArrayList<Deferred<Object>>();

      /**
       * Fetches the next set of rows from the scanner and adds this class as
       * a callback
       * @return The list of delete requests when the scanner returns a null set
       */
      public Deferred<Boolean> deleteTree() {
        return scanner.nextRows().addCallbackDeferring(this);
      }

      @Override
      public Deferred<Boolean> call(ArrayList<ArrayList<KeyValue>> rows)
              throws Exception {
        if (rows == null) {
          completed.callback(true);
          return null;
        }

        for (final ArrayList<KeyValue> row : rows) {
          // one delete request per row. We'll almost always delete the whole
          // row, so just preallocate the entire row.
          ArrayList<byte[]> qualifiers = new ArrayList<byte[]>(row.size());
          for (KeyValue column : row) {
            // tree
            if (delete_definition && Bytes.equals(TREE_QUALIFIER, column.qualifier())) {
              LOG.trace("Deleting tree defnition in row: " +
                      Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());

              // branches
            } else if (Bytes.equals(Branch.BRANCH_QUALIFIER(), column.qualifier())) {
              LOG.trace("Deleting branch in row: " +
                      Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());

              // leaves
            } else if (column.qualifier().length > Leaf.LEAF_PREFIX().length &&
                    Bytes.memcmp(Leaf.LEAF_PREFIX(), column.qualifier(), 0,
                            Leaf.LEAF_PREFIX().length) == 0) {
              LOG.trace("Deleting leaf in row: " +
                      Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());

              // collisions
            } else if (column.qualifier().length > COLLISION_PREFIX.length &&
                    Bytes.memcmp(COLLISION_PREFIX, column.qualifier(), 0,
                            COLLISION_PREFIX.length) == 0) {
              LOG.trace("Deleting collision in row: " +
                      Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());

              // not matched
            } else if (column.qualifier().length > NOT_MATCHED_PREFIX.length &&
                    Bytes.memcmp(NOT_MATCHED_PREFIX, column.qualifier(), 0,
                            NOT_MATCHED_PREFIX.length) == 0) {
              LOG.trace("Deleting not matched in row: " +
                      Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());

              // tree rule
            } else if (delete_definition && column.qualifier().length > TreeRule.RULE_PREFIX().length &&
                    Bytes.memcmp(TreeRule.RULE_PREFIX(), column.qualifier(), 0,
                            TreeRule.RULE_PREFIX().length) == 0) {
              LOG.trace("Deleting tree rule in row: " +
                      Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());
            }
          }

          if (qualifiers.size() > 0) {
            final DeleteRequest delete = new DeleteRequest(tree_table_name,
                    row.get(0).key(), TREE_FAMILY,
                    qualifiers.toArray(new byte[qualifiers.size()][])
            );
            delete_deferreds.add(delete(delete));
          }
        }

        /**
         * Callback used as a kind of buffer so that we don't wind up loading
         * thousands or millions of delete requests into memory and possibly run
         * into a StackOverflowError or general OOM. The scanner defaults are
         * our limit so each pass of the scanner will wait for the previous set
         * of deferreds to complete before continuing
         */
        final class ContinueCB implements Callback<Deferred<Boolean>,
                ArrayList<Object>> {

          public Deferred<Boolean> call(ArrayList<Object> objects) {
            LOG.debug("Purged [" + objects.size() + "] columns, continuing");
            delete_deferreds.clear();
            // call ourself again to get the next set of rows from the scanner
            return deleteTree();
          }

        }

        // call ourself again after waiting for the existing delete requests
        // to complete
        Deferred.group(delete_deferreds).addCallbackDeferring(new ContinueCB());
        return null;
      }
    }

    // start the scanner
    new DeleteTreeScanner().deleteTree();
    return completed;
  }

  @Override
  public Deferred<Map<String, String>> fetchCollisions(int tree_id, List<String> tsuids) {

    final byte[] row_key = new byte[TREE_ID_WIDTH + 1];
    System.arraycopy(Tree.idToBytes(tree_id), 0, row_key, 0, TREE_ID_WIDTH);
    row_key[TREE_ID_WIDTH] = COLLISION_ROW_SUFFIX;

    final GetRequest get = new GetRequest(tree_table_name, row_key);
    get.family(TREE_FAMILY);

    // if the caller provided a list of TSUIDs, then we need to compile a list
    // of qualifiers so we only fetch those columns.
    if (tsuids != null && !tsuids.isEmpty()) {
      final byte[][] qualifiers = new byte[tsuids.size()][];
      int index = 0;
      for (String tsuid : tsuids) {
        final byte[] qualifier = new byte[COLLISION_PREFIX.length +
                (tsuid.length() / 2)];
        System.arraycopy(COLLISION_PREFIX, 0, qualifier, 0,
                COLLISION_PREFIX.length);
        final byte[] tsuid_bytes = UniqueId.stringToUid(tsuid);
        System.arraycopy(tsuid_bytes, 0, qualifier, COLLISION_PREFIX.length,
                tsuid_bytes.length);
        qualifiers[index] = qualifier;
        index++;
      }
      get.qualifiers(qualifiers);
    }
    /**
     * Called after issuing the row get request to parse out the results and
     * compile the list of collisions.
     */
    final class GetCB implements Callback<Deferred<Map<String, String>>,
            ArrayList<KeyValue>> {

      @Override
      public Deferred<Map<String, String>> call(final ArrayList<KeyValue> row)
              throws Exception {
        if (row == null || row.isEmpty()) {
          final Map<String, String> empty = new HashMap<String, String>(0);
          return Deferred.fromResult(empty);
        }

        final Map<String, String> collisions =
                new HashMap<String, String>(row.size());

        for (KeyValue column : row) {
          if (column.qualifier().length > COLLISION_PREFIX.length &&
                  Bytes.memcmp(COLLISION_PREFIX, column.qualifier(), 0,
                          COLLISION_PREFIX.length) == 0) {
            final byte[] parsed_tsuid = Arrays.copyOfRange(column.qualifier(),
                    COLLISION_PREFIX.length, column.qualifier().length);
            collisions.put(UniqueId.uidToString(parsed_tsuid),
                    new String(column.value(), CHARSET));
          }
        }

        return Deferred.fromResult(collisions);
      }

    }

    return client.get(get).addCallbackDeferring(new GetCB());

  }

  @Override
  public Deferred<Map<String, String>> fetchNotMatched(final int tree_id,
                                                       final List<String> tsuids) {

    final byte[] row_key = new byte[TREE_ID_WIDTH + 1];
    System.arraycopy(Tree.idToBytes(tree_id), 0, row_key, 0, TREE_ID_WIDTH);
    row_key[TREE_ID_WIDTH] = NOT_MATCHED_ROW_SUFFIX;

    final GetRequest get = new GetRequest(tree_table_name, row_key);
    get.family(TREE_FAMILY);

    // if the caller provided a list of TSUIDs, then we need to compile a list
    // of qualifiers so we only fetch those columns.
    if (tsuids != null && !tsuids.isEmpty()) {
      final byte[][] qualifiers = new byte[tsuids.size()][];
      int index = 0;
      for (String tsuid : tsuids) {
        final byte[] qualifier = new byte[NOT_MATCHED_PREFIX.length +
                (tsuid.length() / 2)];
        System.arraycopy(NOT_MATCHED_PREFIX, 0, qualifier, 0,
                NOT_MATCHED_PREFIX.length);
        final byte[] tsuid_bytes = UniqueId.stringToUid(tsuid);
        System.arraycopy(tsuid_bytes, 0, qualifier, NOT_MATCHED_PREFIX.length,
                tsuid_bytes.length);
        qualifiers[index] = qualifier;
        index++;
      }
      get.qualifiers(qualifiers);
    }

    /**
     * Called after issuing the row get request to parse out the results and
     * compile the list of collisions.
     */
    final class GetCB implements Callback<Deferred<Map<String, String>>,
            ArrayList<KeyValue>> {

      @Override
      public Deferred<Map<String, String>> call(final ArrayList<KeyValue> row)
              throws Exception {
        if (row == null || row.isEmpty()) {
          final Map<String, String> empty = new HashMap<String, String>(0);
          return Deferred.fromResult(empty);
        }

        Map<String, String> not_matched = new HashMap<String, String>(row.size());

        for (KeyValue column : row) {
          final byte[] parsed_tsuid = Arrays.copyOfRange(column.qualifier(),
                  NOT_MATCHED_PREFIX.length, column.qualifier().length);
          not_matched.put(UniqueId.uidToString(parsed_tsuid),
                  new String(column.value(), CHARSET));
        }
        return Deferred.fromResult(not_matched);
      }
    }
    return client.get(get).addCallbackDeferring(new GetCB());
  }

  @Override
  public Deferred<Boolean> flushTreeCollisions(final Tree tree) {
    final byte[] row_key = new byte[TREE_ID_WIDTH + 1];
    System.arraycopy(
            Tree.idToBytes(tree.getTreeId()), 0, row_key, 0, TREE_ID_WIDTH);
    row_key[TREE_ID_WIDTH] = COLLISION_ROW_SUFFIX;

    final byte[][] qualifiers = new byte[tree.getCollisions().size()][];
    final byte[][] values = new byte[tree.getCollisions().size()][];

    int index = 0;
    for (Map.Entry<String, String> entry : tree.getCollisions().entrySet()) {
      qualifiers[index] = new byte[COLLISION_PREFIX.length +
              (entry.getKey().length() / 2)];
      System.arraycopy(COLLISION_PREFIX, 0, qualifiers[index], 0,
              COLLISION_PREFIX.length);
      final byte[] tsuid = UniqueId.stringToUid(entry.getKey());
      System.arraycopy(tsuid, 0, qualifiers[index],
              COLLISION_PREFIX.length, tsuid.length);

      values[index] = entry.getValue().getBytes(CHARSET);
      index++;
    }

    final PutRequest put = new PutRequest(tree_table_name, row_key,
            TREE_FAMILY, qualifiers, values);
    tree.getCollisions().clear();

    /**
     * Super simple callback used to convert the Deferred&lt;Object&gt; to a
     * Deferred&lt;Boolean&gt; so that it can be grouped with other storage
     * calls
     */
    final class PutCB implements Callback<Deferred<Boolean>, Object> {

      @Override
      public Deferred<Boolean> call(Object result) throws Exception {
        return Deferred.fromResult(true);
      }
    }
    return client.put(put).addCallbackDeferring(new PutCB());
  }

  @Override
  public Deferred<Boolean> flushTreeNotMatched(final Tree tree) {
    final byte[] row_key = new byte[TREE_ID_WIDTH + 1];
    System.arraycopy(
            Tree.idToBytes(tree.getTreeId()), 0, row_key, 0, TREE_ID_WIDTH);
    row_key[TREE_ID_WIDTH] = NOT_MATCHED_ROW_SUFFIX;

    final byte[][] qualifiers = new byte[tree.getNotMatched().size()][];
    final byte[][] values = new byte[tree.getNotMatched().size()][];

    int index = 0;
    for (Map.Entry<String, String> entry : tree.getNotMatched().entrySet()) {
      qualifiers[index] = new byte[NOT_MATCHED_PREFIX.length +
              (entry.getKey().length() / 2)];
      System.arraycopy(NOT_MATCHED_PREFIX, 0, qualifiers[index], 0,
              NOT_MATCHED_PREFIX.length);
      final byte[] tsuid = UniqueId.stringToUid(entry.getKey());
      System.arraycopy(tsuid, 0, qualifiers[index],
              NOT_MATCHED_PREFIX.length, tsuid.length);

      values[index] = entry.getValue().getBytes(CHARSET);
      index++;
    }

    final PutRequest put = new PutRequest(tree_table_name, row_key,
            TREE_FAMILY, qualifiers, values);
    tree.getNotMatched().clear();

    /**
     * Super simple callback used to convert the Deferred&lt;Object&gt; to a
     * Deferred&lt;Boolean&gt; so that it can be grouped with other storage
     * calls
     */
    final class PutCB implements Callback<Deferred<Boolean>, Object> {

      @Override
      public Deferred<Boolean> call(Object result) throws Exception {
        return Deferred.fromResult(true);
      }

    }

    return client.put(put).addCallbackDeferring(new PutCB());
  }

  /**
   * Configures a scanner to run through all rows in the UID table that are
   * {@link #TREE_ID_WIDTH} bytes wide using a row key regex filter
   * @return The configured HBase scanner
   */
  private Scanner setupAllTreeScanner() {
    final byte[] start = new byte[TREE_ID_WIDTH];
    final byte[] end = new byte[TREE_ID_WIDTH];
    Arrays.fill(end, (byte)0xFF);

    final Scanner scanner = newScanner(tree_table_name);
    scanner.setStartKey(start);
    scanner.setStopKey(end);
    scanner.setFamily(TREE_FAMILY);

    // set the filter to match only on TREE_ID_WIDTH row keys
    final StringBuilder buf = new StringBuilder(20);
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
            + "^\\Q");
    buf.append("\\E(?:.{").append(TREE_ID_WIDTH).append("})$");
    scanner.setKeyRegexp(buf.toString(), CHARSET);
    return scanner;
  }
}
