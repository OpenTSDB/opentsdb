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
package net.opentsdb.storage;

import com.google.common.base.Charsets;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.StringCoder;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;
import org.hbase.async.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * The HBaseStore that implements the client interface required by TSDB.
 */
public class HBaseStore implements TsdbStore {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseStore.class);

  private static final byte[] TS_FAMILY = { 't' };

  /**
   * Charset used to convert Strings to byte arrays and back.
   */
  private static final Charset CHARSET = Charsets.ISO_8859_1;

  /**
   * The single column family used by this class.
   */
  private static final byte[] ID_FAMILY = StringCoder.toBytes("id");
  /**
   * The single column family used by this class.
   */
  private static final byte[] NAME_FAMILY = StringCoder.toBytes("name");

  /**
   * The single column family used by this class.
   */
  private static final byte[] UID_FAMILY = StringCoder.toBytes("name");

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
    super();
    this.client = client;

    enable_tree_processing = config.enable_tree_processing();
    enable_realtime_ts = config.enable_realtime_ts();
    enable_realtime_uid = config.enable_realtime_uid();
    enable_tsuid_incrementing = config.enable_tsuid_incrementing();
    enable_compactions = config.enable_compactions();

    data_table_name = config.getString("tsd.storage.hbase.data_table").getBytes(CHARSET);
    uid_table_name = config.getString("tsd.storage.hbase.uid_table").getBytes(CHARSET);
    tree_table_name = config.getString("tsd.storage.hbase.tree_table").getBytes(CHARSET);
    meta_table_name = config.getString("tsd.storage.hbase.meta_table").getBytes(CHARSET);

    client.setFlushInterval(config.getShort("tsd.storage.flush_interval"));

    compactionq = new CompactionQueue(this, config, data_table_name, TS_FAMILY);
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
  public Deferred<Object> addPoint(byte[] value, byte[] row, byte[] qualifier) {
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

    if (enable_compactions) {
      LOG.info("Flushing compaction queue");
      return compactionq.flush().addCallback(new CompactCB());
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
  public Deferred<String> getName(final byte[] id, byte[] table, byte[] kind) {
    class NameFromHBaseCB implements Callback<String, byte[]> {
      public String call(final byte[] name) {
        return name == null ? null : StringCoder.fromBytes(name);
      }
    }

    final GetRequest request = new GetRequest(table, id);
    request.family(NAME_FAMILY).qualifier(kind);

    class GetCB implements Callback<byte[], ArrayList<KeyValue>> {
      public byte[] call(final ArrayList<KeyValue> row) {
        if (row == null || row.isEmpty()) {
          return null;
        }
        return row.get(0).value();
      }
    }

    return client.get(request).addCallback(new GetCB()).addCallback(new
      NameFromHBaseCB());
  }

  @Override
  public Deferred<byte[]> getId(final String name, byte[] table, byte[] kind) {
    final GetRequest get = new GetRequest(table, StringCoder.toBytes(name));
    get.family(ID_FAMILY).qualifier(kind);

    class GetCB implements Callback<byte[], ArrayList<KeyValue>> {
      public byte[] call(final ArrayList<KeyValue> row) {
        if (row == null || row.isEmpty()) {
          return null;
        }
        return row.get(0).value();
      }
    }

    return client.get(get).addCallback(new GetCB());
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
      (meta.getType().toString().toLowerCase() + "_meta").getBytes(CHARSET),
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
      (meta.getType().toString().toLowerCase() + "_meta").getBytes(CHARSET));
    return client.delete(delete);
  }

  @Override
  public Deferred<Boolean> updateMeta(final UIDMeta meta, final boolean overwrite) {

    return getMeta(meta.getUID().getBytes(CHARSET),
      meta.getType()).addCallbackDeferring(

      /**
       *  Nested callback used to merge and store the meta data after verifying
       *  that the UID mapping exists. It has to access the {@code local_meta}
       *  object so that's why it's nested within the NameCB class
       */
      new Callback<Deferred<Boolean>, KeyValue>() {
        /**
         * Executes the CompareAndSet after merging changes
         * @return True if the CAS was successful, false if the stored data
         * was modified during flight.
         */

      @Override
      public Deferred<Boolean> call(KeyValue cell) throws Exception {
        final UIDMeta stored_meta;
        if (null == cell) {
          stored_meta = null;
        } else {
          stored_meta = JSON.parseToObject(cell.value(), UIDMeta.class);
          stored_meta.initializeChangedMap();
        }

        final byte[] original_meta = cell == null ?
          new byte[0] : cell.value();

        if (stored_meta != null) {
          meta.syncMeta(stored_meta, overwrite);
        }

        final PutRequest put = new PutRequest(uid_table_name,
          UniqueId.stringToUid(meta.getUID()), UID_FAMILY,
          (meta.getType().toString().toLowerCase() + "_meta").getBytes
            (CHARSET),
          meta.getStorageJSON());
        return client.compareAndSet(put, original_meta);
      }
    });
  }


  private Deferred<KeyValue> getMeta(byte[] uid, final UniqueId.UniqueIdType
    type) {
    /**
     * Inner class called to retrieve the meta data after verifying that the
     * name mapping exists. It requires the name to set the default, hence
     * the reason it's nested.
     */
    class FetchMetaCB implements Callback<KeyValue, ArrayList<KeyValue>> {

      /**
       * Called to parse the response of our storage GET call after
       * verification
       * @return The stored UIDMeta or a default object if the meta data
       * did not exist
       */
      @Override
      public KeyValue call(ArrayList<KeyValue> row)
        throws Exception {

        if (row == null || row.isEmpty()) {
          return null;
        } else {
          return row.get(0);
        }
      }
    }

    final GetRequest request = new GetRequest(uid_table_name, uid);
    request.family(UID_FAMILY);
    request.qualifier((type.toString().toLowerCase() + "_meta").getBytes(CHARSET));
    return client.get(request).addCallback(new FetchMetaCB());
  }

  @Override
  public Deferred<UIDMeta> getMeta(final byte[] uid, final String name,
                                   final UniqueId.UniqueIdType type) {
    /**
     * Inner class called to retrieve the meta data after verifying that the
     * name mapping exists. It requires the name to set the default, hence
     * the reason it's nested.
     */
    class FetchMetaCB implements Callback<UIDMeta, KeyValue> {
      /**
       * Called to parse the response of our storage GET call after
       * verification
       * @return The stored UIDMeta or a default object if the meta data
       * did not exist
       */
      @Override
      public UIDMeta call(KeyValue cell)
        throws Exception {
        if (cell == null) {
          // return the default
          return new UIDMeta(type,
            uid, name, false);
        }

        UniqueId.UniqueIdType effective_type = type;
        if (effective_type == null) {
          final String qualifier =
            new String(cell.qualifier(), CHARSET);
          effective_type = UniqueId.stringToUniqueIdType(qualifier.substring(0,
            qualifier.indexOf("_meta")));
        }

        UIDMeta return_meta = UIDMeta.buildFromJSON(cell.value(),
          effective_type, uid, name);

        return return_meta;
      }
    }

    return getMeta(uid, type).addCallback(new FetchMetaCB());
  }

  // ------------------ //
  // Compaction helpers //
  // ------------------ //
  @Override
  public final KeyValue compact(final ArrayList<KeyValue> row,
                                List<Annotation> annotations) {
    return compactionq.compact(row, annotations);
  }

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
}
