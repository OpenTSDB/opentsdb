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
import com.google.common.base.Optional;
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
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.opentsdb.core.StringCoder.fromBytes;
import static net.opentsdb.core.StringCoder.toBytes;
import static net.opentsdb.uid.UniqueId.UniqueIdType;

/**
 * The HBaseStore that implements the client interface required by TSDB.
 */
public class HBaseStore implements TsdbStore {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseStore.class);

  /** Row key of the special row used to track the max ID already assigned. */
  private static final byte[] MAXID_ROW = { 0 };

  private static final byte[] TS_FAMILY = { 't' };

  /**
   * Charset used to convert Strings to byte arrays and back.
   */
  private static final Charset CHARSET = Charsets.ISO_8859_1;

  /**
   * The single column family used by this class.
   */
  private static final byte[] ID_FAMILY = toBytes("id");
  /**
   * The single column family used by this class.
   */
  private static final byte[] NAME_FAMILY = toBytes("name");

  /**
   * The single column family used by this class.
   */
  private static final byte[] UID_FAMILY = toBytes("name");

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
            .qualifier(type.qualifier.getBytes(CHARSET));

    return client.get(request)
            .addCallback(new GetCellNotEmptyCB())
            .addCallback(new GetCB());
  }

  @Override
  public Deferred<Optional<byte[]>> getId(final String name, final UniqueIdType type) {
    return getId(toBytes(name), type);
  }

  private Deferred<Optional<byte[]>> getId(final byte[] name, final UniqueIdType type) {
    final byte[] qualifier = type.qualifier.getBytes(CHARSET);

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
            new String(cell.get().qualifier(), CHARSET);
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
  public Deferred<byte[]> allocateUID(final byte[] name,
                                      final UniqueIdType type,
                                      final short id_width) {
    class IdCB implements Callback<Deferred<byte[]>, Long> {
      @Override
      public Deferred<byte[]> call(Long id) throws Exception {
        if (id <= 0) {
          throw new IllegalStateException("Got a negative ID from HBase: " + id);
        }

        LOG.info("Got ID={} for kind='{}' name='{}'", id, type.qualifier, name);

        final byte[] row = Bytes.fromLong(id);

        // row.length should actually be 8.
        if (row.length < id_width) {
          throw new IllegalStateException("row.length = " + row.length
                  + " which is less than " + id_width
                  + " for id=" + id
                  + " row=" + Arrays.toString(row));
        }

        // Verify that the indices in the row array that won't be used in the
        // uid with the current length are zero so we haven't reached the upper
        // limits.
        for (int i = 0; i < row.length - id_width; i++) {
          if (row[i] != 0) {
            throw new IllegalStateException("All Unique IDs for " + type.qualifier
                    + " on " + id_width + " bytes are already assigned!");
          }
        }

        // Shrink the ID on the requested number of bytes.
        final byte[] uid = Arrays.copyOfRange(row, row.length - id_width,
                row.length);

        return allocateUID(name, uid, type);
      }
    }

    final byte[] qualifier = type.qualifier.getBytes(CHARSET);
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
  public Deferred<byte[]> allocateUID(final byte[] name,
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

    final byte[] qualifier = type.qualifier.getBytes(CHARSET);
    final PutRequest reverse_mapping = new PutRequest(uid_table_name, uid, NAME_FAMILY, qualifier, name);
    final PutRequest forward_mapping = new PutRequest(uid_table_name, name, ID_FAMILY, qualifier, uid);

    return client.compareAndSet(reverse_mapping, HBaseClient.EMPTY_ARRAY)
            .addCallbackDeferring(new ReverseCB(reverse_mapping, forward_mapping))
            .addCallbackDeferring(new ForwardCB(forward_mapping, uid));
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
}
