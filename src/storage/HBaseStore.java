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
import net.opentsdb.core.Const;
import net.opentsdb.core.Internal;
import net.opentsdb.meta.Annotation;
import net.opentsdb.utils.Config;
import org.hbase.async.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * The HBaseStore that implements the client interface required by TSDB.
 */
public final class HBaseStore implements TsdbStore {
  /**
   * Charset used to convert Strings to byte arrays and back.
   */
  private static final Charset CHARSET = Charsets.ISO_8859_1;
  private static final int MS_IN_A_SEC = 1000;
  private static final Logger LOG = LoggerFactory.getLogger(HBaseStore.class);

  private final org.hbase.async.HBaseClient client;

  private final boolean enable_realtime_ts;
  private final boolean enable_realtime_uid;
  private final boolean enable_tsuid_incrementing;
  private final boolean enable_tree_processing;
  // TODO Make these private
  public final boolean enable_compactions;
  public final boolean fix_duplicates;

  private final byte[] data_table_name;
  private final byte[] uid_table_name;
  private final byte[] tree_table_name;
  private final byte[] meta_table_name;

  static final byte[] FAMILY = {'t'};

  public static final short METRICS_WIDTH = 3;

  /**
   * Row keys that need to be compacted.
   * Whenever we write a new data point to a row, we add the row key to this
   * set.  Every once in a while, the compaction thread will go through old
   * row keys and will read re-compact them.
   */
  private final CompactionQueue compactionq;


  public HBaseStore(final Config config) {
    super();
    this.client = new org.hbase.async.HBaseClient(
            config.getString("tsd.storage.hbase.zk_quorum"),
            config.getString("tsd.storage.hbase.zk_basedir"));

    enable_tree_processing = config.enable_tree_processing();
    enable_realtime_ts = config.enable_realtime_ts();
    enable_realtime_uid = config.enable_realtime_uid();
    enable_tsuid_incrementing = config.enable_tsuid_incrementing();
    enable_compactions = config.enable_compactions();
    fix_duplicates = config.fix_duplicates();

    data_table_name = config.getString("tsd.storage.hbase.data_table").getBytes(CHARSET);
    uid_table_name = config.getString("tsd.storage.hbase.uid_table").getBytes(CHARSET);
    tree_table_name = config.getString("tsd.storage.hbase.tree_table").getBytes(CHARSET);
    meta_table_name = config.getString("tsd.storage.hbase.meta_table").getBytes(CHARSET);


    compactionq = new CompactionQueue(this);
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
    if (enable_realtime_ts || enable_realtime_uid ||
            enable_tsuid_incrementing) {
      checks.add(client.ensureTableExists(meta_table_name));
    }

    return Deferred.group(checks);
  }

  @Override
  public Deferred<Object> flush() throws HBaseException {
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
  public Deferred<Object> shutdown() {
    if (enable_compactions) {
      LOG.info("Flushing compaction queue");

      return compactionq.flush().addCallback(new Callback<Object,
              ArrayList<Object>>() {
        @Override
        public Object call(ArrayList<Object> arg) throws Exception {
          return client.shutdown();
        }
      });
    }

    return client.shutdown();
  }

  @Override
  public ClientStats stats() {
    return this.client.stats();
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
  public Deferred<Object> addPoint(final byte[] row,
                                   final long timestamp,
                                   final byte[] value,
                                   final short flags) {
    final long base_time;
    final byte[] qualifier = Internal.buildQualifier(timestamp, flags);

    if ((timestamp & Const.SECOND_MASK) != 0) {
      // drop the ms timestamp to seconds to calculate the base timestamp
      base_time = ((timestamp / MS_IN_A_SEC) -
              ((timestamp / MS_IN_A_SEC) % Const.MAX_TIMESPAN));
    } else {
      base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
    }

    Bytes.setInt(row, (int) base_time, METRICS_WIDTH);
    scheduleForCompaction(row, (int) base_time);
    final PutRequest point = new PutRequest(data_table_name, row, FAMILY, qualifier,
            value);

    return client.put(point);
  }


  // ------------------ //
  // Compaction helpers //
  // ------------------ //
  final KeyValue compact(final ArrayList<KeyValue> row,
                         List<Annotation> annotations) {
    return compactionq.compact(row, annotations);
  }

  /**
   * Schedules the given row key for later re-compaction.
   * Once this row key has become "old enough", we'll read back all the data
   * points in that row, write them back to TsdbStore in a more compact fashion,
   * and delete the individual data points.
   *
   * @param row       The row key to re-compact later.  Will not be modified.
   * @param base_time The 32-bit unsigned UNIX timestamp.
   */
  final void scheduleForCompaction(final byte[] row, final int base_time) {
    if (enable_compactions) {
      compactionq.add(row);
    }
  }


  /**
   * Gets the entire given row from the data table.
   */
  @Deprecated
  final Deferred<ArrayList<KeyValue>> get(final byte[] key) {
    return this.get(new GetRequest(data_table_name, key));
  }

  /**
   * Puts the given value into the data table.
   */
  @Deprecated
  final Deferred<Object> put(final byte[] key,
                             final byte[] qualifier,
                             final byte[] value) {
    return this.put(new PutRequest(data_table_name, key, FAMILY, qualifier,
            value));
  }

  /**
   * Deletes the given cells from the data table.
   */
  @Deprecated
  final Deferred<Object> delete(final byte[] key, final byte[][] qualifiers) {
    return this.delete(new DeleteRequest(data_table_name, key, FAMILY,
            qualifiers));
  }
}
