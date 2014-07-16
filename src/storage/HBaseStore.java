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
import net.opentsdb.core.*;
import net.opentsdb.meta.Annotation;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import org.hbase.async.*;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.*;

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
  public void collectStats(StatsCollector collector) {
    ClientStats stats = client.stats();

    collector.record("hbase.root_lookups", stats.rootLookups());
    collector.record("hbase.meta_lookups",
            stats.uncontendedMetaLookups(), "type=uncontended");
    collector.record("hbase.meta_lookups",
            stats.contendedMetaLookups(), "type=contended");
    collector.record("hbase.rpcs",
            stats.atomicIncrements(), "type=increment");
    collector.record("hbase.rpcs", stats.deletes(), "type=delete");
    collector.record("hbase.rpcs", stats.gets(), "type=get");
    collector.record("hbase.rpcs", stats.puts(), "type=put");
    collector.record("hbase.rpcs", stats.rowLocks(), "type=rowLock");
    collector.record("hbase.rpcs", stats.scannersOpened(), "type=openScanner");
    collector.record("hbase.rpcs", stats.scans(), "type=scan");
    collector.record("hbase.rpcs.batched", stats.numBatchedRpcSent());
    collector.record("hbase.flushes", stats.flushes());
    collector.record("hbase.connections.created", stats.connectionsCreated());
    collector.record("hbase.nsre", stats.noSuchRegionExceptions());
    collector.record("hbase.nsre.rpcs_delayed",
            stats.numRpcDelayedDueToNSRE());

    compactionq.collectStats(collector);
  }

  @Override
  public Deferred<DataPoints[]> executeQuery(Query query) {
    return findSpans(query).addCallback(new GroupByAndAggregateCB());
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
    return addPoint(row, timestamp, value, flags, false);
  }

  @Override
  public Deferred<Object> addPoint(final byte[] row,
                                   final long timestamp,
                                   final byte[] value,
                                   final short flags,
                                   final boolean batch_import) {
    final long base_time;
    final byte[] qualifier = Internal.buildQualifier(timestamp, flags);

    // true is we have higher resolution than seconds on timestamp
    final boolean ms_timestamp = (timestamp & Const.SECOND_MASK) != 0;

    if (ms_timestamp) {
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
    point.setDurable(batch_import);

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













  /**
   * Finds all the {@link net.opentsdb.core.Span}s that match this query.
   * This is what actually scans the HBase table and loads the data into
   * {@link net.opentsdb.core.Span}s.
   * @return A map from HBase row key to the {@link net.opentsdb.core.Span} for that row key.
   * Since a {@link net.opentsdb.core.Span} actually contains multiple HBase rows, the row key
   * stored in the map has its timestamp zero'ed out.
   * @throws HBaseException if there was a problem communicating with HBase to
   * perform the search.
   * @throws IllegalArgumentException if bad data was retreived from HBase.
   * @param query
   */
  private Deferred<TreeMap<byte[], Span>> findSpans(final Query query) throws
          HBaseException {
    final short metric_width = Const.METRICS_WIDTH;
    final TreeMap<byte[], Span> spans = // The key is a row key from HBase.
            new TreeMap<byte[], Span>(new SpanCmp(metric_width));
    final Scanner scanner = getScanner(query);
    final Deferred<TreeMap<byte[], Span>> results =
            new Deferred<TreeMap<byte[], Span>>();

    /**
     * Scanner callback executed recursively each time we get a set of data
     * from storage. This is responsible for determining what columns are
     * returned and issuing requests to load leaf objects.
     * When the scanner returns a null set of rows, the method initiates the
     * final callback.
     */
    final class ScannerCB implements Callback<Object,
            ArrayList<ArrayList<KeyValue>>> {

      int nrows = 0;
      boolean seenAnnotation = false;
      int hbase_time = 0; // milliseconds.
      long starttime = System.nanoTime();

      /**
       * Starts the scanner and is called recursively to fetch the next set of
       * rows from the scanner.
       * @return The map of spans if loaded successfully, null if no data was
       * found
       */
      public Object scan() {
        starttime = System.nanoTime();
        return scanner.nextRows().addCallback(this);
      }

      /**
       * Loops through each row of the scanner results and parses out data
       * points and optional meta data
       * @return null if no rows were found, otherwise the TreeMap with spans
       */
      @Override
      public Object call(final ArrayList<ArrayList<KeyValue>> rows)
              throws Exception {
        hbase_time += (System.nanoTime() - starttime) / 1000000;
        try {
          if (rows == null) {
            hbase_time += (System.nanoTime() - starttime) / 1000000;
            scanlatency.add(hbase_time);
            LOG.info(TsdbQuery.this + " matched " + nrows + " rows in " +
                    spans.size() + " spans in " + hbase_time + "ms");
            if (nrows < 1 && !seenAnnotation) {
              results.callback(null);
            } else {
              results.callback(spans);
            }
            scanner.close();
            return null;
          }

          for (final ArrayList<KeyValue> row : rows) {
            final byte[] key = row.get(0).key();
            if (Bytes.memcmp(metric, key, 0, metric_width) != 0) {
              scanner.close();
              throw new IllegalDataException(
                      "HBase returned a row that doesn't match"
                              + " our scanner (" + scanner + ")! " + row + " does not start"
                              + " with " + Arrays.toString(metric));
            }
            Span datapoints = spans.get(key);
            if (datapoints == null) {
              datapoints = new Span(tsdb);
              spans.put(key, datapoints);
            }
            final KeyValue compacted =
                    tsdb.compact(row, datapoints.getAnnotations());
            seenAnnotation |= !datapoints.getAnnotations().isEmpty();
            if (compacted != null) { // Can be null if we ignored all KVs.
              datapoints.addRow(compacted);
              nrows++;
            }
          }

          return scan();
        } catch (Exception e) {
          scanner.close();
          results.callback(e);
          return null;
        }
      }
    }

    new ScannerCB().scan();
    return results;
  }

  /**
   * Callback that should be attached the the output of
   * {@link TsdbQuery#findSpans} to group and sort the results.
   */
  private class GroupByAndAggregateCB implements
          Callback<DataPoints[], TreeMap<byte[], Span>>{

    /**
     * Creates the {@link SpanGroup}s to form the final results of this query.
     * @param spans The {@link Span}s found for this query ({@link #findSpans}).
     * Can be {@code null}, in which case the array returned will be empty.
     * @return A possibly empty array of {@link SpanGroup}s built according to
     * any 'GROUP BY' formulated in this query.
     */
    @Override
    public DataPoints[] call(final TreeMap<byte[], Span> spans) throws Exception {
      if (spans == null || spans.size() <= 0) {
        return NO_RESULT;
      }
      if (group_bys == null) {
        // We haven't been asked to find groups, so let's put all the spans
        // together in the same group.
        final SpanGroup group = new SpanGroup(tsdb,
                getScanStartTimeSeconds(),
                getScanEndTimeSeconds(),
                spans.values(),
                rate, rate_options,
                aggregator,
                sample_interval_ms, downsampler);
        return new SpanGroup[] { group };
      }

      // Maps group value IDs to the SpanGroup for those values. Say we've
      // been asked to group by two things: foo=* bar=* Then the keys in this
      // map will contain all the value IDs combinations we've seen. If the
      // name IDs for `foo' and `bar' are respectively [0, 0, 7] and [0, 0, 2]
      // then we'll have group_bys=[[0, 0, 2], [0, 0, 7]] (notice it's sorted
      // by ID, so bar is first) and say we find foo=LOL bar=OMG as well as
      // foo=LOL bar=WTF and that the IDs of the tag values are:
      // LOL=[0, 0, 1] OMG=[0, 0, 4] WTF=[0, 0, 3]
      // then the map will have two keys:
      // - one for the LOL-OMG combination: [0, 0, 1, 0, 0, 4] and,
      // - one for the LOL-WTF combination: [0, 0, 1, 0, 0, 3].
      final Bytes.ByteMap<SpanGroup> groups = new Bytes.ByteMap<SpanGroup>();
      final short value_width = tsdb.tag_values.width();
      final byte[] group = new byte[group_bys.size() * value_width];
      for (final Map.Entry<byte[], Span> entry : spans.entrySet()) {
        final byte[] row = entry.getKey();
        byte[] value_id = null;
        int i = 0;
        // TODO(tsuna): The following loop has a quadratic behavior. We can
        // make it much better since both the row key and group_bys are sorted.
        for (final byte[] tag_id : group_bys) {
          value_id = Tags.getValueId(tsdb, row, tag_id);
          if (value_id == null) {
            break;
          }
          System.arraycopy(value_id, 0, group, i, value_width);
          i += value_width;
        }
        if (value_id == null) {
          LOG.error("WTF? Dropping span for row " + Arrays.toString(row)
                  + " as it had no matching tag from the requested groups,"
                  + " which is unexpected. Query=" + this);
          continue;
        }
        //LOG.info("Span belongs to group " + Arrays.toString(group) + ": " + Arrays.toString(row));
        SpanGroup thegroup = groups.get(group);
        if (thegroup == null) {
          thegroup = new SpanGroup(tsdb, getScanStartTimeSeconds(),
                  getScanEndTimeSeconds(),
                  null, rate, rate_options, aggregator,
                  sample_interval_ms, downsampler);
          // Copy the array because we're going to keep `group' and overwrite
          // its contents. So we want the collection to have an immutable copy.
          final byte[] group_copy = new byte[group.length];
          System.arraycopy(group, 0, group_copy, 0, group.length);
          groups.put(group_copy, thegroup);
        }
        thegroup.add(entry.getValue());
      }
      //for (final Map.Entry<byte[], SpanGroup> entry : groups) {
      // LOG.info("group for " + Arrays.toString(entry.getKey()) + ": " + entry.getValue());
      //}
      return groups.values().toArray(new SpanGroup[groups.size()]);
    }
  }

  /**
   * Comparator that ignores timestamps in row keys.
   */
  private static final class SpanCmp implements Comparator<byte[]> {

    private final short metric_width;

    public SpanCmp(final short metric_width) {
      this.metric_width = metric_width;
    }

    @Override
    public int compare(final byte[] a, final byte[] b) {
      final int length = Math.min(a.length, b.length);
      if (a == b) {  // Do this after accessing a.length and b.length
        return 0;    // in order to NPE if either a or b is null.
      }
      int i;
      // First compare the metric ID.
      for (i = 0; i < metric_width; i++) {
        if (a[i] != b[i]) {
          return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
        }
      }
      // Then skip the timestamp and compare the rest.
      for (i += Const.TIMESTAMP_BYTES; i < length; i++) {
        if (a[i] != b[i]) {
          return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
        }
      }
      return a.length - b.length;
    }

  }

  /**
   * Returns a scanner set for the given metric (from {@link #metric} or from
   * the first TSUID in the {@link #tsuids}s list. If one or more tags are
   * provided, it calls into {@link #createAndSetFilter} to setup a row key
   * filter. If one or more TSUIDs have been provided, it calls into
   * {@link #createAndSetTSUIDFilter} to setup a row key filter.
   * @return A scanner to use for fetching data points
   */
  protected Scanner getScanner(final Query query) throws HBaseException {
    final short metric_width = Const.METRICS_WIDTH;
    final byte[] start_row = new byte[metric_width + Const.TIMESTAMP_BYTES];
    final byte[] end_row = new byte[metric_width + Const.TIMESTAMP_BYTES];
    // We search at least one row before and one row after the start & end
    // time we've been given as it's quite likely that the exact timestamp
    // we're looking for is in the middle of a row.  Plus, a number of things
    // rely on having a few extra data points before & after the exact start
    // & end dates in order to do proper rate calculation or downsampling near
    // the "edges" of the graph.
    Bytes.setInt(start_row, (int) getScanStartTimeSeconds(), metric_width);
    Bytes.setInt(end_row, (end_time == UNSET
                    ? -1  // Will scan until the end (0xFFF...).
                    : (int) getScanEndTimeSeconds()),
            metric_width);

    // set the metric UID based on the TSUIDs if given, or the metric UID
    if (tsuids != null && !tsuids.isEmpty()) {
      final String tsuid = tsuids.get(0);
      final String metric_uid = tsuid.substring(0, Const.METRICS_WIDTH * 2);
      metric = UniqueId.stringToUid(metric_uid);
      System.arraycopy(metric, 0, start_row, 0, metric_width);
      System.arraycopy(metric, 0, end_row, 0, metric_width);
    } else {
      System.arraycopy(metric, 0, start_row, 0, metric_width);
      System.arraycopy(metric, 0, end_row, 0, metric_width);
    }

    final Scanner scanner = client.newScanner(data_table_name);
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    if (tsuids != null && !tsuids.isEmpty()) {
      createAndSetTSUIDFilter(scanner);
    } else if (tags.size() > 0 || group_bys != null) {
      createAndSetFilter(scanner);
    }
    scanner.setFamily(FAMILY);
    return scanner;
  }



  /** Returns the UNIX timestamp from which we must start scanning.  */
  private long getScanStartTimeSeconds(final Query query) {
    // The reason we look before by `MAX_TIMESPAN * 2' seconds is because of
    // the following.  Let's assume MAX_TIMESPAN = 600 (10 minutes) and the
    // start_time = ... 12:31:00.  If we initialize the scanner to look
    // only 10 minutes before, we'll start scanning at time=12:21, which will
    // give us the row that starts at 12:30 (remember: rows are always aligned
    // on MAX_TIMESPAN boundaries -- so in this example, on 10m boundaries).
    // But we need to start scanning at least 1 row before, so we actually
    // look back by twice MAX_TIMESPAN.  Only when start_time is aligned on a
    // MAX_TIMESPAN boundary then we'll mistakenly scan back by an extra row,
    // but this doesn't really matter.
    // Additionally, in case our sample_interval_ms is large, we need to look
    // even further before/after, so use that too.
    long start = query.getStartTime();
    // down cast to seconds if we have a query in ms
    if ((start & Const.SECOND_MASK) != 0) {
      start /= 1000;
    }
    final long ts = start - Const.MAX_TIMESPAN * 2 - query.getSampleInterval() / 1000;
    return ts > 0 ? ts : 0;
  }

  /** Returns the UNIX timestamp at which we must stop scanning.  */
  private long getScanEndTimeSeconds(final Query query) {
    // For the end_time, we have a different problem.  For instance if our
    // end_time = ... 12:30:00, we'll stop scanning when we get to 12:40, but
    // once again we wanna try to look ahead one more row, so to avoid this
    // problem we always add 1 second to the end_time.  Only when the end_time
    // is of the form HH:59:59 then we will scan ahead an extra row, but once
    // again that doesn't really matter.
    // Additionally, in case our sample_interval_ms is large, we need to look
    // even further before/after, so use that too.
    long end = query.getEndTime();
    if ((end & Const.SECOND_MASK) != 0) {
      end /= 1000;
    }
    return end + Const.MAX_TIMESPAN + 1 + query.getSampleInterval() / 1000;
  }
}
