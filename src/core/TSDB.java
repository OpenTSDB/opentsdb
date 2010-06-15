// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import net.opentsdb.HBaseException;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.StatsCollector;

/**
 * Thread-safe implementation of the {@link TSDBInterface}.
 */
public final class TSDB implements TSDBInterface {

  private static final Logger LOG = LoggerFactory.getLogger(TSDB.class);

  static final byte[] FAMILY = { 't' };

  private static final String METRICS_QUAL = "metrics";
  private static final short METRICS_WIDTH = 3;
  private static final String TAG_NAME_QUAL = "tagk";
  private static final short TAG_NAME_WIDTH = 3;
  private static final String TAG_VALUE_QUAL = "tagv";
  private static final short TAG_VALUE_WIDTH = 3;

  /** HTable in which timeseries are stored. */
  final HTable timeseries_table;

  /** Unique IDs for the metric names. */
  final UniqueId metrics;
  /** Unique IDs for the tag names. */
  final UniqueId tag_names;
  /** Unique IDs for the tag values. */
  final UniqueId tag_values;

  /**
   * Constructor.
   * @param timeseries_table The name of the HBase table where time series
   * data is stored.
   * @param uniqueids_table The name of the HBase table where the unique IDs
   * are stored.
   */
  public TSDB(final String timeseries_table, final String uniqueids_table)
    throws HBaseException {
    final Configuration conf = getHbaseConfiguration();
    try {
      this.timeseries_table = new HTable(conf, timeseries_table);
    } catch (IOException e) {
      throw new HBaseException("Failed to make a HTable for "
                               + timeseries_table, e);
    }
    this.timeseries_table.setAutoFlush(false);

    try {
      metrics = new UniqueId(new HTable(conf, uniqueids_table),
                             METRICS_QUAL, METRICS_WIDTH);
      tag_names = new UniqueId(new HTable(conf, uniqueids_table),
                               TAG_NAME_QUAL, TAG_NAME_WIDTH);
      tag_values = new UniqueId(new HTable(conf, uniqueids_table),
                                TAG_VALUE_QUAL, TAG_VALUE_WIDTH);
    } catch (IOException e) {
      throw new HBaseException("Failed to make a HTable for "
                               + uniqueids_table, e);
    }
  }

  /**
   * Create a configuration object to use HBase.
   * Screw the whole {@code hbase-site.xml} crap.  XML sucks.
   */
  private static Configuration getHbaseConfiguration() {
    final Configuration conf = HBaseConfiguration.create();
    // Try to fetch this many rows at a time when scanning.
    conf.setInt("hbase.client.scanner.caching", 1024);
    // Set the size of the write buffer in order to keep this many data points
    // in memory before persisting them in HBase and making them available to the
    // rest of the world.
    conf.setInt("hbase.client.write.buffer", Const.AVG_PUT_SIZE * 64);
    conf.set("hbase.zookeeper.quorum",
             System.getProperty("hbase.zookeeper.quorum", "127.0.0.1"));
    final String rootzknode = System.getProperty("zookeeper.znode.parent");
    if (rootzknode != null) {
      conf.set("zookeeper.znode.parent", rootzknode);
    }
    return conf;
  }

  /** Number of cache hits during lookups involving UIDs. */
  public int uidCacheHits() {
    return (metrics.cacheHits() + tag_names.cacheHits()
            + tag_values.cacheHits());
  }

  /** Number of cache misses during lookups involving UIDs. */
  public int uidCacheMisses() {
    return (metrics.cacheMisses() + tag_names.cacheMisses()
            + tag_values.cacheMisses());
  }

  /** Number of cache entries currently in RAM for lookups involving UIDs. */
  public int uidCacheSize() {
    return (metrics.cacheSize() + tag_names.cacheSize()
            + tag_values.cacheSize());
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public void collectStats(final StatsCollector collector) {
    collectUidStats(metrics, collector);
    collectUidStats(tag_names, collector);
    collectUidStats(tag_values, collector);

    {
      final Runtime runtime = Runtime.getRuntime();
      collector.record("jvm.ramfree", runtime.freeMemory());
      collector.record("jvm.ramused", runtime.totalMemory());
    }

    collector.addExtraTag("class", "IncomingDataPoints");
    try {
      collector.record("hbase.latency", IncomingDataPoints.putlatency, "method=put");
    } finally {
      collector.clearExtraTag("class");
    }

    collector.addExtraTag("class", "TsdbQuery");
    try {
      collector.record("hbase.latency", TsdbQuery.scanlatency, "method=scan");
    } finally {
      collector.clearExtraTag("class");
    }
  }

  /** Returns a latency histogram for Put RPCs used to store data points. */
  public Histogram getPutLatencyHistogram() {
    return IncomingDataPoints.putlatency;
  }

  /** Returns a latency histogram for Scan RPCs used to fetch data points.  */
  public Histogram getScanLatencyHistogram() {
    return TsdbQuery.scanlatency;
  }

  /**
   * Collects the stats for a {@link UniqueId}.
   * @param uid The instance from which to collect stats.
   * @param collector The collector to use.
   */
  private static void collectUidStats(final UniqueId uid,
                                      final StatsCollector collector) {
    collector.record("uid.cache-hit", uid.cacheHits(), "kind=" + uid.kind());
    collector.record("uid.cache-miss", uid.cacheMisses(), "kind=" + uid.kind());
    collector.record("uid.cache-size", uid.cacheSize(), "kind=" + uid.kind());
  }

  public Query newQuery() {
    return new TsdbQuery(this);
  }

  public WritableDataPoints newDataPoints() {
    return new IncomingDataPoints(this);
  }

  public void flush() throws HBaseException {
    if (timeseries_table.getWriteBuffer().isEmpty()) {
      return;
    }
    try {
      timeseries_table.flushCommits();
    } catch (IOException e) {
      final String errmsg = "Error while flushing Puts.  There are"
        + timeseries_table.getWriteBuffer().size()
        + " Puts left in the write buffer.";
      LOG.error(errmsg, e);
      throw new HBaseException(errmsg, e);
    }
  }

}
