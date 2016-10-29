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
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.KeyValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
             "ch.qos.*", "org.slf4j.*",
             "com.sum.*", "org.xml.*"})
@PrepareForTest({ RowSeq.class, TSDB.class, UniqueId.class, KeyValue.class, 
Config.class, RowKey.class })
public class TestTsdbQueryRollup extends BaseTsdbTest {
  private final static byte[] FAMILY = "t".getBytes(MockBase.ASCII());
  private TsdbQuery query = null;
  private RollupConfig rollup_config;
  private Map<String, String> tags2;
  private TSQuery ts_query;
  
  @Before
  public void beforeLocal() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    final List<byte[]> families = new ArrayList<byte[]>();
    families.add(FAMILY);
    
    storage.addTable("tsdb-rollup-10m".getBytes(), families);
    storage.addTable("tsdb-rollup-agg-10m".getBytes(), families);
    storage.addTable("tsdb-rollup-1h".getBytes(), families);
    storage.addTable("tsdb-rollup-agg-1h".getBytes(), families);
    storage.addTable("tsdb-rollup-1d".getBytes(), families);
    storage.addTable("tsdb-rollup-agg-1d".getBytes(), families);
    
    query = new TsdbQuery(tsdb);
    tags2 = new HashMap<String, String>(1);
    tags2.put(TAGK_STRING, TAGV_B_STRING);
    
    final List<RollupInterval> rollups = new ArrayList<RollupInterval>();
    rollups.add(new RollupInterval(
        "tsdb-rollup-10m", "tsdb-rollup-agg-10m", "10m", "6h"));
    rollups.add(new RollupInterval(
        "tsdb-rollup-1h", "tsdb-rollup-agg-1h", "1h", "1d"));
    rollups.add(new RollupInterval(
        "tsdb-rollup-1d", "tsdb-rollup-agg-1d", "1d", "1m"));
    
    rollup_config = new RollupConfig(rollups);
    Whitebox.setInternalState(tsdb, "rollup_config", rollup_config);
    Whitebox.setInternalState(tsdb, "default_interval", new RollupInterval(
        "tsdb", "tsdb-agg", "1m", "1h"));
  }

  // This test shows us falling back to raw data if the requested downsample
  // interval doesn't match a configured rollup
  @Test
  public void run15mSumLongSingleTS() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    final Aggregator aggr = Aggregators.SUM;
    long start_timestamp = 1356998400L;
    long end_timestamp = 1357041600L;
    storeLongRollup(start_timestamp, end_timestamp, false, false, 
      interval, aggr);

    // 15 minutes down sampling that falls back to raw data
    final int time_interval = 15 * 60 * 1000;
    setQuery("15m", aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);
    
    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));
    
    long ts = start_timestamp * 1000;
    double value = 435;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.00001);
      if (value >= 8535.0) {
        value = 300; // last dp all by it's lonesom
      } else {
        value += 900;
      }
      ts += time_interval;
    }
    assertEquals(11, dps[0].size());
  }

  // In this case we're downsampling rolled up values
  @Test
  public void run30mSumLongSingleTS() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    final Aggregator aggr = Aggregators.SUM;
    long start_timestamp = 1356998400L;
    long end_timestamp = 1357041599L;
    storeLongRollup(start_timestamp, end_timestamp, false, false, 
      interval, aggr);

    setQuery("30m", aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);
    DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));
    
    double value = 3600;
    long ts = start_timestamp * 1000;
    
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0);
      assertEquals(ts, dp.timestamp());
      value += 5400;
      ts += (interval.getInterval() * 3) * 1000;
    }
    assertEquals(24, dps[0].size());
  }

  // Zimsum == SUM when comparing qualifiers
  @Test
  public void run10mZimSumLongSingleTS() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    Aggregator aggr = Aggregators.SUM; // still have to write as sum
    long start_timestamp = 1356998400L;
    long end_timestamp = 1357041599L;
    storeLongRollup(start_timestamp, end_timestamp, false, false, 
      interval, aggr);

    aggr = Aggregators.ZIMSUM;
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);
    
    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));

    int i = 600;
    long ts = start_timestamp * 1000;
    
    for (final DataPoint dp : dps[0]) {
      assertFalse(dp.isInteger());
      assertEquals(i, dp.doubleValue(), 0.0001);
      assertEquals(ts, dp.timestamp());
      ts += interval.getInterval() * 1000;
      i += interval.getInterval();
    }
    assertEquals(72, dps[0].size());
  }
  
  @Test
  public void run10mMaxLongSingleTSNotFound() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    Aggregator aggr = Aggregators.SUM;
    long start_timestamp = 1356998400L;
    long end_timestamp = 1357041599L;
    storeLongRollup(start_timestamp, end_timestamp, false, false, 
      interval, aggr);

    aggr = Aggregators.MAX;
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);
    
    final DataPoints[] dps = query.run();
    assertEquals(0, dps.length);
  }
  
  @Test
  public void run10mSumLongSingleTS() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    final Aggregator aggr = Aggregators.SUM;
    long start_timestamp = 1356998400L;
    long end_timestamp = 1357041600L;
    storeLongRollup(1356998400L, end_timestamp, false, false, interval, aggr);

    final int time_interval = interval.getInterval();
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));

    int i = 600;
    long ts = start_timestamp * 1000;
    
    for (final DataPoint dp : dps[0]) {
      assertFalse(dp.isInteger());
      assertEquals(i, dp.doubleValue(), 0.0001);
      assertEquals(ts, dp.timestamp());
      ts += time_interval * 1000;
      i += time_interval;
    }
    assertEquals(73, dps[0].size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void run10mSumLongSingleTSInMS() throws Exception {
    RollupInterval ten_min_interval = rollup_config.getRollupInterval("10m");
    Aggregator aggr = Aggregators.SUM;
    long start_timestamp = 1356998400000L;
    
    //rollup doesn't accept timestamps in milliseconds
    tsdb.addAggregatePoint(METRIC_STRING, start_timestamp, 
          0, tags, false, ten_min_interval.getStringInterval(), 
          aggr.toString()).joinUninterruptibly();
  }

  @Test
  public void run10mSumLongSingleTSRate() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    final Aggregator aggr = Aggregators.SUM;
    final long start_timestamp = 1356998400;
    final long end_timestamp = 1357041600;
    
    storeLongRollup(start_timestamp, end_timestamp, false, false, 
      interval, aggr);

    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    ts_query.getQueries().get(0).setRate(true);
    query.configureFromQuery(ts_query, 0);
    final DataPoints[] dps = query.run();
    
    assertNotNull(dps);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));

    long expected_timestamp = (start_timestamp + interval.getInterval()) * 1000;
    for (DataPoint dp : dps[0]) {
      assertEquals(1.0F, dp.doubleValue(), 0.00001);
      assertEquals(expected_timestamp, dp.timestamp());
      expected_timestamp += interval.getInterval() * 1000;
    }

    assertEquals(72, dps[0].size());
  }

  @Test
  public void run10mSumFloatSingleTS() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    final Aggregator aggr = Aggregators.SUM;
    long start_timestamp = 1356998400L;
    final long end_timestamp = 1357041600;
    storeFloatRollup(start_timestamp, end_timestamp, true, false, interval, aggr);

    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);
    final DataPoints[] dps = query.run();
    
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));

    double value = 600.5F;
    long expected_timestamp = start_timestamp * 1000;
    
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0.00001);
      assertEquals(expected_timestamp, dp.timestamp());
      value += interval.getInterval();
      expected_timestamp += interval.getInterval() * 1000;
    }

    assertEquals(73, dps[0].size());
  }

  @Test
  public void run10mSumFloatSingleTSRate() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    final Aggregator aggr = Aggregators.SUM;
    final long start_timestamp = 1356998400;
    final long end_timestamp = 1357041600;
    
    storeFloatRollup(start_timestamp, end_timestamp, false, false, 
      interval, aggr);

    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    ts_query.getQueries().get(0).setRate(true);
    query.configureFromQuery(ts_query, 0);
    final DataPoints[] dps = query.run();
    
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));
    
    long expected_timestamp = (start_timestamp + interval.getInterval()) * 1000;
    for (DataPoint dp : dps[0]) {
      assertEquals(1.0F, dp.doubleValue(), 0.00001);
      assertEquals(expected_timestamp, dp.timestamp());
      expected_timestamp += interval.getInterval() * 1000;
    }
    assertEquals(72, dps[0].size());
  }

  // Make sure filtering on time series still operates
  @Test
  public void run10mSumLongDoubleTSFilter() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    final Aggregator aggr = Aggregators.SUM;
    long start_timestamp = 1356998400L;
    long end_timestamp = 1357041600L;
    storeLongRollup(1356998400L, end_timestamp, true, false, interval, aggr);

    final int time_interval = interval.getInterval();
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));

    int i = 600;
    long ts = start_timestamp * 1000;
    
    for (final DataPoint dp : dps[0]) {
      assertFalse(dp.isInteger());
      assertEquals(i, dp.doubleValue(), 0.0001);
      assertEquals(ts, dp.timestamp());
      ts += time_interval * 1000;
      i += time_interval;
    }
    assertEquals(73, dps[0].size());
  }
  
  @Test
  public void run10mSumLongDoubleTS() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    final Aggregator aggr = Aggregators.SUM;
    long start_timestamp = 1356998400L;
    long end_timestamp = 1357041600L;
    storeLongRollup(1356998400L, end_timestamp, true, false, interval, aggr);

    final int time_interval = interval.getInterval();
    tags.clear();
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    ts_query.getQueries().get(0).getFilters().clear();
    ts_query.validateAndSetQuery();
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertEquals(TAGK_STRING, dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertNull(dps[0].getTags().get(TAGK_STRING));

    long ts = start_timestamp * 1000;
    
    for (final DataPoint dp : dps[0]) {
      assertEquals(43800, dp.doubleValue(), 0.0001);
      assertEquals(ts, dp.timestamp());
      ts += time_interval * 1000;
    }
    assertEquals(73, dps[0].size());
  }
  
  // Make sure other aggregates don't polute our results
  @Test
  public void run10mSumLongDoubleTSFilterOtherAggs() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    final Aggregator aggr = Aggregators.SUM;
    long start_timestamp = 1356998400L;
    long end_timestamp = 1357041600L;
    storeLongRollup(1356998400L, end_timestamp, true, false, interval, aggr);
    storeLongRollup(1356998400L, end_timestamp, true, false, interval, 
        Aggregators.MAX);
    storeLongRollup(1356998400L, end_timestamp, true, false, interval, 
        Aggregators.MIN);

    final int time_interval = interval.getInterval();
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));

    int i = 600;
    long ts = start_timestamp * 1000;
    
    for (final DataPoint dp : dps[0]) {
      assertFalse(dp.isInteger());
      assertEquals(i, dp.doubleValue(), 0.0001);
      assertEquals(ts, dp.timestamp());
      ts += time_interval * 1000;
      i += time_interval;
    }
    assertEquals(73, dps[0].size());
  }
  
  @Test
  public void run10mMaxLongSingleTS() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    final Aggregator aggr = Aggregators.MAX;
    long start_timestamp = 1356998400L;
    long end_timestamp = 1357041600L;
    storeLongRollup(1356998400L, end_timestamp, false, false, interval, aggr);

    final int time_interval = interval.getInterval();
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));

    int i = 600;
    long ts = start_timestamp * 1000;
    
    for (final DataPoint dp : dps[0]) {
      assertFalse(dp.isInteger());
      assertEquals(i, dp.doubleValue(), 0.0001);
      assertEquals(ts, dp.timestamp());
      ts += time_interval * 1000;
      i += time_interval;
    }
    assertEquals(73, dps[0].size());
  }
  
  @Test
  public void run10mMinLongSingleTS() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    final Aggregator aggr = Aggregators.MIN;
    long start_timestamp = 1356998400L;
    long end_timestamp = 1357041600L;
    storeLongRollup(1356998400L, end_timestamp, false, false, interval, aggr);

    final int time_interval = interval.getInterval();
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));

    int i = 600;
    long ts = start_timestamp * 1000;
    
    for (final DataPoint dp : dps[0]) {
      assertFalse(dp.isInteger());
      assertEquals(i, dp.doubleValue(), 0.0001);
      assertEquals(ts, dp.timestamp());
      ts += time_interval * 1000;
      i += time_interval;
    }
    assertEquals(73, dps[0].size());
  }
  
  @Test
  public void run10mAvgLongSingleTS() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    Aggregator aggr = Aggregators.SUM;
    long start_timestamp = 1356998400L;
    long end_timestamp = 1357041600L;
    storeLongRollup(start_timestamp, end_timestamp, false, false, interval, aggr);
    storeCount(start_timestamp, end_timestamp, false, false, interval, 2);
    
    aggr = Aggregators.AVG;
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));

    int i = 300;
    long ts = start_timestamp * 1000;
    
    for (final DataPoint dp : dps[0]) {
      assertFalse(dp.isInteger());
      assertEquals(i, dp.doubleValue(), 0.0001);
      assertEquals(ts, dp.timestamp());
      ts += interval.getInterval() * 1000;
      i += interval.getInterval() / 2;
    }
    assertEquals(73, dps[0].size());
  }
  
  @Test
  public void run10mAvgLongSingleTSMissingCount() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    Aggregator aggr = Aggregators.SUM;
    long start_timestamp = 1356998400L;
    long end_timestamp = 1357041600L;
    storeLongRollup(start_timestamp, end_timestamp, false, false, interval, aggr);

    aggr = Aggregators.AVG;
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals("", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertNull(dps[0].getTags().get(TAGK_STRING));
    assertFalse(dps[0].iterator().hasNext());
    assertEquals(0, dps[0].size());
  }
  
  @Test
  public void run10mAvgLongSingleTSMissingSum() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    final Aggregator aggr = Aggregators.AVG;
    long start_timestamp = 1356998400L;
    long end_timestamp = 1357041600L;
    storeCount(start_timestamp, end_timestamp, false, false, interval, 1);

    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals("", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertNull(dps[0].getTags().get(TAGK_STRING));
    assertFalse(dps[0].iterator().hasNext());
    assertEquals(0, dps[0].size());
  }
  
  @Test
  public void run10mAvgLongSingleTSMissingACount() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    
    storePoint(1356998400, 20, Aggregators.SUM, interval);
    storePoint(1356998400, 2, Aggregators.COUNT, interval);
    storePoint(1356999000, 40, Aggregators.SUM, interval);
    //storePoint(1356999000, 5, Aggregators.COUNT, interval);
    storePoint(1356999600, 60, Aggregators.SUM, interval);
    storePoint(1356999600, 3, Aggregators.COUNT, interval);
    storePoint(1357000200, 80, Aggregators.SUM, interval);
    storePoint(1357000200, 4, Aggregators.COUNT, interval);
    
    Aggregator aggr = Aggregators.AVG;
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));

    final SeekableView it = dps[0].iterator();
    DataPoint dp = it.next();
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(10, dp.doubleValue(), 0.0001);
    dp = it.next();
    assertEquals(1356999600000L, dp.timestamp());
    assertEquals(20, dp.doubleValue(), 0.0001);
    dp = it.next();
    assertEquals(1357000200000L, dp.timestamp());
    assertEquals(20, dp.doubleValue(), 0.0001);
    assertFalse(it.hasNext());
    assertEquals(3, dps[0].size());
  }
  
  @Test
  public void run10mAvgLongSingleTSMissingASum() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    
    storePoint(1356998400, 20, Aggregators.SUM, interval);
    storePoint(1356998400, 2, Aggregators.COUNT, interval);
    //storePoint(1356999000, 40, Aggregators.SUM, interval);
    storePoint(1356999000, 5, Aggregators.COUNT, interval);
    storePoint(1356999600, 60, Aggregators.SUM, interval);
    storePoint(1356999600, 3, Aggregators.COUNT, interval);
    storePoint(1357000200, 80, Aggregators.SUM, interval);
    storePoint(1357000200, 4, Aggregators.COUNT, interval);
    
    Aggregator aggr = Aggregators.AVG;
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));

    final SeekableView it = dps[0].iterator();
    DataPoint dp = it.next();
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(10, dp.doubleValue(), 0.0001);
    dp = it.next();
    assertEquals(1356999600000L, dp.timestamp());
    assertEquals(20, dp.doubleValue(), 0.0001);
    dp = it.next();
    assertEquals(1357000200000L, dp.timestamp());
    assertEquals(20, dp.doubleValue(), 0.0001);
    assertFalse(it.hasNext());
    assertEquals(3, dps[0].size());
  }
  
  @Test
  public void run10mAvgLongSingleTSMissingToZero() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    
    storePoint(1356998400, 20, Aggregators.SUM, interval);
    //storePoint(1356998400, 2, Aggregators.COUNT, interval);
    //storePoint(1356999000, 40, Aggregators.SUM, interval);
    storePoint(1356999000, 5, Aggregators.COUNT, interval);
    storePoint(1356999600, 60, Aggregators.SUM, interval);
    //storePoint(1356999600, 3, Aggregators.COUNT, interval);
    //storePoint(1357000200, 80, Aggregators.SUM, interval);
    storePoint(1357000200, 4, Aggregators.COUNT, interval);
    
    Aggregator aggr = Aggregators.AVG;
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals("", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertNull(dps[0].getTags().get(TAGK_STRING));

    final SeekableView it = dps[0].iterator();
    assertFalse(it.hasNext());
    assertEquals(0, dps[0].size());
  }
  
  @Test
  public void run10mAvgLongSingleTSMissingToZeroOneSpan() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    
    // For this test, a span in the middle was zero'd out
    storePoint(1356998400, 20, Aggregators.SUM, interval);
    storePoint(1356998400, 2, Aggregators.COUNT, interval);
    storePoint(1356999000, 40, Aggregators.SUM, interval);
    storePoint(1356999000, 5, Aggregators.COUNT, interval);
    
    storePoint(1357084800, 60, Aggregators.SUM, interval);
    //storePoint(1357084800, 3, Aggregators.COUNT, interval);
    //storePoint(1357085400, 80, Aggregators.SUM, interval);
    storePoint(1357085400, 4, Aggregators.COUNT, interval);
    
    storePoint(1357171200, 90, Aggregators.SUM, interval);
    storePoint(1357171200, 3, Aggregators.COUNT, interval);
    storePoint(1357171800, 100, Aggregators.SUM, interval);
    storePoint(1357171800, 5, Aggregators.COUNT, interval);
    
    Aggregator aggr = Aggregators.AVG;
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    ts_query.setEnd("1359590400");
    ts_query.validateAndSetQuery();
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));

    final SeekableView it = dps[0].iterator();
    DataPoint dp = it.next();
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(10, dp.doubleValue(), 0.0001);
    dp = it.next();
    assertEquals(1356999000000L, dp.timestamp());
    assertEquals(8, dp.doubleValue(), 0.0001);
    dp = it.next();
    assertEquals(1357171200000L, dp.timestamp());
    assertEquals(30, dp.doubleValue(), 0.0001);
    dp = it.next();
    assertEquals(1357171800000L, dp.timestamp());
    assertEquals(20, dp.doubleValue(), 0.0001);
    assertFalse(it.hasNext());
    assertEquals(4, dps[0].size());
  }
  
  @Test
  public void run10mAvgLongSingleTSMissingToZeroBookends() throws Exception {
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    
    // For this test, the spans on either side are zero'd
    storePoint(1356998400, 20, Aggregators.SUM, interval);
    //storePoint(1356998400, 2, Aggregators.COUNT, interval);
    //storePoint(1356999000, 40, Aggregators.SUM, interval);
    storePoint(1356999000, 5, Aggregators.COUNT, interval);
    
    storePoint(1357084800, 60, Aggregators.SUM, interval);
    storePoint(1357084800, 3, Aggregators.COUNT, interval);
    storePoint(1357085400, 80, Aggregators.SUM, interval);
    storePoint(1357085400, 4, Aggregators.COUNT, interval);
    
    //storePoint(1357171200, 90, Aggregators.SUM, interval);
    storePoint(1357171200, 3, Aggregators.COUNT, interval);
    storePoint(1357171800, 100, Aggregators.SUM, interval);
    //storePoint(1357171800, 5, Aggregators.COUNT, interval);
    
    Aggregator aggr = Aggregators.AVG;
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    ts_query.setEnd("1359590400");
    ts_query.validateAndSetQuery();
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));

    final SeekableView it = dps[0].iterator();
    DataPoint dp = it.next();
    assertEquals(1357084800000L, dp.timestamp());
    assertEquals(20, dp.doubleValue(), 0.0001);
    dp = it.next();
    assertEquals(1357085400000L, dp.timestamp());
    assertEquals(20, dp.doubleValue(), 0.0001);
    assertFalse(it.hasNext());
    assertEquals(2, dps[0].size());
  }
  
  @Test
  public void runDupes() throws Exception {
    storage.flushStorage();

    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    final Aggregator aggr = Aggregators.SUM;
    
    tsdb.addAggregatePoint(METRIC_STRING, 1357026600L, Integer.MAX_VALUE, tags, false, 
        interval.getStringInterval(), aggr.toString()).joinUninterruptibly();
    tsdb.addAggregatePoint(METRIC_STRING, 1357026600L, 42.5F, tags, false, 
        interval.getStringInterval(), aggr.toString()).joinUninterruptibly();
    
    setQuery(interval.getStringInterval(), aggr, tags, aggr);
    query.configureFromQuery(ts_query, 0);
    DataPoints[] dps = null;
    try {
      dps = query.run();
    } catch (IllegalDataException e) { }
    
    config.setFixDuplicates(true);
    dps = query.run();
    DataPoint dp = dps[0].iterator().next();
    assertEquals(1357026600000L, dp.timestamp());
    assertEquals(42.5F, dp.toDouble(), 0.0001);
  }
  
  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private void storeLongRollup(final long start_timestamp,
        final long end_timestamp,
        final boolean two_metrics, 
        final boolean offset, 
        final RollupInterval interval, 
        final Aggregator aggr) throws Exception {
    
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    int time_interval = interval.getInterval();
    long start_a = start_timestamp;
    long start_b = start_timestamp + (offset ? time_interval : 0);
    int i = 0;

    while (start_a <= end_timestamp) {
      i += time_interval;
      tsdb.addAggregatePoint(METRIC_STRING, start_a, i, tags, false, 
          interval.getStringInterval(), aggr.toString()).joinUninterruptibly();
      if (two_metrics) {
        tsdb.addAggregatePoint(METRIC_B_STRING, start_b, i, tags, false, 
            interval.getStringInterval(), aggr.toString()).joinUninterruptibly();
      }
      start_a += (offset ? time_interval * 2 : time_interval);
      start_b += (offset ? time_interval * 2 : time_interval);
    }

    // dump a parallel set but invert the values
    start_a = start_timestamp;
    start_b = start_timestamp + (offset ? time_interval : 0);
    
    while (start_a <= end_timestamp) {
      i -= time_interval;
      tsdb.addAggregatePoint(METRIC_STRING, start_a, i, tags2, false,
          interval.getStringInterval(), aggr.toString()).joinUninterruptibly();
      if (two_metrics) {
        tsdb.addAggregatePoint(METRIC_B_STRING, start_b, i, tags2, false,
            interval.getStringInterval(), aggr.toString()).joinUninterruptibly();
      }
      
      start_a += (offset ? time_interval * 2 : time_interval);
      start_b += (offset ? time_interval * 2 : time_interval);
    }
  }
  
  private void storeCount(final long start_timestamp,
      final long end_timestamp,
      final boolean two_metrics, 
      final boolean offset, 
      final RollupInterval interval, 
      final int value) throws Exception {
  
  // dump a bunch of rows of two metrics so that we can test filtering out
  // on the metric
  int time_interval = interval.getInterval();
  long start_a = start_timestamp;
  long start_b = start_timestamp + (offset ? time_interval : 0);

  while (start_a <= end_timestamp) {
    tsdb.addAggregatePoint(METRIC_STRING, start_a, value, tags, false, 
        interval.getStringInterval(), Aggregators.COUNT.toString())
        .joinUninterruptibly();
    if (two_metrics) {
      tsdb.addAggregatePoint(METRIC_B_STRING, start_b, value, tags, false, 
          interval.getStringInterval(), Aggregators.COUNT.toString())
          .joinUninterruptibly();
    }
    start_a += (offset ? time_interval * 2 : time_interval);
    start_b += (offset ? time_interval * 2 : time_interval);
  }

  start_a = start_timestamp;
  start_b = start_timestamp + (offset ? time_interval : 0);
  
  while (start_a <= end_timestamp) {
    tsdb.addAggregatePoint(METRIC_STRING, start_a, value, tags2, false,
        interval.getStringInterval(), Aggregators.COUNT.toString())
        .joinUninterruptibly();
    if (two_metrics) {
      tsdb.addAggregatePoint(METRIC_B_STRING, start_b, value, tags2, false,
          interval.getStringInterval(), Aggregators.COUNT.toString())
          .joinUninterruptibly();
    }
    
    start_a += (offset ? time_interval * 2 : time_interval);
    start_b += (offset ? time_interval * 2 : time_interval);
  }
}
  
  private void storeFloatRollup(final long start_timestamp,
        final long end_timestamp,
        final boolean two_metrics, 
        final boolean offset, 
        final RollupInterval interval, 
        final Aggregator aggr) throws Exception {
    
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    int time_interval = interval.getInterval();
    long start_a = start_timestamp;
    long start_b = start_timestamp + (offset ? time_interval : 0);
    float i = 0.5F;

    while (start_a <= end_timestamp) {
      i += time_interval;
      tsdb.addAggregatePoint(METRIC_STRING, start_a, i, tags, false, 
          interval.getStringInterval(), aggr.toString()).joinUninterruptibly();
      
      if (two_metrics) {
        tsdb.addAggregatePoint(METRIC_B_STRING, start_b,i, tags, false, 
            interval.getStringInterval(), aggr.toString()).joinUninterruptibly();
      }
      start_a += (offset ? time_interval * 2 : time_interval);
      start_b += (offset ? time_interval * 2 : time_interval);
    }

    // dump a parallel set but invert the values
    start_a = start_timestamp;
    start_b = start_timestamp + (offset ? time_interval : 0);
    
    while (start_a <= end_timestamp) {
      i -= time_interval;
      tsdb.addAggregatePoint(METRIC_STRING, start_a, i, tags2, false, 
          interval.getStringInterval(), aggr.toString()).joinUninterruptibly();
      if (two_metrics) {
        tsdb.addAggregatePoint(METRIC_B_STRING, start_b, i, tags2, false, 
            interval.getStringInterval(), aggr.toString()).joinUninterruptibly();
      }
      
      start_a += (offset ? time_interval * 2 : time_interval);
      start_b += (offset ? time_interval * 2 : time_interval);
    }
  }

  private void storePoint(final long ts, final long value, final Aggregator agg, 
      final RollupInterval interval) throws Exception {
    tsdb.addAggregatePoint(METRIC_STRING, ts, value, tags, false, 
        interval.getStringInterval(), agg.toString()).joinUninterruptibly();
  }
  
  @SuppressWarnings("deprecation")
  private void setQuery(final String ds_interval, final Aggregator ds_agg, 
      final Map<String, String> tags, final Aggregator group_by) {
    ts_query = new TSQuery();
    ts_query.setStart("1356998400");
    ts_query.setEnd("1357041600");
    
    final TSSubQuery sub = new TSSubQuery();
    sub.setMetric(METRIC_STRING);
    sub.setDownsample(ds_interval + "-" + ds_agg);
    sub.setTags(new HashMap<String, String>(tags));
    sub.setAggregator(group_by.toString());

    ts_query.setQueries(Arrays.asList(sub));
    ts_query.validateAndSetQuery();
  }
}
