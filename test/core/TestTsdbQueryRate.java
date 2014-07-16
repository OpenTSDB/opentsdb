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
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import net.opentsdb.utils.DateTime;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

import com.stumbleupon.async.Deferred;
import net.opentsdb.uid.NoSuchUniqueName;
import org.junit.Before;

import net.opentsdb.storage.MockBase;

import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import org.apache.zookeeper.proto.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests TsdbQuery for rate conversion.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class,
  CompactionQueue.class, GetRequest.class, PutRequest.class, KeyValue.class,
  Scanner.class, TsdbQuery.class, DeleteRequest.class, Annotation.class,
  RowKey.class, Span.class, SpanGroup.class, IncomingDataPoints.class })
public class TestTsdbQueryRate {

  private Config config;
  private TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueId metrics = mock(UniqueId.class);
  private UniqueId tag_names = mock(UniqueId.class);
  private UniqueId tag_values = mock(UniqueId.class);
  private TsdbQuery query = null;
  private MockBase storage = null;

  @Before
  public void before() throws Exception {
    config = new Config(false);
    tsdb = new TSDB(config);
    query = new TsdbQuery(tsdb);

    // replace the "real" field objects with mocks
    Field cl = tsdb.getClass().getDeclaredField("client");
    cl.setAccessible(true);
    cl.set(tsdb, client);

    Field met = tsdb.getClass().getDeclaredField("metrics");
    met.setAccessible(true);
    met.set(tsdb, metrics);

    Field tagk = tsdb.getClass().getDeclaredField("tag_names");
    tagk.setAccessible(true);
    tagk.set(tsdb, tag_names);

    Field tagv = tsdb.getClass().getDeclaredField("tag_values");
    tagv.setAccessible(true);
    tagv.set(tsdb, tag_values);

    // mock UniqueId
    when(metrics.getId("sys.cpu.user")).thenReturn(new byte[] { 0, 0, 1 });
    when(metrics.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("sys.cpu.user"));
    when(metrics.getId("sys.cpu.system"))
      .thenThrow(new NoSuchUniqueName("sys.cpu.system", "metric"));
    when(metrics.getId("sys.cpu.nice")).thenReturn(new byte[] { 0, 0, 2 });
    when(metrics.getNameAsync(new byte[] { 0, 0, 2 }))
      .thenReturn(Deferred.fromResult("sys.cpu.nice"));
    when(tag_names.getId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getIdAsync("host")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_names.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("host"));
    when(tag_names.getOrCreateIdAsync("host")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_names.getIdAsync("dc"))
      .thenThrow(new NoSuchUniqueName("dc", "metric"));
    when(tag_values.getId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getIdAsync("web01")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_values.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("web01"));
    when(tag_values.getOrCreateIdAsync("web01")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_values.getId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getIdAsync("web02")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 2 }));
    when(tag_values.getNameAsync(new byte[] { 0, 0, 2 }))
      .thenReturn(Deferred.fromResult("web02"));
    when(tag_values.getOrCreateIdAsync("web02")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 2 }));
    when(tag_values.getId("web03"))
      .thenThrow(new NoSuchUniqueName("web03", "metric"));

    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
  }

  @Test
  public void testScanTime_rate() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM,
                        true, new RateOptions());
    query.setInterpolationWindow(DateTime.parseDuration("100s"));
    int downsampleInterval = (int)DateTime.parseDuration("60s");
    query.setPredownsample(new DownsampleOptions(1000, Aggregators.SUM));
    query.setPostdownsample(new DownsampleOptions(downsampleInterval, Aggregators.SUM));
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    assertEquals(60000, TsdbQuery.ForTesting.getDownsampleIntervalMs(query));
    long scanStartTime = 1356998400 - downsampleInterval / 1000 -
                         DateTime.parseDuration("100s") * 2 / 1000;
    assertEquals(scanStartTime,
                 TsdbQuery.ForTesting.getScanStartTimeSeconds(query));
    long hbaseScanStartTime = scanStartTime - Const.MAX_TIMESPAN_SECS + 1;
    assertEquals(hbaseScanStartTime,
                 TsdbQuery.ForTesting.getHBaseScannerStartTimeSeconds(query));
    long scanEndTime = 1357041600 + downsampleInterval / 1000 +
                       DateTime.parseDuration("100s") / 1000;
    assertEquals(scanEndTime,
                 TsdbQuery.ForTesting.getScanEndTimeSeconds(query));
  }

  @Test
  public void testScanTimeMilliseconds_rate() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM,
                        true, new RateOptions());
    query.setInterpolationWindow(DateTime.parseDuration("110s"));
    int downsampleInterval = (int)DateTime.parseDuration("60s");
    query.setPredownsample(new DownsampleOptions(1000, Aggregators.SUM));
    query.setPostdownsample(new DownsampleOptions(downsampleInterval, Aggregators.SUM));
    query.setStartTime(2356998400000L);
    query.setEndTime(2357041600000L);
    assertEquals(60000, TsdbQuery.ForTesting.getDownsampleIntervalMs(query));
    long scanStartTime = 2356998400L - downsampleInterval / 1000 -
                         DateTime.parseDuration("110s") * 2 / 1000;
    assertEquals(scanStartTime,
                 TsdbQuery.ForTesting.getScanStartTimeSeconds(query));
    long hbaseScanStartTime = scanStartTime - Const.MAX_TIMESPAN_SECS + 1;
    assertEquals(hbaseScanStartTime,
                 TsdbQuery.ForTesting.getHBaseScannerStartTimeSeconds(query));
    long scanEndTime = 2357041600L + downsampleInterval / 1000 +
                       DateTime.parseDuration("110s") / 1000;
    assertEquals(scanEndTime,
                 TsdbQuery.ForTesting.getScanEndTimeSeconds(query));
  }

  @Test
  public void testScanTime_rateBigDownsampleTime() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM,
                        true, new RateOptions());
    query.setInterpolationWindow(DateTime.parseDuration("110s"));
    int downsampleInterval = (int)DateTime.parseDuration("200s");
    query.setPredownsample(new DownsampleOptions(1000, Aggregators.SUM));
    query.setPostdownsample(new DownsampleOptions(downsampleInterval, Aggregators.SUM));
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    assertEquals(200000, TsdbQuery.ForTesting.getDownsampleIntervalMs(query));
    long scanStartTime = 1356998400 - downsampleInterval / 1000 -
                         DateTime.parseDuration("110s") * 2 / 1000;
    assertEquals(scanStartTime,
                 TsdbQuery.ForTesting.getScanStartTimeSeconds(query));
    long hbaseScanStartTime = scanStartTime - Const.MAX_TIMESPAN_SECS + 1;
    assertEquals(hbaseScanStartTime,
                 TsdbQuery.ForTesting.getHBaseScannerStartTimeSeconds(query));
    long scanEndTime = 1357041600 + downsampleInterval / 1000 +
                       DateTime.parseDuration("110s") / 1000;
    assertEquals(scanEndTime,
                 TsdbQuery.ForTesting.getScanEndTimeSeconds(query));
  }

  @Test
  public void runLongSingleTSRate() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    for (DataPoint dp : dps[0]) {
      assertEquals(0.033F, dp.doubleValue(), 0.001);
    }
    assertEquals(299, dps[0].size());
  }

  @Test
  public void runLongSingleTSRate_zeroInterpolationWindow() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true);
    query.setInterpolationWindow(0);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // No rates can be calculated.
    assertEquals(0, dps[0].size());
  }

  @Test
  public void runLongSingleTSRate_timeLimit() throws Exception {
    setQueryStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    tsdb.addPoint("sys.cpu.user", timestamp, 0, tags).joinUninterruptibly();
    // The rate is 30/30 and not valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 30, tags).joinUninterruptibly();
    // The rate is 30/30 and not valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 60, tags).joinUninterruptibly();
    // The rate is 30/15 and valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 15, 90, tags).joinUninterruptibly();
    // The rate is 30/15 and valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 15, 120, tags).joinUninterruptibly();
    // The rate is 30/30 and not valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 150, tags).joinUninterruptibly();
    // The rate is 15/15 and valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 15, 165, tags).joinUninterruptibly();
    // The rate is 15/30 and not valid.
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 195, tags).joinUninterruptibly();

    RateOptions ro = new RateOptions(true, Long.MAX_VALUE, 0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true, ro);
    query.setInterpolationWindow(DateTime.parseDuration("20s"));
    final DataPoints[] dps = query.run();
    assertEquals(3, dps[0].size());

    // Test input:
    // time range: 1356998400 - 1356998550 seconds
    // time: 400, 415, 430, 445, 460, 475, 490, 505, 520, 535, 565
    // Web01   0,       30,       60,  90, 120,      150, 165, 195
    // Calculated rates:
    // rate01          1.0       1.0  2.0, 2.0,      1.0, 1.0  1.0
    // valid points                    *    *              *
    //                 ^^^       ^^^ <- dropped because of the big time gap.
    double[] expectedValues = new double[] {
        2.0, 2.0, 1.0
    };
    long[] expectedTimestamps = new long[] {
        1356998475, 1356998490, 1356998535
    };
    int i = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(expectedValues[i], dp.doubleValue(), 0);
      assertEquals(expectedTimestamps[i] * 1000, dp.timestamp());
      ++i;
    }
  }

  @Test
  public void runLongDoubleTSRate_timeLimit() throws Exception {
    setQueryStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    tsdb.addPoint("sys.cpu.user", timestamp, 0, tags).joinUninterruptibly();
    // The rate is 30/30 and not valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 30, tags).joinUninterruptibly();
    // The rate is 30/30 and not valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 60, tags).joinUninterruptibly();
    // The rate is 30/15 and valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 15, 90, tags).joinUninterruptibly();
    // The rate is 30/15 and valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 15, 120, tags).joinUninterruptibly();
    // The rate is 30/30 and not valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 150, tags).joinUninterruptibly();
    // The rate is 15/30 and not valid.
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 165, tags).joinUninterruptibly();

    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998415;
    tsdb.addPoint("sys.cpu.user", timestamp, 0, tags).joinUninterruptibly();
    // The rate is 30/30 and not valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 30, tags).joinUninterruptibly();
    // The rate is -30/30 and not valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 0, tags).joinUninterruptibly();
    // The rate is 30/15 and valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 15, 30, tags).joinUninterruptibly();
    // The rate is 30/15 and valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 15, 60, tags).joinUninterruptibly();
    // The rate is 30/30 and not valid for 20s interpolation window.
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 90, tags).joinUninterruptibly();
    // The rate is 15/30 and not valid.
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 105, tags).joinUninterruptibly();

    RateOptions ro = new RateOptions(true, Long.MAX_VALUE, 0);
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true, ro);
    query.setInterpolationWindow(DateTime.parseDuration("20s"));
    final DataPoints[] dps = query.run();
    assertEquals(3, dps[0].size());

    // Test input:
    // time range: 1356998400 - 1356998565 seconds
    // time: 400, 415, 430, 445, 460, 475, 490, 505, 520, 535, 550, 565
    // Web01   0,       30,       60,  90, 120,      150,      165
    // Web02        0,       30,        0,  30,  60,       90,      105
    // Calculated rates:
    // rate01          1.0       1.0  2.0, 2.0,      1.0,      0.5
    // valid01                         *    *
    // rate02               1.0      -1.0  2.0, 2.0,      1.0,      0.5
    // valid01                              *    *
    // agg values                     2.0  4.0  2.0            0.5  0.5
    //                                ^^^ <- -1.0 of rate02 is dropped
    // because of the big time gap.
    double[] expectedValues = new double[] {
        2.0, 4.0, 2.0
    };
    long[] expectedTimestamps = new long[] {
        1356998475, 1356998490, 1356998505
    };
    int i = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals("i = " + i, expectedValues[i], dp.doubleValue(), 0);
      assertEquals("i = " + i, expectedTimestamps[i] * 1000, dp.timestamp());
      ++i;
    }
  }

  @Test
  public void runLongSingleTSRateMs() throws Exception {
    storeLongTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    for (DataPoint dp : dps[0]) {
      assertEquals(2.0F, dp.doubleValue(), 0.001);
    }
    assertEquals(299, dps[0].size());
  }

  @Test
  public void runFloatSingleTSRate() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    for (DataPoint dp : dps[0]) {
      assertEquals(0.00833F, dp.doubleValue(), 0.00001);
    }
    assertEquals(299, dps[0].size());
  }

  @Test
  public void runFloatSingleTSRateMs() throws Exception {
    storeFloatTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    for (DataPoint dp : dps[0]) {
      assertEquals(0.5F, dp.doubleValue(), 0.00001);
    }
    assertEquals(299, dps[0].size());
  }

  @Test
  public void runRateCounterDefault() throws Exception {
    setQueryStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    tsdb.addPoint("sys.cpu.user", timestamp += 30, Long.MAX_VALUE - 55, tags)
      .joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp += 30, Long.MAX_VALUE - 25, tags)
      .joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 5, tags).joinUninterruptibly();

    RateOptions ro = new RateOptions(true, Long.MAX_VALUE, 0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true, ro);
    final DataPoints[] dps = query.run();

    for (DataPoint dp : dps[0]) {
      assertEquals(1.0, dp.doubleValue(), 0.001);
    }
    assertEquals(2, dps[0].size());
  }

  @Test
  public void runRateCounterDefaultNoOp() throws Exception {
    setQueryStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 30, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 60, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 90, tags).joinUninterruptibly();

    RateOptions ro = new RateOptions(true, Long.MAX_VALUE, 0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true, ro);
    final DataPoints[] dps = query.run();

    for (DataPoint dp : dps[0]) {
      assertEquals(1.0, dp.doubleValue(), 0.001);
    }
    assertEquals(2, dps[0].size());
  }

  @Test
  public void runRateCounterMaxSet() throws Exception {
    setQueryStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 45, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 75, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 5, tags).joinUninterruptibly();

    RateOptions ro = new RateOptions(true, 100, 0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true, ro);
    final DataPoints[] dps = query.run();

    for (DataPoint dp : dps[0]) {
      assertEquals(1.0, dp.doubleValue(), 0.001);
    }
    assertEquals(2, dps[0].size());
  }

  @Test
  public void runRateCounterAnomally() throws Exception {
    setQueryStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 45, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 75, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp += 30, 25, tags).joinUninterruptibly();

    RateOptions ro = new RateOptions(true, 10000, 35);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true, ro);
    final DataPoints[] dps = query.run();

    assertEquals(1.0, dps[0].doubleValue(0), 0.001);
    assertEquals(0, dps[0].doubleValue(1), 0.001);
    assertEquals(2, dps[0].size());
  }

  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private void storeLongTimeSeriesSeconds(final boolean two_metrics,
      final boolean offset) throws Exception {
    storeLongTimeSeriesSecondsWithBasetime(1356998400L, two_metrics, offset);
  }

  private void storeLongTimeSeriesSecondsWithBasetime(final long baseTimestamp,
      final boolean two_metrics, final boolean offset) throws Exception {
    setQueryStorage();
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = baseTimestamp;
    for (int i = 1; i <= 300; i++) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
      if (two_metrics) {
        tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
      }
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = baseTimestamp + (offset ? 15 : 0);
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
      if (two_metrics) {
        tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
      }
    }
  }

  private void storeLongTimeSeriesMs() throws Exception {
    setQueryStorage();
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400000L;
    for (int i = 1; i <= 300; i++) {
      tsdb.addPoint("sys.cpu.user", timestamp += 500, i, tags).joinUninterruptibly();
      tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998400000L;
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint("sys.cpu.user", timestamp += 500, i, tags).joinUninterruptibly();
      tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
    }
  }

  private void storeFloatTimeSeriesSeconds(final boolean two_metrics,
      final boolean offset) throws Exception {
    setQueryStorage();
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
      if (two_metrics) {
        tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
      }
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = offset ? 1356998415 : 1356998400;
    for (float i = 75F; i > 0; i -= 0.25F) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
      if (two_metrics) {
        tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
      }
    }
  }

  private void storeFloatTimeSeriesMs() throws Exception {
    setQueryStorage();
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400000L;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      tsdb.addPoint("sys.cpu.user", timestamp += 500, i, tags).joinUninterruptibly();
      tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998400000L;
    for (float i = 75F; i > 0; i -= 0.25F) {
      tsdb.addPoint("sys.cpu.user", timestamp += 500, i, tags).joinUninterruptibly();
      tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
    }
  }

  @SuppressWarnings("unchecked")
  private void setQueryStorage() throws Exception {
    storage = new MockBase(tsdb, client, true, true, true, true);
    storage.setFamily("t".getBytes(MockBase.ASCII()));

    PowerMockito.mockStatic(IncomingDataPoints.class);
    PowerMockito.doAnswer(
        new Answer<byte[]>() {
          @Override
          public byte[] answer(final InvocationOnMock args)
            throws Exception {
            final String metric = (String)args.getArguments()[1];
            final Map<String, String> tags = 
              (Map<String, String>)args.getArguments()[2];

            if (metric.equals("sys.cpu.user")) {
              if (tags.get("host").equals("web01")) {
                return new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1};
              } else {
                return new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 2};
              }
            } else {
              if (tags.get("host").equals("web01")) {
                return new byte[] { 0, 0, 2, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1};
              } else {
                return new byte[] { 0, 0, 2, 0, 0, 0, 0, 0, 0, 1, 0, 0, 2};
              }
            }
          }
        }
    ).when(IncomingDataPoints.class, "rowKeyTemplate", any(), anyString(), 
        any());
  }
}
