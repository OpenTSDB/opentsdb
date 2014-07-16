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
import net.opentsdb.utils.DateTime;

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
 * Tests interpolation of data with query.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class,
  CompactionQueue.class, GetRequest.class, PutRequest.class, KeyValue.class,
  Scanner.class, TsdbQuery.class, DeleteRequest.class, Annotation.class,
  RowKey.class, Span.class, SpanGroup.class, IncomingDataPoints.class })
public class TestTsdbQueryInterpolation {

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
  public void testScanStartTime_bigInterpolationTime() throws Exception {
    query.setInterpolationWindow(DateTime.parseDuration("12345s"));
    int downsampleInterval = (int)DateTime.parseDuration("200s");
    query.setPredownsample(new DownsampleOptions(downsampleInterval, Aggregators.SUM));
    query.setPostdownsample(new DownsampleOptions(downsampleInterval, Aggregators.SUM));
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    assertEquals(200000, TsdbQuery.ForTesting.getDownsampleIntervalMs(query));
    long scanStartTime = 1356998400 - downsampleInterval / 1000 -
        DateTime.parseDuration("12345s") / 1000;
    assertEquals(scanStartTime,
        TsdbQuery.ForTesting.getScanStartTimeSeconds(query));
    long hbaseScanStartTime = scanStartTime - Const.MAX_TIMESPAN_SECS + 1;
    assertEquals(hbaseScanStartTime,
        TsdbQuery.ForTesting.getHBaseScannerStartTimeSeconds(query));
    long scanEndTime = 1357041600 + downsampleInterval / 1000 +
        DateTime.parseDuration("12345s") / 1000;
    assertEquals(scanEndTime,
        TsdbQuery.ForTesting.getScanEndTimeSeconds(query));
  }

  @Test
  public void runInterpolationSeconds() throws Exception {
    prepareLongDataSecond();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());

    long v = 1;
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.longValue());

      if (dp.timestamp() == 1357007400000L) {
        v = 1;
      } else if (v == 1 || v == 302) {
        v = 301;
      } else {
        v = 302;
      }
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runInterpolationSeconds_timeWindow() throws Exception {
    prepareLongDataSecond();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    // All the interpolation data points will be dropped because the time gap
    // of them are bigger than 20 seconds.
    query.setInterpolationWindow(20000);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());

    long v = 0;
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      // Values should come from alternating spans because all the
      // interpolation data points should be dropped.
      if ((v % 2) == 0) {
        // web01 began with 1 and kept incremented.
        assertEquals(v / 2 + 1, dp.longValue());
      } else {
        // web02 began with 300 and kept decremented.
        assertEquals(300 - v / 2, dp.longValue());
      }
      ++v;
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runInterpolationSeconds_zeroInterpolationWindow() throws Exception {
    // Tests that the zero interpolation window causes no bug.
    prepareLongDataSecond();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    // All the interpolation data points will be dropped because the time gap
    // of them are bigger than 0 seconds.
    query.setInterpolationWindow(0);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());

    long v = 0;
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      // Values should come from alternating spans because all the
      // interpolation data points should be dropped.
      if ((v % 2) == 0) {
        // web01 began with 1 and kept incremented.
        assertEquals(v / 2 + 1, dp.longValue());
      } else {
        // web02 began with 300 and kept decremented.
        assertEquals(300 - v / 2, dp.longValue());
      }
      ++v;
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runInterpolation_doubeValueWithTimeWindow() throws Exception {
    prepareDoubleDataSecond();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    // All the interpolation data points will be dropped because the time gap
    // of them are bigger than 20 seconds.
    query.setInterpolationWindow(20000);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());

    long v = 0;
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      // Values should come from alternating spans because all the
      // interpolation data points should be dropped.
      if ((v % 2) == 0) {
        // web01 began with 1 and kept incremented.
        assertEquals(v / 2 + 1, dp.doubleValue(), 0.000000001);
      } else {
        // web02 began with 300 and kept decremented.
        assertEquals(300 - v / 2, dp.doubleValue(), 0.000000001);
      }
      ++v;
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runInterpolationMs() throws Exception {
    setQueryStorage();

    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400000L;
    for (int i = 1; i <= 300; i++) {
      tsdb.addPoint("sys.cpu.user", timestamp += 500, i, tags)
        .joinUninterruptibly();
    }

    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998400250L;
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint("sys.cpu.user", timestamp += 500, i, tags)
        .joinUninterruptibly();
    }

    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());

    long v = 1;
    long ts = 1356998400500L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 250;
      assertEquals(v, dp.longValue());

      if (dp.timestamp() == 1356998550000L) {
        v = 1;
      } else if (v == 1 || v == 302) {
        v = 301;
      } else {
        v = 302;
      }
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runInterpolationMsDownsampled() throws Exception {
    setQueryStorage();

    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    // ts = 1356998400500, v = 1
    // ts = 1356998401000, v = 2
    // ts = 1356998401500, v = 3
    // ts = 1356998402000, v = 4
    // ts = 1356998402500, v = 5
    // ...
    // ts = 1356998449000, v = 98
    // ts = 1356998449500, v = 99
    // ts = 1356998450000, v = 100
    // ts = 1356998455000, v = 101
    // ts = 1356998460000, v = 102
    // ...
    // ts = 1356998550000, v = 120
    long timestamp = 1356998400000L;
    for (int i = 1; i <= 120; i++) {
      timestamp += i <= 100 ? 500 : 5000;
      tsdb.addPoint("sys.cpu.user", timestamp, i, tags)
        .joinUninterruptibly();
    }

    // ts = 1356998400750, v = 300
    // ts = 1356998401250, v = 299
    // ts = 1356998401750, v = 298
    // ts = 1356998402250, v = 297
    // ts = 1356998402750, v = 296
    // ...
    // ts = 1356998549250, v = 3
    // ts = 1356998549750, v = 2
    // ts = 1356998550250, v = 1
    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998400250L;
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint("sys.cpu.user", timestamp += 500, i, tags)
        .joinUninterruptibly();
    }

    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    query.setPredownsample(new DownsampleOptions(1000, Aggregators.SUM));
    query.setPostdownsample(new DownsampleOptions(1000, Aggregators.SUM));
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());

    // TS1 in intervals = (1), (2,3), (4,5) ... (98,99), 100, (), (), (), (),
    //                    (101), ... (120)
    // TS2 in intervals = (300), (299,298), (297,296), ... (203, 202) ...
    //                    (3,2), (1)
    // TS1 downsample = 1, 5, 9, ... 197, 100, _, _, _, _, 101, ... 120
    // TS1 interpolation = 1, 5, ... 197, 100, 100.2, 100.4, 100.6, 100.8, 101,
    //                     ... 119.6, 119.8, 120
    // TS2 downsample = 300, 597, 593, ... 405, 401, ... 5, 1
    // TS1 + TS2 = 301, 602, 602, ... 501, 497.2, ... 124.8, 121
    int i = 0;
    long ts = 1356998400000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 1000;
      if (i == 0) {
        assertEquals(301, dp.doubleValue(), 0.0000001);
      } else if (i < 50) {
        // TS1 = i * 2 + i * 2 + 1
        // TS2 = (300 - i * 2 + 1) + (300 - i * 2)
        // TS1 + TS2 = 602
        assertEquals(602, dp.doubleValue(), 0.0000001);
      } else {
        // TS1 = 100 + (i - 50) * 0.2
        // TS2 = (300 - i * 2 + 1) + (300 - i * 2)
        // TS1 + TS2 = 701 + (i - 50) * 0.2 - i * 4
        double value = 701 + (i - 50) * 0.2 - i * 4;
        assertEquals(value, dp.doubleValue(), 0.0000001);
      }
      ++i;
    }
    assertEquals(151, dps[0].size());
  }

  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private void prepareLongDataSecond() throws Exception {
    setQueryStorage();

    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    for (int i = 1; i <= 300; i++) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags)
        .joinUninterruptibly();
    }

    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998415;
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags)
        .joinUninterruptibly();
    }
  }

  private void prepareDoubleDataSecond() throws Exception {
    setQueryStorage();

    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    for (int i = 1; i <= 300; i++) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, (double)i, tags)
        .joinUninterruptibly();
    }

    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998415;
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, (double)i, tags)
        .joinUninterruptibly();
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
