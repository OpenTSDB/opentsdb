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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.stumbleupon.async.Deferred;

import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

import org.apache.zookeeper.proto.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests downsampling with query.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class,
  CompactionQueue.class, GetRequest.class, PutRequest.class, KeyValue.class,
  Scanner.class, TsdbQuery.class, DeleteRequest.class, Annotation.class,
  RowKey.class, Span.class, SpanGroup.class, IncomingDataPoints.class })
public class TestTsdbQueryDownsample {

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
  public void downsample() throws Exception {
    int downsampleInterval = (int)DateTime.parseDuration("60s");
    query.downsample(downsampleInterval, Aggregators.SUM);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    assertEquals(60000, TsdbQuery.ForTesting.getDownsampleIntervalMs(query));
    long scanStartTime = 1356998400 - Const.MAX_TIMESPAN * 2 - 60;
    assertEquals(scanStartTime, TsdbQuery.ForTesting.getScanStartTimeSeconds(query));
    long scanEndTime = 1357041600 + Const.MAX_TIMESPAN + 1 + 60;
    assertEquals(scanEndTime, TsdbQuery.ForTesting.getScanEndTimeSeconds(query));
  }

  @Test
  public void downsampleMilliseconds() throws Exception {
    int downsampleInterval = (int)DateTime.parseDuration("60s");
    query.downsample(downsampleInterval, Aggregators.SUM);
    query.setStartTime(1356998400000L);
    query.setEndTime(1357041600000L);
    assertEquals(60000, TsdbQuery.ForTesting.getDownsampleIntervalMs(query));
    long scanStartTime = 1356998400 - Const.MAX_TIMESPAN * 2 - 60;
    assertEquals(scanStartTime, TsdbQuery.ForTesting.getScanStartTimeSeconds(query));
    long scanEndTime = 1357041600 + Const.MAX_TIMESPAN + 1 + 60;
    assertEquals(scanEndTime, TsdbQuery.ForTesting.getScanEndTimeSeconds(query));
  }

  @Test (expected = NullPointerException.class)
  public void downsampleNullAgg() throws Exception {
    query.downsample(60, null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void downsampleInvalidInterval() throws Exception {
    query.downsample(0, Aggregators.SUM);
  }

  @Test
  public void runLongSingleTSDownsample() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries: (1, 2, 3, 4, ..., 299, 300) at 30-second interval timestamps.
    // Timeseries in 60s intervals: (1, 2), (3, 4), ..., (299, 300)
    // Integer average downsampling: 1, 3, 5, ... 297, 299
    int i = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(i, dp.longValue());
      i += 2;
   }
    assertEquals(150, dps[0].size());
  }

  @Test
  public void runLongSingleTSDownsampleMs() throws Exception {
    storeLongTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(1000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    verify(client).newScanner(tsdb.table);
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries: (1, 2, 3, 4, ..., 299, 300) at 500-ms interval timestamps.
    // Timeseries in 1sec intervals: (1, 2), (3, 4), ..., (299, 300) - 150 DPs
    int i = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(i, dp.longValue());
      i += 2;
    }
    assertEquals(150, dps[0].size());
  }
  @Test
  public void runLongSingleTSDownsampleAndRate() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries: (1, 2, 3, 4, ..., 299, 300) at 30-second interval timestamps.
    // Integer average 60s downsampling: 1, 3, 5, ... 297, 299
    // Timeseries in rate: 2 every 60 seconds or 1/30 per second
    for (DataPoint dp : dps[0]) {
      assertEquals(0.033F, dp.doubleValue(), 0.001);
    }
    assertEquals(149, dps[0].size());
  }

  @Test
  public void runLongSingleTSDownsampleAndRateMs() throws Exception {
    storeLongTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(1000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries: (1, 2, 3, 4, ..., 299, 300) at 500-ms interval timestamps.
    // Integer average 1 sec downsampling: 1, 3, 5, ... 297, 299
    for (DataPoint dp : dps[0]) {
      assertEquals(2.0F, dp.doubleValue(), 0.001);
    }
    assertEquals(149, dps[0].size());
  }

  @Test
  public void runFloatSingleTSDownsample() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries in 30s intervals: (1.25, 1.5, 1.75, 2, 2.25, ..., 75.75, 76).
    // Float average 60s downsampling: 2.75/2, 3.75/2, ... 151.75/2
    double i = 1.375D;
    for (DataPoint dp : dps[0]) {
      assertEquals(i, dp.doubleValue(), 0.00001);
      i += 0.5D;
    }
    assertEquals(150, dps[0].size());
  }

  @Test
  public void runFloatSingleTSDownsampleMs() throws Exception {
    storeFloatTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(1000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries in 500ms intervals: (1.25, 1.5, 1.75, 2, ..., 75.75, 76).
    // Float average 1s downsampling: 2.75/2, 3.75/2, ... 151.75/2
    double i = 1.375D;
    for (DataPoint dp : dps[0]) {
      assertEquals(i, dp.doubleValue(), 0.00001);
      i += 0.5D;
    }
    assertEquals(150, dps[0].size());
  }

  @Test
  public void runFloatSingleTSDownsampleAndRate() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries in 30s intervals: (1.25, 1.5, 1.75, 2, 2.25, ..., 75.75, 76).
    // Float average 60s downsampling: 2.75/2, 3.75/2, ... 151.75/2
    // Rate = (3.75/2 - 2.75/2) / 60 = 1 / 120.
    for (DataPoint dp : dps[0]) {
      assertEquals(0.00833F, dp.doubleValue(), 0.00001);
    }
    assertEquals(149, dps[0].size());
  }

  @Test
  public void runFloatSingleTSDownsampleAndRateMs() throws Exception {
    storeFloatTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(1000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries in 500ms intervals: (1.25, 1.5, 1.75, 2, ..., 75.75, 76).
    // Float average 1s downsampling: 2.75/2, 3.75/2, ... 151.75/2
    for (DataPoint dp : dps[0]) {
      assertEquals(0.5F, dp.doubleValue(), 0.00001);
    }
    assertEquals(149, dps[0].size());
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
    ).when(IncomingDataPoints.class, "rowKeyTemplate", (TSDB)any(), anyString(),
        (Map<String, String>)any());
  }
}
