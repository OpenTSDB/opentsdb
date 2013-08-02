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
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.apache.zookeeper.proto.DeleteRequest;
import org.hbase.async.Bytes;
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

import com.stumbleupon.async.Deferred;

/**
 * Massive test class that is used to test all facets of querying for data. 
 * Since data is fetched using the TsdbQuery class, it makes sense to put all
 * of the unit tests here that deal with actual data. This includes:
 * - queries
 * - aggregations
 * - rate conversion
 * - downsampling
 * - compactions (read and write)
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  CompactionQueue.class, GetRequest.class, PutRequest.class, KeyValue.class, 
  Scanner.class, TsdbQuery.class, DeleteRequest.class, Annotation.class, 
  RowKey.class, Span.class, SpanGroup.class, IncomingDataPoints.class })
public final class TestTsdbQuery {
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
    when(metrics.getName(new byte[] { 0, 0, 1 })).thenReturn("sys.cpu.user");
    when(metrics.getId("sys.cpu.system"))
      .thenThrow(new NoSuchUniqueName("sys.cpu.system", "metric"));
    when(metrics.getId("sys.cpu.nice")).thenReturn(new byte[] { 0, 0, 2 });
    when(metrics.getName(new byte[] { 0, 0, 2 })).thenReturn("sys.cpu.nice");
    when(tag_names.getId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getIdAsync("host")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_names.getName(new byte[] { 0, 0, 1 })).thenReturn("host");
    when(tag_names.getOrCreateIdAsync("host")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_names.getIdAsync("dc")).thenThrow(new NoSuchUniqueName("dc", "metric"));
    when(tag_values.getId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getIdAsync("web01")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_values.getName(new byte[] { 0, 0, 1 })).thenReturn("web01");
    when(tag_values.getOrCreateIdAsync("web01")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_values.getId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getIdAsync("web02")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 2 }));
    when(tag_values.getName(new byte[] { 0, 0, 2 })).thenReturn("web02");
    when(tag_values.getOrCreateIdAsync("web02")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 2 }));
    when(tag_values.getId("web03"))
      .thenThrow(new NoSuchUniqueName("web03", "metric"));
    
    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
  }
  
  @Test
  public void setStartTime() throws Exception {
    query.setStartTime(1356998400L);
    assertEquals(1356998400L, query.getStartTime());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeInvalid() throws Exception {
    query.setStartTime(13717504770L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeInvalidNegative() throws Exception {
    query.setStartTime(-1L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeInvalidTooBig() throws Exception {
    query.setStartTime(17592186044416L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeEqualtoEndTime() throws Exception {
    query.setEndTime(1356998400L);
    query.setStartTime(1356998400L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeGreaterThanEndTime() throws Exception {
    query.setEndTime(1356998400L);
    query.setStartTime(1356998460L);
  }
  
  @Test
  public void setEndTime() throws Exception {
    query.setEndTime(1356998400L);
    assertEquals(1356998400L, query.getEndTime());
  }
  
  @Test (expected = IllegalStateException.class)
  public void getStartTimeNotSet() throws Exception {
    query.getStartTime();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeInvalidNegative() throws Exception {
    query.setEndTime(-1L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeInvalidTooBig() throws Exception {
    query.setEndTime(17592186044416L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeEqualtoEndTime() throws Exception {
    query.setStartTime(1356998400L);
    query.setEndTime(1356998400L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeGreaterThanEndTime() throws Exception {
    query.setStartTime(1356998460L);
    query.setEndTime(1356998400L);
  }
  
  @Test
  public void getEndTimeNotSet() throws Exception {
    PowerMockito.mockStatic(System.class);
    when(System.currentTimeMillis()).thenReturn(1357300800000L);
    assertEquals(1357300800000L, query.getEndTime());
  }
  
  @Test
  public void setTimeSeries() throws Exception {
    setQueryStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    assertNotNull(query);
  }
  
  @Test (expected = NullPointerException.class)
  public void setTimeSeriesNullTags() throws Exception {
    query.setTimeSeries("sys.cpu.user", null, Aggregators.SUM, false);
  }
  
  @Test
  public void setTimeSeriesEmptyTags() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    assertNotNull(query);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setTimeSeriesNosuchMetric() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setTimeSeries("sys.cpu.system", tags, Aggregators.SUM, false);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setTimeSeriesNosuchTagk() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("dc", "web01");
    query.setTimeSeries("sys.cpu.system", tags, Aggregators.SUM, false);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setTimeSeriesNosuchTagv() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web03");
    query.setTimeSeries("sys.cpu.system", tags, Aggregators.SUM, false);
  }

  @Test
  public void setTimeSeriesTS() throws Exception {
    final List<String> tsuids = new ArrayList<String>(2);
    tsuids.add("000001000001000001");
    tsuids.add("000001000001000002");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
    assertNotNull(query);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setTimeSeriesTSNullList() throws Exception {
    query.setTimeSeries(null, Aggregators.SUM, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setTimeSeriesTSEmptyList() throws Exception {
    final List<String> tsuids = new ArrayList<String>();
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setTimeSeriesTSDifferentMetrics() throws Exception {
    final List<String> tsuids = new ArrayList<String>(2);
    tsuids.add("000001000001000001");
    tsuids.add("000002000001000002");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
  }
  
  @Test
  public void downsample() throws Exception {
    query.downsample(60, Aggregators.SUM);
    assertNotNull(query);
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
  public void runLongSingleTS() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].aggregatedSize());
  }
  
  @Test
  public void runLongSingleTSMs() throws Exception {
    storeLongTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].aggregatedSize());
  }
  
  @Test
  public void runLongSingleTSNoData() throws Exception {
    setQueryStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals(0, dps.length);
  }
  
  @Test
  public void runLongTwoAggSum() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    HashMap<String, String> tags = new HashMap<String, String>();
    query.setStartTime(1356998400L);
    query.setEndTime(1357041600L);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    for (DataPoint dp : dps[0]) {
      assertEquals(301, dp.longValue());
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runLongTwoAggSumMs() throws Exception {
    storeLongTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>();
    query.setStartTime(1356998400L);
    query.setEndTime(1357041600L);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    for (DataPoint dp : dps[0]) {
      assertEquals(301, dp.longValue());
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runLongTwoGroup() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "*");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals(2, dps.length);
    
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    assertEquals("sys.cpu.user", dps[1].metricName());
    assertTrue(dps[1].getAggregatedTags().isEmpty());
    assertNull(dps[1].getAnnotations());
    assertEquals("web02", dps[1].getTags().get("host"));
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].size());
    
    value = 300;
    for (DataPoint dp : dps[1]) {
      assertEquals(value, dp.longValue());
      value--;
    }
    assertEquals(300, dps[1].size());
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
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
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
    
    for (DataPoint dp : dps[0]) {
      assertEquals(2.0F, dp.doubleValue(), 0.001);
    }
    assertEquals(149, dps[0].size());
  }

  @Test
  public void runLongSingleTSCompacted() throws Exception {
    storeLongCompactions();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].size());
  }
  
  // Can't run this one since the TreeMap will order the compacted row AFTER
  // the other data points. A full MockBase implementation would allow this
//  @Test
//  public void runLongSingleTSCompactedAndNonCompacted() throws Exception {
//    storeLongCompactions();
//    HashMap<String, String> tags = new HashMap<String, String>(1);
//    tags.put("host", "web01");
//    
//    long timestamp = 1357007460;
//    for (int i = 301; i <= 310; i++) {
//      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
//    }
//    storage.dumpToSystemOut(false);
//    query.setStartTime(1356998400);
//    query.setEndTime(1357041600);
//    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
//    final DataPoints[] dps = query.run();
//    assertNotNull(dps);
//    
//    int value = 1;
//    for (DataPoint dp : dps[0]) {
//      assertEquals(value, dp.longValue());
//      value++;
//    }
//    assertEquals(310, dps[0].size());
//  }
  
  @Test
  public void runFloatSingleTS() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    double value = 1.25D;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0.001);
      value += 0.25D;
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runFloatSingleTSMs() throws Exception {
    storeFloatTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    double value = 1.25D;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0.001);
      value += 0.25D;
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runFloatTwoAggSum() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    for (DataPoint dp : dps[0]) {
      assertEquals(76.25, dp.doubleValue(), 0.00001);
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runFloatTwoAggSumMs() throws Exception {
    storeFloatTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    for (DataPoint dp : dps[0]) {
      assertEquals(76.25, dp.doubleValue(), 0.00001);
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runFloatTwoGroup() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "*");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals(2, dps.length);
    
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    assertEquals("sys.cpu.user", dps[1].metricName());
    assertTrue(dps[1].getAggregatedTags().isEmpty());
    assertNull(dps[1].getAnnotations());
    assertEquals("web02", dps[1].getTags().get("host"));
    
    double value = 1.25D;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0.0001);
      value += 0.25D;
    }
    assertEquals(300, dps[0].size());
    
    value = 75D;
    for (DataPoint dp : dps[1]) {
      assertEquals(value, dp.doubleValue(), 0.0001);
      value -= 0.25d;
    }
    assertEquals(300, dps[1].size());
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
    
    for (DataPoint dp : dps[0]) {
      assertEquals(0.5F, dp.doubleValue(), 0.00001);
    }
    assertEquals(149, dps[0].size());
  }
  
  @Test
  public void runFloatSingleTSCompacted() throws Exception {
    storeFloatCompactions();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    double value = 1.25D;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0.001);
      value += 0.25D;
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMixedSingleTS() throws Exception {
    storeMixedTimeSeriesSeconds();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.AVG, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    double float_value = 1.25D;
    int int_value = 76;
    // due to aggregation, the only int that will be returned will be the very
    // last value of 76 since the agg will convert every point in between to a
    // double
    for (DataPoint dp : dps[0]) {
      if (dp.isInteger()) {
        assertEquals(int_value, dp.longValue());
        int_value++;
        float_value = int_value;
      } else {
        assertEquals(float_value, dp.doubleValue(), 0.001);
        float_value += 0.25D;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMixedSingleTSMsAndS() throws Exception {
    storeMixedTimeSeriesMsAndS();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.AVG, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    double float_value = 1.25D;
    int int_value = 76;
    // due to aggregation, the only int that will be returned will be the very
    // last value of 76 since the agg will convert every point in between to a
    // double
    for (DataPoint dp : dps[0]) {
      if (dp.isInteger()) {
        assertEquals(int_value, dp.longValue());
        int_value++;
        float_value = int_value;
      } else {
        assertEquals(float_value, dp.doubleValue(), 0.001);
        float_value += 0.25D;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMixedSingleTSPostCompaction() throws Exception {
    storeMixedTimeSeriesSeconds();
    
    final Field compact = Config.class.getDeclaredField("enable_compactions");
    compact.setAccessible(true);
    compact.set(config, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.AVG, false);
    assertNotNull(query.run());
    
    // this should only compact the rows for the time series that we fetched and
    // leave the others alone
    assertEquals(1, storage.numColumns(
        MockBase.stringToBytes("00000150E22700000001000001")));
    assertEquals(1, storage.numColumns(
        MockBase.stringToBytes("00000150E23510000001000001")));
    assertEquals(1, storage.numColumns(
        MockBase.stringToBytes("00000150E24320000001000001")));
    
    // run it again to verify the compacted data uncompacts properly
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    double float_value = 1.25D;
    int int_value = 76;
    // due to aggregation, the only int that will be returned will be the very
    // last value of 76 since the agg will convert every point in between to a
    // double
    for (DataPoint dp : dps[0]) {
      if (dp.isInteger()) {
        assertEquals(int_value, dp.longValue());
        int_value++;
        float_value = int_value;
      } else {
        assertEquals(float_value, dp.doubleValue(), 0.001);
        float_value += 0.25D;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMixedSingleTSCompacted() throws Exception {
    storeMixedCompactions();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.AVG, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    double float_value = 1.25D;
    int int_value = 76;
    // due to aggregation, the only int that will be returned will be the very
    // last value of 76 since the agg will convert every point in between to a
    // double
    for (DataPoint dp : dps[0]) {
      if (dp.isInteger()) {
        assertEquals(int_value, dp.longValue());
        int_value++;
        float_value = int_value;
      } else {
        assertEquals(float_value, dp.doubleValue(), 0.001);
        float_value += 0.25D;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runEndTime() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357001900);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(236, dps[0].size());
  }
  
  @Test
  public void runCompactPostQuery() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    
    final Field compact = Config.class.getDeclaredField("enable_compactions");
    compact.setAccessible(true);
    compact.set(config, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    assertNotNull(query.run());
  
    // this should only compact the rows for the time series that we fetched and
    // leave the others alone
    assertEquals(1, storage.numColumns(
        MockBase.stringToBytes("00000150E22700000001000001")));
    assertEquals(119, storage.numColumns(
        MockBase.stringToBytes("00000150E22700000001000002")));
    assertEquals(1, storage.numColumns(
        MockBase.stringToBytes("00000150E23510000001000001")));
    assertEquals(120, storage.numColumns(
        MockBase.stringToBytes("00000150E23510000001000002")));
    assertEquals(1, storage.numColumns(
        MockBase.stringToBytes("00000150E24320000001000001")));
    assertEquals(61, storage.numColumns(
        MockBase.stringToBytes("00000150E24320000001000002")));
    
    // run it again to verify the compacted data uncompacts properly
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test (expected = IllegalStateException.class)
  public void runStartNotSet() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    query.run();
  }

  @Test (expected = IllegalDataException.class)
  public void runFloatAndIntSameTS() throws Exception {
    // if a row has an integer and a float for the same timestamp, there will be
    // two different qualifiers that will resolve to the same offset. This tosses
    // an exception
    storeLongTimeSeriesSeconds(true, false);;
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998430, 42.5F, tags).joinUninterruptibly();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true);
    query.run();
  }
  
  @Test
  public void runWithAnnotation() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    
    final Annotation note = new Annotation();
    note.setTSUID("000001000001000001");
    note.setStartTime(1356998490);
    note.setDescription("Hello World!");
    note.syncToStorage(tsdb, false).joinUninterruptibly();
    
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals(1, dps[0].getAnnotations().size());
    assertEquals("Hello World!", dps[0].getAnnotations().get(0).getDescription());
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runWithAnnotationPostCompact() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    
    final Annotation note = new Annotation();
    note.setTSUID("000001000001000001");
    note.setStartTime(1356998490);
    note.setDescription("Hello World!");
    note.syncToStorage(tsdb, false).joinUninterruptibly();
    
    final Field compact = Config.class.getDeclaredField("enable_compactions");
    compact.setAccessible(true);
    compact.set(config, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    assertNotNull(query.run());
    
    // this should only compact the rows for the time series that we fetched and
    // leave the others alone
    assertEquals(2, storage.numColumns(
        MockBase.stringToBytes("00000150E22700000001000001")));
    assertEquals(119, storage.numColumns(
        MockBase.stringToBytes("00000150E22700000001000002")));
    assertEquals(1, storage.numColumns(
        MockBase.stringToBytes("00000150E23510000001000001")));
    assertEquals(120, storage.numColumns(
        MockBase.stringToBytes("00000150E23510000001000002")));
    assertEquals(1, storage.numColumns(
        MockBase.stringToBytes("00000150E24320000001000001")));
    assertEquals(61, storage.numColumns(
        MockBase.stringToBytes("00000150E24320000001000002")));
    
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals(1, dps[0].getAnnotations().size());
    assertEquals("Hello World!", dps[0].getAnnotations().get(0).getDescription());
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].size());
  } 
  
  @Test
  public void runWithOnlyAnnotation() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    
    // verifies that we can pickup an annotation stored all bye it's lonesome
    // in a row without any data
    storage.flushRow(MockBase.stringToBytes("00000150E23510000001000001"));
    final Annotation note = new Annotation();
    note.setTSUID("000001000001000001");
    note.setStartTime(1357002090);
    note.setDescription("Hello World!");
    note.syncToStorage(tsdb, false).joinUninterruptibly();
    
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals(1, dps[0].getAnnotations().size());
    assertEquals("Hello World!", dps[0].getAnnotations().get(0).getDescription());
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
      // account for the jump
      if (value == 120) {
        value = 240;
      }
    }
    assertEquals(180, dps[0].size());
  }
  
  @Test
  public void runTSUIDQuery() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add("000001000001000001");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].aggregatedSize());
  }
  
  @Test
  public void runTSUIDsAggSum() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add("000001000001000001");
    tsuids.add("000001000001000002");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    for (DataPoint dp : dps[0]) {
      assertEquals(301, dp.longValue());
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runTSUIDQueryNoData() throws Exception {
    setQueryStorage();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add("000001000001000001");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals(0, dps.length);
  }
  
  @Test
  public void runTSUIDQueryNoDataForTSUID() throws Exception {
    // this doesn't throw an exception since the UIDs are only looked for when
    // the query completes.
    setQueryStorage();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add("000001000001000005");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals(0, dps.length);
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void runTSUIDQueryNSU() throws Exception {
    when(metrics.getName(new byte[] { 0, 0, 1 }))
      .thenThrow(new NoSuchUniqueId("metrics", new byte[] { 0, 0, 1 }));
    storeLongTimeSeriesSeconds(true, false);;
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add("000001000001000001");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    dps[0].metricName();
  }
  
  @Test
  public void runRateCounterDefault() throws Exception {
    setQueryStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    tsdb.addPoint("sys.cpu.user", timestamp += 30, (long)(Long.MAX_VALUE - 55), tags)
      .joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp += 30, (long)(Long.MAX_VALUE - 25), tags)
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

  @Test
  public void runMultiCompact() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(1L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(2L);

    // 2nd compaction
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(3L);
    final byte[] qual4 = { 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(4L);

    // 3rd compaction
    final byte[] qual5 = { 0x00, 0x57 };
    final byte[] val5 = Bytes.fromLong(5L);
    final byte[] qual6 = { 0x00, 0x67 };
    final byte[] val6 = Bytes.fromLong(6L);

    final byte[] KEY = { 0, 0, 1, 0x50, (byte) 0xE2, 
        0x27, 0x00, 0, 0, 1, 0, 0, 1 };
    
    setQueryStorage();
    storage.addColumn(KEY, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(KEY, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));
    storage.addColumn(KEY, 
        MockBase.concatByteArrays(qual5, qual6), 
        MockBase.concatByteArrays(val5, val6, new byte[] { 0 }));
    
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(6, dps[0].aggregatedSize());
  }

  @Test
  public void runMultiCompactAndSingles() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(1L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(2L);

    // 2nd compaction
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(3L);
    final byte[] qual4 = { 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(4L);

    // 3rd compaction
    final byte[] qual5 = { 0x00, 0x57 };
    final byte[] val5 = Bytes.fromLong(5L);
    final byte[] qual6 = { 0x00, 0x67 };
    final byte[] val6 = Bytes.fromLong(6L);

    final byte[] KEY = { 0, 0, 1, 0x50, (byte) 0xE2, 
        0x27, 0x00, 0, 0, 1, 0, 0, 1 };
    
    setQueryStorage();
    storage.addColumn(KEY, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(KEY, qual3, val3);
    storage.addColumn(KEY, qual4, val4);
    storage.addColumn(KEY, 
        MockBase.concatByteArrays(qual5, qual6), 
        MockBase.concatByteArrays(val5, val6, new byte[] { 0 }));
    
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(6, dps[0].aggregatedSize());
  }
  
  @Test
  public void runInterpolationSeconds() throws Exception {
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
    query.downsample(1000, Aggregators.SUM);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long v = 3;
    long ts = 1356998400750L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      if ((ts % 1000) != 0) {
        ts += 250;
      } else {
        ts += 750;
      }
      assertEquals(v, dp.longValue());
      
      if (dp.timestamp() == 1356998549750L) {
        v = 3;
      } else {
        v = 603;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  //---------------------- //
  // Aggregator unit tests //
  // --------------------- //
  
  @Test
  public void runZimSum() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.ZIMSUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(301, dp.longValue());
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runZimSumFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.ZIMSUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(76.25, dp.doubleValue(), 0.001);
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runZimSumOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.ZIMSUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long v1 = 1;
    long v2 = 300;
    long ts = 1356998430000L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      
      if (counter % 2 == 0) {        
        assertEquals(v1, dp.longValue());
        v1++;
      } else {
        assertEquals(v2, dp.longValue());
        v2--;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runZimSumFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.ZIMSUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    double v1 = 1.25;
    double v2 = 75.0;
    long ts = 1356998430000L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      if (counter % 2 == 0) {
        assertEquals(v1, dp.doubleValue(), 0.001);
        v1 += 0.25;
      } else {
        assertEquals(v2, dp.doubleValue(), 0.001);
        v2 -= 0.25;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMin() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MIN, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long v = 1;
    long ts = 1356998430000L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());
      
      if (decrement) {
        v--;
      } else {
        v++;
      }
      
      if (v == 151){
        v = 150;
        decrement = true;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMinFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MIN, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    double v = 1.25;
    long ts = 1356998430000L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.0001);
      
      if (decrement) {
        v -= .25;
      } else {
        v += .25;
      }
      
      if (v > 38){
        v = 38.0;
        decrement = true;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMinOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MIN, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long v = 1;
    long ts = 1356998430000L;
    int counter = 0;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.longValue());
      if (counter % 2 != 0) {
        if (decrement) {
          v--;
        } else {
          v++;
        }
      } else if (v == 151){
        v = 150;
        decrement = true;
        counter--; // hack since the hump is 150 150 151 150 150
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMinFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MIN, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    double v = 1.25;
    long ts = 1356998430000L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.doubleValue(), 0.001);
      if (decrement) {
        v -= 0.125;
      } else {
        v += 0.125;
      }
      
      if (v > 38.125){
        v = 38.125;
        decrement = true;
      }
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMax() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MAX, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long v = 300;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());

      if (decrement) {
        v--;
      } else {
        v++;
      }

      if (v == 150){
        v = 151;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMaxFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MAX, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    double v = 75.0;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.001);

      if (decrement) {
        v -= .25;
      } else {
        v += .25;
      }

      if (v < 38.25){
        v = 38.25;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMaxOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MAX, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long v = 1;
    long ts = 1356998430000L;
    int counter = 0;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.longValue());
      if (v == 1) {
        v = 300;
      } else if (dp.timestamp() == 1357007400000L) {
        v = 1;
      } else if (counter % 2 == 0) {
        if (decrement) {
          v--;
        } else {
          v++;
        }
      } 
      
      if (v == 150){
        v = 151;
        decrement = false;
        counter--; // hack since the hump is 151 151 151
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMaxFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MAX, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    double v = 1.25;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.doubleValue(), .0001);
      if (v == 1.25) {
        v = 75.0;
      } else if (dp.timestamp() == 1357007400000L) {
        v = 0.25;
      } else {
        if (decrement) {
          v -= .125;
        } else {
          v += .125;
        }
        
        if (v < 38.25){
          v = 38.25;
          decrement = false;
        }
      }
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runAvg() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.AVG, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(150, dp.longValue());
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runAvgFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.AVG, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(38.125, dp.doubleValue(), 0.001);
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runAvgOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.AVG, false);
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
      if (v == 1) {
        v = 150;
      } else if (dp.timestamp() == 1357007400000L) {
        v = 1;
      } else if (v == 150) {
        v = 151;
      } else {
        v = 150;
      }
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runAvgFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.AVG, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    double v = 1.25;
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.doubleValue(), 0.0001);
      if (v == 1.25) {
        v = 38.1875;
      } else if (dp.timestamp() == 1357007400000L) {
        v = .25;
      }
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runDev() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.DEV, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long v = 149;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());
      
      if (decrement) {
        v--;
      } else {
        v++;
      }
      
      if (v < 0){
        v = 0;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runDevFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.DEV, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    double v = 36.875;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.001);
      
      if (decrement) {
        v -= 0.25;
      } else {
        v += 0.25;
      }
      
      if (v < 0.125){
        v = 0.125;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runDevOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.DEV, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long v = 0;
    long ts = 1356998430000L;
    int counter = 0;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.longValue());
      if (dp.timestamp() == 1356998430000L) {
        v = 149;
      } else if (dp.timestamp() == 1357007400000L) {
        v = 0;
      } else if (counter % 2 == 0) {
        if (decrement) {
          v--;
        } else {
          v++;
        }
        if (v < 0) {
          v = 0;
          decrement = false;
          counter++;
        }
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runDevFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.DEV, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    double v = 0;
    long ts = 1356998430000L;
    int counter = 0;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.doubleValue(), 0.0001);
      if (dp.timestamp() == 1356998430000L) {
        v = 36.8125;
      } else if (dp.timestamp() == 1357007400000L) {
        v = 0;
      } else {
        if (decrement) {
          v -= 0.125;
        } else {
          v += 0.125;
        }
        if (v < 0.0625) {
          v = 0.0625;
          decrement = false;
          counter++;
        }
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMimMin() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MIMMIN, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long v = 1;
    long ts = 1356998430000L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());
      
      if (decrement) {
        v--;
      } else {
        v++;
      }
      
      if (v == 151){
        v = 150;
        decrement = true;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMimMinOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MIMMIN, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long v1 = 1;
    long v2 = 300;
    long ts = 1356998430000L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      
      if (counter % 2 == 0) {        
        assertEquals(v1, dp.longValue());
        v1++;
      } else {
        assertEquals(v2, dp.longValue());
        v2--;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMimMinFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MIMMIN, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    double v = 1.25;
    long ts = 1356998430000L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.0001);
      
      if (decrement) {
        v -= .25;
      } else {
        v += .25;
      }
      
      if (v > 38){
        v = 38.0;
        decrement = true;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMimMinFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MIMMIN, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    double v1 = 1.25;
    double v2 = 75.0;
    long ts = 1356998430000L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      if (counter % 2 == 0) {
        assertEquals(v1, dp.doubleValue(), 0.001);
        v1 += 0.25;
      } else {
        assertEquals(v2, dp.doubleValue(), 0.001);
        v2 -= 0.25;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMimMax() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MIMMAX, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long v = 300;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());

      if (decrement) {
        v--;
      } else {
        v++;
      }

      if (v == 150){
        v = 151;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMimMaxFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MIMMAX, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    double v = 75.0;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.001);

      if (decrement) {
        v -= .25;
      } else {
        v += .25;
      }

      if (v < 38.25){
        v = 38.25;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMimMaxOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MIMMAX, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    long v1 = 1;
    long v2 = 300;
    long ts = 1356998430000L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      
      if (counter % 2 == 0) {        
        assertEquals(v1, dp.longValue());
        v1++;
      } else {
        assertEquals(v2, dp.longValue());
        v2--;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMimMaxFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.MIMMAX, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    double v1 = 1.25;
    double v2 = 75.0;
    long ts = 1356998430000L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      if (counter % 2 == 0) {
        assertEquals(v1, dp.doubleValue(), 0.001);
        v1 += 0.25;
      } else {
        assertEquals(v2, dp.doubleValue(), 0.001);
        v2 -= 0.25;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
 
  // ----------------- //
  // Helper functions. //
  // ----------------- //
  
  @SuppressWarnings("unchecked")
  private void setQueryStorage() throws Exception {
    storage = new MockBase(tsdb, client, true, true, true, true);
    storage.setFamily("t".getBytes(MockBase.ASCII()));

    PowerMockito.mockStatic(IncomingDataPoints.class);   
    PowerMockito.doAnswer(
        new Answer<Deferred<byte[]>>() {
          public Deferred<byte[]> answer(final InvocationOnMock args) 
            throws Exception {
            final String metric = (String)args.getArguments()[1];
            final Map<String, String> tags = 
              (Map<String, String>)args.getArguments()[2];
            
            if (metric.equals("sys.cpu.user")) {
              if (tags.get("host").equals("web01")) {
                return Deferred.fromResult(
                    new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1});
              } else {
                return Deferred.fromResult(
                    new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 2});
              }
            } else {
              if (tags.get("host").equals("web01")) {
                return Deferred.fromResult(
                    new byte[] { 0, 0, 2, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1});
              } else {
                return Deferred.fromResult(
                    new byte[] { 0, 0, 2, 0, 0, 0, 0, 0, 0, 1, 0, 0, 2});
              }
            }
          }
        }
    ).when(IncomingDataPoints.class, "rowKeyTemplate", (TSDB)any(), anyString(), 
        (Map<String, String>)any());
  }
  
  private void storeLongTimeSeriesSeconds(final boolean two_metrics, 
      final boolean offset) throws Exception {
    setQueryStorage();
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    for (int i = 1; i <= 300; i++) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
      if (two_metrics) {
        tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
      }
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = offset ? 1356998415 : 1356998400;
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
  
  private void storeMixedTimeSeriesSeconds() throws Exception {
    setQueryStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      if (i % 2 == 0) {
        tsdb.addPoint("sys.cpu.user", timestamp += 30, (long)i, tags)
          .joinUninterruptibly();
      } else {
        tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags)
          .joinUninterruptibly();
      }
    }
  }
  
  // dumps ints, floats, seconds and ms
  private void storeMixedTimeSeriesMsAndS() throws Exception {
    setQueryStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400000L;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      long ts = timestamp += 500;
      if (ts % 1000 == 0) {
        ts /= 1000;
      }
      if (i % 2 == 0) {
        tsdb.addPoint("sys.cpu.user", ts, (long)i, tags).joinUninterruptibly();
      } else {
        tsdb.addPoint("sys.cpu.user", ts, i, tags).joinUninterruptibly();
      }
    }
  }
  
  private void storeLongCompactions() throws Exception {
    setQueryStorage();
    long base_timestamp = 1356998400;
    long value = 1;
    byte[] qualifier = new byte[119 * 2];
    long timestamp = 1356998430;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column = 
        Bytes.fromShort((short)(offset << Const.FLAG_BITS | 0x7));
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
    }
    
    byte[] column_qualifier = new byte[119 * 8];
    for (int index = 0; index < column_qualifier.length; index += 8) {
      System.arraycopy(Bytes.fromLong(value), 0, column_qualifier, index, 8);
      value++;
    }
    storage.addColumn(MockBase.stringToBytes("00000150E22700000001000001"), 
        qualifier, column_qualifier);
    
    base_timestamp = 1357002000;
    qualifier = new byte[120 * 2];
    timestamp = 1357002000;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column = 
        Bytes.fromShort((short)(offset << Const.FLAG_BITS | 0x7));
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
    }
    
    column_qualifier = new byte[120 * 8];
    for (int index = 0; index < column_qualifier.length; index += 8) {
      System.arraycopy(Bytes.fromLong(value), 0, column_qualifier, index, 8);
      value++;
    }
    storage.addColumn(MockBase.stringToBytes("00000150E23510000001000001"), 
        qualifier, column_qualifier);
    
    base_timestamp = 1357005600;
    qualifier = new byte[61 * 2];
    timestamp = 1357005600;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column = 
        Bytes.fromShort((short)(offset << Const.FLAG_BITS | 0x7));
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
    }
    
    column_qualifier = new byte[61 * 8];
    for (int index = 0; index < column_qualifier.length; index += 8) {
      System.arraycopy(Bytes.fromLong(value), 0, column_qualifier, index, 8);
      value++;
    }
    storage.addColumn(MockBase.stringToBytes("00000150E24320000001000001"), 
        qualifier, column_qualifier);
  }
  
  private void storeFloatCompactions() throws Exception {
    setQueryStorage();
    long base_timestamp = 1356998400;
    float value = 1.25F;
    byte[] qualifier = new byte[119 * 2];
    long timestamp = 1356998430;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column = 
        Bytes.fromShort((short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
    }
    
    byte[] column_qualifier = new byte[119 * 4];
    for (int index = 0; index < column_qualifier.length; index += 4) {
      System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0, 
          column_qualifier, index, 4);
      value += 0.25F;
    }
    storage.addColumn(MockBase.stringToBytes("00000150E22700000001000001"), 
        qualifier, column_qualifier);
    
    base_timestamp = 1357002000;
    qualifier = new byte[120 * 2];
    timestamp = 1357002000;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column = 
        Bytes.fromShort((short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
    }
    
    column_qualifier = new byte[120 * 4];
    for (int index = 0; index < column_qualifier.length; index += 4) {
      System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0, 
          column_qualifier, index, 4);
      value += 0.25F;
    }
    storage.addColumn(MockBase.stringToBytes("00000150E23510000001000001"), 
        qualifier, column_qualifier);
    
    base_timestamp = 1357005600;
    qualifier = new byte[61 * 2];
    timestamp = 1357005600;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column = 
        Bytes.fromShort((short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
    }
    
    column_qualifier = new byte[61 * 4];
    for (int index = 0; index < column_qualifier.length; index += 4) {
      System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0, 
          column_qualifier, index, 4);
      value += 0.25F;
    }
    storage.addColumn(MockBase.stringToBytes("00000150E24320000001000001"), 
        qualifier, column_qualifier);
  }
  
  private void storeMixedCompactions() throws Exception {
    setQueryStorage();
    long base_timestamp = 1356998400;
    float q_counter = 1.25F;
    byte[] qualifier = new byte[119 * 2];
    long timestamp = 1356998430;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column;
      if (q_counter % 1 == 0) {
        column = Bytes.fromShort((short)(offset << Const.FLAG_BITS | 0x7));
      } else {
        column = Bytes.fromShort(
            (short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
      }
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
      q_counter += 0.25F;
    }
    
    float value = 1.25F;
    int num = 119;
    byte[] column_qualifier = new byte[((num / 4) * 8) + ((num - (num / 4)) * 4)];
    int idx = 0;
    while (idx < column_qualifier.length) {
      if (value % 1 == 0) {
        System.arraycopy(Bytes.fromLong((long)value), 0, column_qualifier, idx, 8);
        idx += 8;
      } else {
        System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0, 
            column_qualifier, idx, 4);
        idx += 4;
      }
      value += 0.25F;
    }
    storage.addColumn(MockBase.stringToBytes("00000150E22700000001000001"), 
        qualifier, column_qualifier);
    
    base_timestamp = 1357002000;
    qualifier = new byte[120 * 2];
    timestamp = 1357002000;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column;
      if (q_counter % 1 == 0) {
        column = Bytes.fromShort((short)(offset << Const.FLAG_BITS | 0x7));
      } else {
         column = Bytes.fromShort(
             (short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
      }
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
      q_counter += 0.25F;
    }
    
    num = 120;
    column_qualifier = new byte[((num / 4) * 8) + ((num - (num / 4)) * 4)];
    idx = 0;
    while (idx < column_qualifier.length) {
      if (value % 1 == 0) {
        System.arraycopy(Bytes.fromLong((long)value), 0, column_qualifier, idx, 8);
        idx += 8;
      } else {
        System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0, 
            column_qualifier, idx, 4);
        idx += 4;
      }
      value += 0.25F;
    }
    storage.addColumn(MockBase.stringToBytes("00000150E23510000001000001"), 
        qualifier, column_qualifier);
    
    base_timestamp = 1357005600;
    qualifier = new byte[61 * 2];
    timestamp = 1357005600;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column;
      if (q_counter % 1 == 0) {
        column = Bytes.fromShort((short)(offset << Const.FLAG_BITS | 0x7));
      } else {
         column = Bytes.fromShort(
             (short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
      }
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
      q_counter += 0.25F;
    }
    
    num = 61;
    column_qualifier = 
      new byte[(((num / 4) + 1) * 8) + ((num - ((num / 4) + 1)) * 4)];
    idx = 0;
    while (idx < column_qualifier.length) {
      if (value % 1 == 0) {
        System.arraycopy(Bytes.fromLong((long)value), 0, column_qualifier, idx, 8);
        idx += 8;
      } else {
        System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0, 
            column_qualifier, idx, 4);
        idx += 4;
      }
      value += 0.25F;
    }
    storage.addColumn(MockBase.stringToBytes("00000150E24320000001000001"), 
        qualifier, column_qualifier);
  }
}
