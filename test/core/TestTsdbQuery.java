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
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

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
  RowKey.class, Span.class, SpanGroup.class})
public final class TestTsdbQuery {
  private Config config;
  private TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueId metrics = mock(UniqueId.class);
  private UniqueId tag_names = mock(UniqueId.class);
  private UniqueId tag_values = mock(UniqueId.class);
  private TsdbQuery query = null;
  private MockBase storage;

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
    when(tag_names.getName(new byte[] { 0, 0, 1 })).thenReturn("host");
    when(tag_names.getOrCreateId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getId("dc")).thenThrow(new NoSuchUniqueName("dc", "metric"));
    when(tag_values.getId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getName(new byte[] { 0, 0, 1 })).thenReturn("web01");
    when(tag_values.getOrCreateId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getName(new byte[] { 0, 0, 2 })).thenReturn("web02");
    when(tag_values.getOrCreateId("web02")).thenReturn(new byte[] { 0, 0, 2 });
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
  public void setStartTimeInvalidNegative() throws Exception {
    query.setStartTime(-1L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeInvalidTooBig() throws Exception {
    query.setStartTime(4294967296L);
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
    query.setEndTime(4294967296L);
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
    assertEquals(1357300800L, query.getEndTime());
  }
  
  @Test
  public void setTimeSeries() throws Exception {
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
    storeLongTimeSeries();
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
    storeLongTimeSeries();
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
      assertEquals(301, dp.longValue());
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runLongTwoGroup() throws Exception {
    storeLongTimeSeries();
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
    storeLongTimeSeries();
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
  public void runLongSingleTSDownsample() throws Exception {
    storeLongTimeSeries();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60, Aggregators.AVG);
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
    storeLongTimeSeries();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60, Aggregators.AVG);
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
    storeFloatTimeSeries();
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
    storeFloatTimeSeries();
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
    storeFloatTimeSeries();
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
    storeFloatTimeSeries();
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
  public void runFloatSingleTSDownsample() throws Exception {
    storeFloatTimeSeries();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60, Aggregators.AVG);
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
    storeFloatTimeSeries();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60, Aggregators.AVG);
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
    storeMixedTimeSeries();
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
    storeMixedTimeSeries();
    
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
    storeLongTimeSeries();
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
    storeLongTimeSeries();
    
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
    storeLongTimeSeries();
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
    storeLongTimeSeries();
    
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
    storeLongTimeSeries();
    
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
    storeLongTimeSeries();
    
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
    storeLongTimeSeries();
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
    storeLongTimeSeries();
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
    storeLongTimeSeries();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add("000001000001000001");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    dps[0].metricName();
  }
  
  // TODO - other UTs
  // - fix floating points (CompactionQueue:L267
  
  // ----------------- //
  // Helper functions. //
  // ----------------- //
  
  private void setQueryStorage() throws Exception {
    storage = new MockBase(tsdb, client, true, true, true, true);
    storage.setFamily("t".getBytes(MockBase.ASCII()));
  }
  
  private void storeLongTimeSeries() throws Exception {
    setQueryStorage();
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    for (int i = 1; i <= 300; i++) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
      tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998400;
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
      tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
    }
  }

  private void storeFloatTimeSeries() throws Exception {
    setQueryStorage();
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
      tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998400;
    for (float i = 75F; i > 0; i -= 0.25F) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
      tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
    }
  }
  
  private void storeMixedTimeSeries() throws Exception {
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
      System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0, column_qualifier, index, 4);
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
      System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0, column_qualifier, index, 4);
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
      System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0, column_qualifier, index, 4);
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
        column = Bytes.fromShort((short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
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
        System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0, column_qualifier, idx, 4);
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
         column = Bytes.fromShort((short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
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
        System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0, column_qualifier, idx, 4);
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
         column = Bytes.fromShort((short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
      }
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
      q_counter += 0.25F;
    }
    
    num = 61;
    column_qualifier = new byte[(((num / 4) + 1) * 8) + ((num - ((num / 4) + 1)) * 4)];
    idx = 0;
    while (idx < column_qualifier.length) {
      if (value % 1 == 0) {
        System.arraycopy(Bytes.fromLong((long)value), 0, column_qualifier, idx, 8);
        idx += 8;
      } else {
        System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0, column_qualifier, idx, 4);
        idx += 4;
      }
      value += 0.25F;
    }
    storage.addColumn(MockBase.stringToBytes("00000150E24320000001000001"), 
        qualifier, column_qualifier);
  }
}
