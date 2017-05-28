// This file is part of OpenTSDB.
// Copyright (C) 2016-2017  The OpenTSDB Authors.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import net.opentsdb.meta.Annotation;

public class TestTsdbQueryHistogramQueries extends BaseTsdbTest {
  private TsdbQuery query = null;

  @Before
  public void beforeLocal() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(2);
    PowerMockito.when(Const.MAX_NUM_TAGS()).thenReturn((short) 8);
    query = new TsdbQuery(tsdb);
  }

  @Test
  public void runSingleTsMsSinglePercentile() throws Exception {
    Whitebox.setInternalState(config, "hist_decoder_name", 
        "net.opentsdb.core.LongHistogramDataPointForTestDecoder");
    
    this.storeTestHistogramTimeSeriesMs();
    storage.dumpToSystemOut();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("msg.end2end.latency", tags, Aggregators.SUM, false);
    
    List<Float> percentiles = new ArrayList<Float>();
    float per_98 = 0.98F;
    percentiles.add(per_98);
    
    query.setPercentiles(percentiles);

    final DataPoints[] dps = query.runHistogram();

    assertNotNull(dps);
    assertTrue(dps[0].isPercentile());
    assertEquals("msg.end2end.latency_pct_0.98", dps[0].metricName());
    
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value * 0.98, dp.doubleValue(), 0.0001);
      value++;
    }
    assertEquals(300, dps[0].aggregatedSize());
  } // end runSingleTsMsSinglePercentile()

  @Test
  public void runSingleTsMsDoulePercentile() throws Exception {
    Whitebox.setInternalState(config, "hist_decoder_name", 
        "net.opentsdb.core.LongHistogramDataPointForTestDecoder");
    
    this.storeTestHistogramTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("msg.end2end.latency", tags, Aggregators.SUM, false);
    
    List<Float> percentiles = new ArrayList<Float>();
    float per_98 = 0.98F;
    percentiles.add(per_98);
    float per_95 = 0.95F;
    percentiles.add(per_95);
    
    query.setPercentiles(percentiles);

    final DataPoints[] dps = query.runHistogram();

    assertNotNull(dps);
    assertTrue(dps[0].isPercentile());
    assertTrue(dps[1].isPercentile());
    
    assertEquals("msg.end2end.latency_pct_0.98", dps[0].metricName());
    assertEquals("msg.end2end.latency_pct_0.95", dps[1].metricName());
    
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    assertTrue(dps[1].getAggregatedTags().isEmpty());
    assertNull(dps[1].getAnnotations());
    assertEquals("web01", dps[1].getTags().get("host"));

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value * 0.98, dp.doubleValue(), 0.0001);
      value++;
    }
    assertEquals(300, dps[0].aggregatedSize());
    
    int value_95 = 1;
    for (DataPoint dp : dps[1]) {
      assertEquals(value_95 * 0.95, dp.doubleValue(), 0.0001);
      value_95++;
    }
    assertEquals(300, dps[1].aggregatedSize());
  } // end runSingleTsMsSinglePercentile()
  
  @Test
  public void runSingleTsMsTwoAggSum() throws Exception {
    Whitebox.setInternalState(config, "hist_decoder_name", 
        "net.opentsdb.core.LongHistogramDataPointForTestDecoder");
    this.storeTestHistogramTimeSeriesMs();
    
    HashMap<String, String> tags = new HashMap<String, String>();
    
    query.setStartTime(1356998400L);
    query.setEndTime(1357041600L);
    query.setTimeSeries("msg.end2end.latency", tags, Aggregators.SUM, false);
    
    List<Float> percentiles = new ArrayList<Float>();
    float per_98 = 0.98F;
    percentiles.add(per_98);
    query.setPercentiles(percentiles);
    
    final DataPoints[] dps = query.runHistogram();
    
    assertNotNull(dps);
    assertTrue(dps[0].isPercentile());
    
    assertEquals("msg.end2end.latency_pct_0.98", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    for (DataPoint dp : dps[0]) {
      assertEquals(301 * 0.98, dp.doubleValue(), 0.0001);
    }
    assertEquals(300, dps[0].size());
  } // end runSingleTsMsTwoAggSum()
  
  @Test
  public void runSingleTsMsAggNone() throws Exception {
    Whitebox.setInternalState(config, "hist_decoder_name", 
        "net.opentsdb.core.LongHistogramDataPointForTestDecoder");
    this.storeTestHistogramTimeSeriesMs();
    
    HashMap<String, String> tags = new HashMap<String, String>();
    
    query.setStartTime(1356998400L);
    query.setEndTime(1357041600L);
    query.setTimeSeries("msg.end2end.latency", tags, Aggregators.NONE, false);
    
    List<Float> percentiles = new ArrayList<Float>();
    float per_98 = 0.98F;
    percentiles.add(per_98);
    query.setPercentiles(percentiles);
    
    final DataPoints[] dps = query.runHistogram();
    
    assertNotNull(dps);
    assertEquals(2, dps.length);
    
    assertTrue(dps[0].isPercentile());
    assertTrue(dps[1].isPercentile());
    
    assertEquals("msg.end2end.latency_pct_0.98", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(1, dps[0].getTags().size());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    assertEquals("msg.end2end.latency_pct_0.98", dps[1].metricName());
    assertTrue(dps[1].getAggregatedTags().isEmpty());
    assertNull(dps[1].getAnnotations());
    assertEquals(1, dps[1].getTags().size());
    assertEquals("web02", dps[1].getTags().get("host"));
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value * 0.98, dp.doubleValue(), 0.0001);
      ++value;
    }
    assertEquals(300, dps[0].size());
    
    int value_other = 300;
    for (DataPoint dp : dps[1]) {
      assertEquals(value_other * 0.98, dp.doubleValue(), 0.0001);
      --value_other;
    }
    assertEquals(300, dps[1].size());
  } // end runSingleTsMsTwoAggSum()
  
  @Test
  public void runSingleTsMsAggSumTwoGroups() throws Exception {
    Whitebox.setInternalState(config, "hist_decoder_name", 
        "net.opentsdb.core.LongHistogramDataPointForTestDecoder");
    this.storeTestHistogramTimeSeriesMs();
    
    HashMap<String, String> tags = new HashMap<String, String>();
    tags.put("host", "*");
    
    query.setStartTime(1356998400L);
    query.setEndTime(1357041600L);
    query.setTimeSeries("msg.end2end.latency", tags, Aggregators.SUM, false);
    
    List<Float> percentiles = new ArrayList<Float>();
    float per_98 = 0.98F;
    percentiles.add(per_98);
    query.setPercentiles(percentiles);
    
    final DataPoints[] dps = query.runHistogram();
    
    assertNotNull(dps);
    assertEquals(2, dps.length);
    
    assertTrue(dps[0].isPercentile());
    assertTrue(dps[1].isPercentile());
    
    assertEquals("msg.end2end.latency_pct_0.98", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(1, dps[0].getTags().size());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    assertEquals("msg.end2end.latency_pct_0.98", dps[1].metricName());
    assertTrue(dps[1].getAggregatedTags().isEmpty());
    assertNull(dps[1].getAnnotations());
    assertEquals(1, dps[1].getTags().size());
    assertEquals("web02", dps[1].getTags().get("host"));
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value * 0.98, dp.doubleValue(), 0.0001);
      ++value;
    }
    assertEquals(300, dps[0].size());
    
    int value_other = 300;
    for (DataPoint dp : dps[1]) {
      assertEquals(value_other * 0.98, dp.doubleValue(), 0.0001);
      --value_other;
    }
    assertEquals(300, dps[1].size());
  } // end runSingleTsMsTwoAggSum()
  
  @Test
  public void runWithAnnotation() throws Exception {
    Whitebox.setInternalState(config, "hist_decoder_name", 
        "net.opentsdb.core.LongHistogramDataPointForTestDecoder");
    this.storeTestHistogramTimeSeriesSeconds(false);
    
    final Annotation note = new Annotation();
    note.setTSUID(getTSUIDString(HISTOGRAM_METRIC_STRING, TAGK_STRING, TAGV_STRING));
    note.setStartTime(1356998490);
    note.setDescription("Hello World!");
    note.syncToStorage(tsdb, false).joinUninterruptibly();
    
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("msg.end2end.latency", tags, Aggregators.SUM, false);

    List<Float> percentiles = new ArrayList<Float>();
    float per_98 = 0.98F;
    percentiles.add(per_98);
    query.setPercentiles(percentiles);
    
    final DataPoints[] dps = query.runHistogram();
    
    assertNotNull(dps);
    assertEquals(1, dps[0].getAnnotations().size());
    assertEquals("Hello World!", dps[0].getAnnotations().get(0).getDescription());
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value * 0.98, dp.doubleValue(), 0.0001);
      value++;
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runWithOnlyAnnotation() throws Exception {
    Whitebox.setInternalState(config, "hist_decoder_name", 
        "net.opentsdb.core.LongHistogramDataPointForTestDecoder");
    this.storeTestHistogramTimeSeriesSeconds(false);
    
    byte[] key = getRowKey(HISTOGRAM_METRIC_STRING, 1357002000, TAGK_STRING, TAGV_STRING);
    storage.flushRow(key);
    final Annotation note = new Annotation();
    note.setTSUID(getTSUIDString(HISTOGRAM_METRIC_STRING, TAGK_STRING, TAGV_STRING));
    note.setStartTime(1357002090);
    note.setDescription("Hello World!");
    note.syncToStorage(tsdb, false).joinUninterruptibly();
    
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("msg.end2end.latency", tags, Aggregators.SUM, false);

    List<Float> percentiles = new ArrayList<Float>();
    float per_98 = 0.98F;
    percentiles.add(per_98);
    query.setPercentiles(percentiles);
    
    final DataPoints[] dps = query.runHistogram();
    
    assertNotNull(dps);
    assertEquals(1, dps[0].getAnnotations().size());
    assertEquals("Hello World!", dps[0].getAnnotations().get(0).getDescription());
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value * 0.98, dp.doubleValue(), 0.0001);
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
    Whitebox.setInternalState(config, "hist_decoder_name", 
        "net.opentsdb.core.LongHistogramDataPointForTestDecoder");
    this.storeTestHistogramTimeSeriesSeconds(false);
    
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add(getTSUIDString(HISTOGRAM_METRIC_STRING, TAGK_STRING, TAGV_STRING));
    
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
    List<Float> percentiles = new ArrayList<Float>();
    float per_98 = 0.98F;
    percentiles.add(per_98);
    query.setPercentiles(percentiles);
    
    final DataPoints[] dps = query.runHistogram();
    
    assertNotNull(dps);
    assertEquals("msg.end2end.latency_pct_0.98", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value * 0.98, dp.doubleValue(), 0.0001);
      value++;
    }
    assertEquals(300, dps[0].aggregatedSize());
  }
  
  @Test
  public void runTSUIDsAggSum() throws Exception {
    Whitebox.setInternalState(config, "hist_decoder_name", 
        "net.opentsdb.core.LongHistogramDataPointForTestDecoder");
    this.storeTestHistogramTimeSeriesSeconds(false);
    
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add(getTSUIDString(HISTOGRAM_METRIC_STRING, TAGK_STRING, TAGV_STRING));
    tsuids.add(getTSUIDString(HISTOGRAM_METRIC_STRING, TAGK_STRING, TAGV_B_STRING));
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
   
    List<Float> percentiles = new ArrayList<Float>();
    float per_98 = 0.98F;
    percentiles.add(per_98);
    query.setPercentiles(percentiles);
    
    final DataPoints[] dps = query.runHistogram();
    
    assertNotNull(dps);
    assertEquals("msg.end2end.latency_pct_0.98", dps[0].metricName());
    assertEquals("host", dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());
    
    for (DataPoint dp : dps[0]) {
      assertEquals(301 * 0.98, dp.doubleValue(), 0.0001);
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runTSUIDQueryNoData() throws Exception {
    setDataPointStorage();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add(getTSUIDString(HISTOGRAM_METRIC_STRING, TAGK_STRING, TAGV_STRING));
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
    
    List<Float> percentiles = new ArrayList<Float>();
    float per_98 = 0.98F;
    percentiles.add(per_98);
    query.setPercentiles(percentiles);
    
    final DataPoints[] dps = query.runHistogram();
   
    assertNotNull(dps);
    assertEquals(0, dps.length);
  }
}
