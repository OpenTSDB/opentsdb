// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.processor.groupby;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesByteId;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestGroupByResult {
  
  private GroupBy node;
  private GroupByConfig config;
  private QueryResult result;
  private TimeSpecification time_spec;
  private NumericMillisecondShard ts1;
  private NumericMillisecondShard ts2;
  private NumericMillisecondShard ts3;
  private NumericMillisecondShard ts4;
  private NumericInterpolatorConfig numeric_config;
  private NumericSummaryInterpolatorConfig summary_config;
  
  @Before
  public void before() throws Exception {
    numeric_config = 
          (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    summary_config = 
          (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
      .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setDefaultRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
      .addExpectedSummary(0)
      .setDataType(NumericSummaryType.TYPE.toString())
      .build();
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setMergeIds(true)
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("Testing")
        .build();
    node = mock(GroupBy.class);
    result = mock(QueryResult.class);
    time_spec = mock(TimeSpecification.class);
    
    when(node.config()).thenReturn(config);
    
    ts1 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .addTags("host", "web01")
        .addTags("dc", "lga")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts1.add(1000, 1);
    ts1.add(3000, 5);
    
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .addTags("host", "web02")
        .addTags("dc", "lga")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts2.add(1000, 4);
    ts2.add(3000, 10);
    
    ts3 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .addTags("host", "web01")
        .addTags("dc", "phx")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts3.add(1000, 0);
    ts3.add(3000, 7);
    
    ts4 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .addTags("host", "web02")
        .addTags("dc", "phx")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts4.add(1000, 0);
    ts4.add(3000, 7);
    
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2, ts3, ts4));
    when(result.sequenceId()).thenReturn(42l);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(result.idType()).thenReturn((TypeToken) Const.TS_STRING_ID);
  }
  
  @Test
  public void strings1Tag() throws Exception {
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(2, gbr.results.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = getSeries(gbr, -7536611335599119437L);
    TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    assertEquals("a", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("lga", id.tags().get("dc"));
    assertEquals(1, id.aggregatedTags().size());
    assertTrue(id.aggregatedTags().contains("host"));
    assertEquals(2, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    assertTrue(ts.sources.contains(ts2));
    
    ts = getSeries(gbr, 9058951729670804382L);
    id = (TimeSeriesStringId) ts.id();
    assertEquals("a", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("phx", id.tags().get("dc"));
    assertEquals(1, id.aggregatedTags().size());
    assertTrue(id.aggregatedTags().contains("host"));
    assertEquals(2, ts.sources().size());
    assertTrue(ts.sources.contains(ts3));
    assertTrue(ts.sources.contains(ts4));
  }

  @Test
  public void strings2Tags() throws Exception {
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .addTagKey("host")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(4, gbr.results.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = getSeries(gbr, 8854926265991506377L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    
    ts = getSeries(gbr, 2209259780447305455L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts3));
    
    ts = getSeries(gbr, -665964879746510592L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts2));
    
    ts = getSeries(gbr, -9040751192873861559L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts4));
  }
  
  @Test
  public void stringsNoSuchTag() throws Exception {
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .addTagKey("foo")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(0, gbr.results.size());
  }

  @Test
  public void stringsGroupAll() throws Exception {
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(1, gbr.results.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = getSeries(gbr, -6979747124570991354L);
    assertEquals(4, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    assertTrue(ts.sources.contains(ts2));
    assertTrue(ts.sources.contains(ts3));
    assertTrue(ts.sources.contains(ts4));
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(1, gbr.results.size());
    
    // xx hash is deterministic
    ts = getSeries(gbr, -6979747124570991354L);
    assertEquals(4, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    assertTrue(ts.sources.contains(ts2));
    assertTrue(ts.sources.contains(ts3));
    assertTrue(ts.sources.contains(ts4));
  }
  
  @Test
  public void stringsEmptyResults() throws Exception {
    when(result.timeSeries()).thenReturn(Lists.newArrayList());
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(0, gbr.results.size());
  }
  
  @Test
  public void stringsNoMerging() throws Exception {
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(2, gbr.results.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = getSeries(gbr, 5912253463122939075L);
    TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    assertEquals("a", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("lga", id.tags().get("dc"));
    assertTrue(id.aggregatedTags().isEmpty());
    
    ts = getSeries(gbr, 8554150728605419091L);
    assertEquals(2, ts.sources().size());
    id = (TimeSeriesStringId) ts.id();
    assertEquals("a", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("phx", id.tags().get("dc"));
    assertTrue(id.aggregatedTags().isEmpty());
  }
  
  @Test
  public void stringsNoMergingGroupAll() throws Exception {
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(1, gbr.results.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = (GroupByTimeSeries) gbr.results.get(0);
    TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    assertEquals("a", id.metric());
    assertTrue(id.tags().isEmpty());
    assertTrue(id.aggregatedTags().isEmpty());
  }
  
  @Test
  public void bytes1Tag() throws Exception {
    setupBytes();
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(2, gbr.results.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = getSeries(gbr, -5615300353302416798L);
    TimeSeriesByteId id = (TimeSeriesByteId) ts.id();
    assertArrayEquals("a".getBytes(), id.metric());
    assertEquals(1, id.tags().size());
    assertArrayEquals("phx".getBytes(), id.tags().get("dc".getBytes()));
    assertEquals(1, id.aggregatedTags().size());
    assertArrayEquals("host".getBytes(), id.aggregatedTags().get(0));
    assertEquals(2, ts.sources().size());
    assertTrue(ts.sources.contains(ts3));
    assertTrue(ts.sources.contains(ts4));
    
    ts = getSeries(gbr, -1137681723123188454L);
    id = (TimeSeriesByteId) ts.id();
    assertArrayEquals("a".getBytes(), id.metric());
    assertEquals(1, id.tags().size());
    assertArrayEquals("lga".getBytes(), id.tags().get("dc".getBytes()));
    assertEquals(1, id.aggregatedTags().size());
    assertArrayEquals("host".getBytes(), id.aggregatedTags().get(0));
    assertEquals(2, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    assertTrue(ts.sources.contains(ts2));
  }
  
  @Test
  public void bytes2Tags() throws Exception {
    setupBytes();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .addTagKey("host")
        .addTagKey("dc".getBytes())
        .addTagKey("host".getBytes())
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(4, gbr.results.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = getSeries(gbr, 8606452997040963279L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts2));
    
    ts = getSeries(gbr, 3728368451019728235L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts3));
    
    ts = getSeries(gbr, -5921137706139174957L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts4));
    
    ts = getSeries(gbr, -4228542243250309079L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
  }
  
  @Test
  public void bytesNoSuchTag() throws Exception {
    setupBytes();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .addTagKey("foo")
        .addTagKey("dc".getBytes())
        .addTagKey("foo".getBytes())
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(0, gbr.results.size());
  }
  
  @Test
  public void bytesGroupAll() throws Exception {
    setupBytes();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(1, gbr.results.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = getSeries(gbr, -3292477735350538661L);
    assertEquals(4, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    assertTrue(ts.sources.contains(ts2));
    assertTrue(ts.sources.contains(ts3));
    assertTrue(ts.sources.contains(ts4));
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(1, gbr.results.size());
    
    // xx hash is deterministic
    ts = getSeries(gbr, -3292477735350538661L);
    assertEquals(4, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    assertTrue(ts.sources.contains(ts2));
    assertTrue(ts.sources.contains(ts3));
    assertTrue(ts.sources.contains(ts4));
  }
  
  @Test
  public void bytesEmptyResults() throws Exception {
    setupBytes();
    when(result.timeSeries()).thenReturn(Lists.newArrayList());
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(0, gbr.results.size());
  }
  
  @Test
  public void bytesNoMergeIds() throws Exception {
    setupBytes();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .addTagKey("dc".getBytes())
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(2, gbr.results.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = getSeries(gbr, 5980207842827758726L);
    TimeSeriesByteId id = (TimeSeriesByteId) ts.id();
    assertArrayEquals("a".getBytes(), id.metric());
    assertEquals(1, id.tags().size());
    assertArrayEquals("phx".getBytes(), id.tags().get("dc".getBytes()));
    assertTrue(id.aggregatedTags().isEmpty());
    
    ts = getSeries(gbr, 1162320332169197607L);
    id = (TimeSeriesByteId) ts.id();
    assertArrayEquals("a".getBytes(), id.metric());
    assertEquals(1, id.tags().size());
    assertArrayEquals("lga".getBytes(), id.tags().get("dc".getBytes()));
    assertTrue(id.aggregatedTags().isEmpty());
  }
  
  @Test
  public void bytesNoMergeIdsGroupAll() throws Exception {
    setupBytes();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(1, gbr.results.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = getSeries(gbr, -3292477735350538661L);
    TimeSeriesByteId id = (TimeSeriesByteId) ts.id();
    assertArrayEquals("a".getBytes(), id.metric());
    assertTrue(id.tags().isEmpty());
    assertTrue(id.aggregatedTags().isEmpty());
    
  }
  
  private GroupByTimeSeries getSeries(final GroupByResult results, 
                                      final long hash) {
    for (final TimeSeries result : results.results) {
      if (result.id().buildHashCode() == hash) {
        return (GroupByTimeSeries) result;
      }
    }
    return null;
  }
  
  private void setupBytes() {
    final TimeSeriesDataSourceFactory data_store = mock(TimeSeriesDataSourceFactory.class);
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .addTagKey("dc".getBytes())
        .setMergeIds(true)
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    ts1 = new NumericMillisecondShard(
        BaseTimeSeriesByteId.newBuilder(data_store)
        .setMetric("a".getBytes())
        .addTags("host".getBytes(), "web01".getBytes())
        .addTags("dc".getBytes(), "lga".getBytes())
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts1.add(1000, 1);
    ts1.add(3000, 5);
    
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesByteId.newBuilder(data_store)
        .setMetric("a".getBytes())
        .addTags("host".getBytes(), "web02".getBytes())
        .addTags("dc".getBytes(), "lga".getBytes())
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts2.add(1000, 4);
    ts2.add(3000, 10);
    
    ts3 = new NumericMillisecondShard(
        BaseTimeSeriesByteId.newBuilder(data_store)
        .setMetric("a".getBytes())
        .addTags("host".getBytes(), "web01".getBytes())
        .addTags("dc".getBytes(), "phx".getBytes())
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts3.add(1000, 0);
    ts3.add(3000, 7);
    
    ts4 = new NumericMillisecondShard(
        BaseTimeSeriesByteId.newBuilder(data_store)
        .setMetric("a".getBytes())
        .addTags("host".getBytes(), "web02".getBytes())
        .addTags("dc".getBytes(), "phx".getBytes())
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts4.add(1000, 0);
    ts4.add(3000, 7);
    
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2, ts3, ts4));
    when(result.idType()).thenReturn((TypeToken) Const.TS_BYTE_ID);
  }
}
