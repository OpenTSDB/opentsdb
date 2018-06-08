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
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.storage.TimeSeriesDataStore;

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
      .setType(NumericType.TYPE.toString())
      .build();
    
    summary_config = 
          (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
      .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setDefaultRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
      .addExpectedSummary(0)
      .setType(NumericSummaryType.TYPE.toString())
      .build();
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
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
    assertEquals(2, gbr.groups.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = (GroupByTimeSeries) gbr.groups.get(1016930065491533874L);
    assertEquals(2, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    assertTrue(ts.sources.contains(ts2));
    
    ts = (GroupByTimeSeries) gbr.groups.get(2354124408228422003L);
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
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(4, gbr.groups.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = (GroupByTimeSeries) 
        gbr.groups.get(9128762587020020135L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts2));
    
    ts = (GroupByTimeSeries) gbr.groups.get(-5731302817531122843L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    
    ts = (GroupByTimeSeries) gbr.groups.get(-5470391479372287920L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts3));
    
    ts = (GroupByTimeSeries) gbr.groups.get(-4390926326477623864L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts4));
  }
  
  @Test
  public void stringsNoSuchTag() throws Exception {
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .addTagKey("foo")
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(0, gbr.groups.size());
  }

  @Test
  public void stringsGroupAll() throws Exception {
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setGroupAll(true)
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(1, gbr.groups.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = (GroupByTimeSeries) gbr.groups.get(-1939960532314980458L);
    assertEquals(4, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    assertTrue(ts.sources.contains(ts2));
    assertTrue(ts.sources.contains(ts3));
    assertTrue(ts.sources.contains(ts4));
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setGroupAll(true)
        .addTagKey("host") // <-- ignored
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    when(node.config()).thenReturn(config);
    
    gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(1, gbr.groups.size());
    
    // xx hash is deterministic
    ts = (GroupByTimeSeries) gbr.groups.get(-1939960532314980458L);
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
    assertEquals(0, gbr.groups.size());
  }
  
  @Test
  public void bytes1Tag() throws Exception {
    setupBytes();
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(2, gbr.groups.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = (GroupByTimeSeries) gbr.groups.get(4725406361284816093L);
    assertEquals(2, ts.sources().size());
    assertTrue(ts.sources.contains(ts3));
    assertTrue(ts.sources.contains(ts4));
    
    ts = (GroupByTimeSeries) gbr.groups.get(-2414897214160805570L);
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
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(4, gbr.groups.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = (GroupByTimeSeries) 
        gbr.groups.get(-8373939890777177670L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts3));
    
    ts = (GroupByTimeSeries) gbr.groups.get(5269015763610144674L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    
    ts = (GroupByTimeSeries) gbr.groups.get(-4495118893622513658L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts4));
    
    ts = (GroupByTimeSeries) gbr.groups.get(121883220133700188L);
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts2));
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
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(0, gbr.groups.size());
  }
  
  @Test
  public void bytesGroupAll() throws Exception {
    setupBytes();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setGroupAll(true)
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    when(node.config()).thenReturn(config);
    
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(1, gbr.groups.size());
    
    // xx hash is deterministic
    GroupByTimeSeries ts = (GroupByTimeSeries) gbr.groups.get(-3292477735350538661L);
    assertEquals(4, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    assertTrue(ts.sources.contains(ts2));
    assertTrue(ts.sources.contains(ts3));
    assertTrue(ts.sources.contains(ts4));
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setGroupAll(true)
        .addTagKey("host") // <-- ignored
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    when(node.config()).thenReturn(config);
    
    gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
    assertEquals(1, gbr.groups.size());
    
    // xx hash is deterministic
    ts = (GroupByTimeSeries) gbr.groups.get(-3292477735350538661L);
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
    assertEquals(0, gbr.groups.size());
  }
  
  private void setupBytes() {
    final TimeSeriesDataStore data_store = mock(TimeSeriesDataStore.class);
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .addTagKey("dc".getBytes())
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
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
