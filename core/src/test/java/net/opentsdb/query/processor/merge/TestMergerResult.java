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
package net.opentsdb.query.processor.merge;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.rollup.RollupConfig;

public class TestMergerResult {
  
  private Merger node;
  private MergerConfig config;
  private QueryResult result_a;
  private QueryResult result_b;
  private TimeSpecification time_spec;
  private RollupConfig rollup_config;
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
    
    config = (MergerConfig) MergerConfig.newBuilder()
        .setAggregator("sum")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setDataSource("MyMetric")
        .setId("Testing")
        .build();
    node = mock(Merger.class);
    result_a = mock(QueryResult.class);
    result_b = mock(QueryResult.class);
    time_spec = mock(TimeSpecification.class);
    rollup_config = mock(RollupConfig.class);
    
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
        .addTags("dc", "lga")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts3.add(1000, 0);
    ts3.add(3000, 7);
    
    ts4 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .addTags("host", "web02")
        .addTags("dc", "lga")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts4.add(1000, 0);
    ts4.add(3000, 7);
    
    when(result_a.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2));
    when(result_b.timeSeries()).thenReturn(Lists.newArrayList(ts3, ts4));
    when(result_b.rollupConfig()).thenReturn(rollup_config);
    when(result_a.sequenceId()).thenReturn(42l);
    when(result_a.timeSpecification()).thenReturn(time_spec);
  }
  
  @Test
  public void addTimeSpec() throws Exception {
    MergerResult merger = new MergerResult(node, result_b);
    assertNull(merger.timeSpecification());
    
    merger.add(result_a);
    assertSame(time_spec, merger.timeSpecification());
    assertEquals("MyMetric", merger.dataSource());
  }
  
  @Test
  public void addRollupConfig() throws Exception {
    MergerResult merger = new MergerResult(node, result_a);
    assertNull(merger.rollupConfig());
    
    merger.add(result_b);
    assertSame(rollup_config, merger.rollupConfig());
  }
  
  @Test
  public void fullMatch() throws Exception {
    MergerResult merger = new MergerResult(node, result_a);
    merger.add(result_b);
    merger.join();
    assertEquals(42, merger.sequenceId());
    assertSame(time_spec, merger.timeSpecification());
    assertEquals(2, merger.groups.size());
    
    // xx hash is deterministic
    MergerTimeSeries ts = (MergerTimeSeries) 
        merger.groups.get(ts1.id().buildHashCode());
    assertEquals(2, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    assertTrue(ts.sources.contains(ts3));
    
    ts = (MergerTimeSeries) merger.groups.get(ts2.id().buildHashCode());
    assertEquals(2, ts.sources().size());
    assertTrue(ts.sources.contains(ts2));
    assertTrue(ts.sources.contains(ts4));
  }

  @Test
  public void aMissingOne() throws Exception {
    when(result_a.timeSeries()).thenReturn(
        Lists.<TimeSeries>newArrayList(ts2));
    
    MergerResult merger = new MergerResult(node, result_a);
    merger.add(result_b);
    merger.join();
    assertEquals(42, merger.sequenceId());
    assertSame(time_spec, merger.timeSpecification());
    assertEquals(2, merger.groups.size());
    
    // xx hash is deterministic
    MergerTimeSeries ts = (MergerTimeSeries) 
        merger.groups.get(ts1.id().buildHashCode());
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts3));
    
    ts = (MergerTimeSeries) merger.groups.get(ts2.id().buildHashCode());
    assertEquals(2, ts.sources().size());
    assertTrue(ts.sources.contains(ts2));
    assertTrue(ts.sources.contains(ts4));
  }
  
  @Test
  public void bMissingOne() throws Exception {
    when(result_b.timeSeries()).thenReturn(
        Lists.<TimeSeries>newArrayList(ts4));
    
    MergerResult merger = new MergerResult(node, result_a);
    merger.add(result_b);
    merger.join();
    assertEquals(42, merger.sequenceId());
    assertSame(time_spec, merger.timeSpecification());
    assertEquals(2, merger.groups.size());
    
    // xx hash is deterministic
    MergerTimeSeries ts = (MergerTimeSeries) 
        merger.groups.get(ts1.id().buildHashCode());
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    
    ts = (MergerTimeSeries) merger.groups.get(ts2.id().buildHashCode());
    assertEquals(2, ts.sources().size());
    assertTrue(ts.sources.contains(ts2));
    assertTrue(ts.sources.contains(ts4));
  }
  
  @Test
  public void aEmpty() throws Exception {
    when(result_a.timeSeries()).thenReturn(
        Lists.<TimeSeries>newArrayList());
    
    MergerResult merger = new MergerResult(node, result_a);
    merger.add(result_b);
    merger.join();
    assertEquals(42, merger.sequenceId());
    assertSame(time_spec, merger.timeSpecification());
    assertEquals(2, merger.groups.size());
    
    // xx hash is deterministic
    MergerTimeSeries ts = (MergerTimeSeries) 
        merger.groups.get(ts1.id().buildHashCode());
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts3));
    
    ts = (MergerTimeSeries) merger.groups.get(ts2.id().buildHashCode());
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts4));
  }

  @Test
  public void bEmpty() throws Exception {
    when(result_b.timeSeries()).thenReturn(
        Lists.<TimeSeries>newArrayList());
    
    MergerResult merger = new MergerResult(node, result_a);
    merger.add(result_b);
    merger.join();
    assertEquals(42, merger.sequenceId());
    assertSame(time_spec, merger.timeSpecification());
    assertEquals(2, merger.groups.size());
    
    // xx hash is deterministic
    MergerTimeSeries ts = (MergerTimeSeries) 
        merger.groups.get(ts1.id().buildHashCode());
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts1));
    
    ts = (MergerTimeSeries) merger.groups.get(ts2.id().buildHashCode());
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts2));
  }

  @Test
  public void allEmpty() throws Exception {
    when(result_a.timeSeries()).thenReturn(
        Lists.<TimeSeries>newArrayList());
    when(result_b.timeSeries()).thenReturn(
        Lists.<TimeSeries>newArrayList());
    
    MergerResult merger = new MergerResult(node, result_a);
    merger.add(result_b);
    merger.join();
    assertEquals(42, merger.sequenceId());
    assertSame(time_spec, merger.timeSpecification());
    assertEquals(0, merger.groups.size());
    assertEquals(0, merger.timeSeries().size());
  }
  
  @Test
  public void joinOneError() throws Exception {
    when(result_a.timeSeries()).thenReturn(
        Lists.<TimeSeries>newArrayList(ts2));
    
    MergerResult merger = new MergerResult(node, result_a);
    result_b = mock(QueryResult.class);
    when(result_b.error()).thenReturn("Error");
    merger.add(result_b);
    merger.join();
    assertEquals(42, merger.sequenceId());
    assertSame(time_spec, merger.timeSpecification());
    assertEquals(1, merger.groups.size());
    assertNull(merger.error());
    
    // xx hash is deterministic
    MergerTimeSeries ts = (MergerTimeSeries) 
        merger.groups.get(ts2.id().buildHashCode());
    assertEquals(1, ts.sources().size());
    assertTrue(ts.sources.contains(ts2));
  }
  
  @Test
  public void joinTwoErrors() throws Exception {
    result_a = mock(QueryResult.class);
    when(result_a.sequenceId()).thenReturn(42l);
    when(result_a.timeSpecification()).thenReturn(time_spec);
    when(result_a.error()).thenReturn("ErrorA");
    result_b = mock(QueryResult.class);
    when(result_b.error()).thenReturn("ErrorB");
    
    MergerResult merger = new MergerResult(node, result_a);
    merger.add(result_b);
    merger.join();
    assertEquals(42, merger.sequenceId());
    assertSame(time_spec, merger.timeSpecification());
    assertEquals(0, merger.groups.size());
    assertEquals("ErrorA", merger.error);
  }
}
