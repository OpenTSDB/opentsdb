// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.topn;

import com.google.common.reflect.TypeToken;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestTopNNumericSummaryAggregator {
  
  public static MockTSDB TSDB;

  private TopN node;
  private TopNConfig config;
  private QueryResult result;
  private TimeSeries source;
  
  @BeforeClass
  public static void beforeClass() {
    TSDB = MockTSDBDefault.getMockTSDB();
  }
  
  @Before
  public void before() throws Exception {
    node = mock(TopN.class);
    result = mock(QueryResult.class);
    source = mock(TimeSeries.class);
    
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("sum")
        .setCount(10)
        .setTop(true)
        .setId("top")
        .build();
    when(node.config()).thenReturn(config);
    
    RollupConfig rollup_config = DefaultRollupConfig.newBuilder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 2)
        .addAggregationId("avg", 5)
        .addAggregationId("max", 1)
        .addInterval(RollupInterval.builder()
            .setInterval("1m")
            .setTable("tsdb")
            .setRowSpan("1h")
            .setPreAggregationTable("tsdb-agg")
            .setDefaultInterval(true)
            .build())
        .build();
    when(result.rollupConfig()).thenReturn(rollup_config);
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(node.pipelineContext()).thenReturn(context);
    when(context.tsdb()).thenReturn(TSDB);
  }
  
  @Test
  public void ctor() throws Exception {
    TopNNumericSummaryAggregator aggregator = 
        new TopNNumericSummaryAggregator(node, result, source);
    assertSame(node, aggregator.node);
    assertSame(source, aggregator.series);
    
    // no such agg
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("nosuchagg")
        .setCount(10)
        .setTop(true)
        .setId("top")
        .build();
    when(node.config()).thenReturn(config);
    try {
      new TopNNumericSummaryAggregator(node, result, source);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void noSuchType() throws Exception {
    when(source.iterator(any(TypeToken.class)))
      .thenReturn(Optional.empty());
    TopNNumericSummaryAggregator aggregator = 
        new TopNNumericSummaryAggregator(node, result, source);
    assertNull(aggregator.run());
  }
  
  @Test
  public void empty() throws Exception {
    when(source.iterator(any(TypeToken.class)))
      .thenAnswer((Answer<Optional>) invocation -> {
        TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = mock(TypedTimeSeriesIterator.class);
        return Optional.of(iterator);
      });
    TopNNumericSummaryAggregator aggregator = 
        new TopNNumericSummaryAggregator(node, result, source);
    assertNull(aggregator.run());
  }

  @Test
  public void runLongs() throws Exception {
    setSource(
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(0L))
          .addValue(0, 1).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(1000L))
          .addValue(0, 3).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(2000L))
          .addValue(0, -1).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(3000L))
          .addValue(0, 4).build()
        );
    TopNNumericSummaryAggregator aggregator = 
        new TopNNumericSummaryAggregator(node, result, source);
    assertEquals(7, aggregator.run().longValue());
    
    // nulls
    setSource(
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(0L))
          .setNull().build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(1000L))
          .addValue(0, 3).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(2000L))
          .setNull().build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(3000L))
          .addValue(0, 4).build()
        );
    aggregator = new TopNNumericSummaryAggregator(node, result, source);
    assertEquals(7, aggregator.run().longValue());
  }
  
  @Test
  public void runDoubles() throws Exception {
    setSource(
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(0L))
          .addValue(0, 1.7).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(1000L))
          .addValue(0, 3.6).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(2000L))
          .addValue(0, -1.0).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(3000L))
          .addValue(0, 4.3).build()
        );
    TopNNumericSummaryAggregator aggregator = 
        new TopNNumericSummaryAggregator(node, result, source);
    assertEquals(8.6, aggregator.run().doubleValue(), 0.001);
    
    // nulls
    setSource(
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(0L))
          .setNull().build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(1000L))
          .addValue(0, 3.6).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(2000L))
          .setNull().build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(3000L))
          .addValue(0, 4.3).build()
        );
    aggregator = new TopNNumericSummaryAggregator(node, result, source);
    assertEquals(7.9, aggregator.run().doubleValue(), 0.001);
    
    // NaNs non-infectious
    setSource(
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(0L))
          .addValue(0, Double.NaN).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(1000L))
          .addValue(0, 3.6).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(2000L))
          .addValue(0, Double.NaN).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(3000L))
          .addValue(0, 4.3).build()
        );
    aggregator = new TopNNumericSummaryAggregator(node, result, source);
    assertEquals(7.9, aggregator.run().doubleValue(), 0.001);
    
    // NaNs infectious
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("sum")
        .setCount(10)
        .setTop(true)
        .setInfectiousNan(true)
        .setId("top")
        .build();
    when(node.config()).thenReturn(config);
    aggregator = new TopNNumericSummaryAggregator(node, result, source);
    assertTrue(Double.isNaN(aggregator.run().doubleValue()));
  }
  
  @Test
  public void runLongThenDoubles() throws Exception {
    setSource(
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(0L))
          .addValue(0, 1).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(1000L))
          .addValue(0, 3).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(2000L))
          .addValue(0, -1.0).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(3000L))
          .addValue(0, 4.3).build()
        );
    TopNNumericSummaryAggregator aggregator = 
        new TopNNumericSummaryAggregator(node, result, source);
    assertEquals(7.3, aggregator.run().doubleValue(), 0.001);
  }
  
  @Test
  public void runDoublesThenLongs() throws Exception {
    setSource(
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(0L))
          .addValue(0, 1.7).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(1000L))
          .addValue(0, 3.8).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(2000L))
          .addValue(0, -1).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(3000L))
          .addValue(0, 4).build()
        );
    TopNNumericSummaryAggregator aggregator = 
        new TopNNumericSummaryAggregator(node, result, source);
    assertEquals(8.5, aggregator.run().doubleValue(), 0.001);
  }
  
  @Test
  public void runCount() throws Exception {
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("count")
        .setCount(10)
        .setTop(true)
        .setId("top")
        .build();
    when(node.config()).thenReturn(config);
    
    setSource(
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(0L))
          .addValue(2, 1).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(1000L))
          .addValue(2, 3).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(2000L))
          .addValue(2, -1).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(3000L))
          .addValue(2, 4).build()
        );
    TopNNumericSummaryAggregator aggregator = 
        new TopNNumericSummaryAggregator(node, result, source);
    assertEquals(7, aggregator.run().longValue());
  }
  
  @Test
  public void runAvgPresent() throws Exception {
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("avg")
        .setCount(10)
        .setTop(true)
        .setId("top")
        .build();
    when(node.config()).thenReturn(config);
    
    setSource(
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(0L))
          .addValue(5, 1).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(1000L))
          .addValue(5, 3).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(2000L))
          .addValue(5, -1).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(3000L))
          .addValue(5, 4).build()
        );
    TopNNumericSummaryAggregator aggregator = 
        new TopNNumericSummaryAggregator(node, result, source);
    assertEquals(1.75, aggregator.run().doubleValue(), 0.001);
  }
  
  @Test
  public void runAvgNotPresent() throws Exception {
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("avg")
        .setCount(10)
        .setTop(true)
        .setId("top")
        .build();
    when(node.config()).thenReturn(config);
    
    setSource(
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(0L))
          .addValue(0, 1)
          .addValue(2, 1).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(1000L))
          .addValue(0, 3)
          .addValue(2, 1).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(2000L))
          .addValue(0, -1)
          .addValue(2, 1).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(3000L))
          .addValue(0, 4)
          .addValue(2, 1).build()
        );
    TopNNumericSummaryAggregator aggregator = 
        new TopNNumericSummaryAggregator(node, result, source);
    assertEquals(1.75, aggregator.run().doubleValue(), 0.001);
  }
  
  // shouldn't happen
  @Test
  public void runAvgSwitchUp() throws Exception {
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("avg")
        .setCount(10)
        .setTop(true)
        .setId("top")
        .build();
    when(node.config()).thenReturn(config);
    
    setSource(
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(0L))
          .addValue(5, 1).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(1000L))
          .addValue(0, 3)
          .addValue(2, 1).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(2000L))
          .addValue(5, -1).build(),
        MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new MillisecondTimeStamp(3000L))
          .addValue(0, 4)
          .addValue(2, 1).build()
        );
    TopNNumericSummaryAggregator aggregator = 
        new TopNNumericSummaryAggregator(node, result, source);
    assertEquals(3.5, aggregator.run().doubleValue(), 0.001);
  }
  
  void setSource(final MutableNumericSummaryValue ...values) {
    source = new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    for (final MutableNumericSummaryValue value : values) {
      ((MockTimeSeries) source).addValue(value);
    }
  }
}
