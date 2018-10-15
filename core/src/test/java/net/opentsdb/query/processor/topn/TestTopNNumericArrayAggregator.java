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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.reflect.TypeToken;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;

public class TestTopNNumericArrayAggregator {

  public static MockTSDB TSDB;
  
  private TopN node;
  private TopNConfig config;
  private QueryResult result;
  private TimeSeries source;
  
  @BeforeClass
  public static void beforeClass() {
    TSDB = new MockTSDB();
    TSDB.registry = new DefaultRegistry(TSDB);
    ((DefaultRegistry) TSDB.registry).initialize(true);
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
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(node.pipelineContext()).thenReturn(context);
    when(context.tsdb()).thenReturn(TSDB);
  }
  
  @Test
  public void ctor() throws Exception {
    TopNNumericArrayAggregator aggregator = 
        new TopNNumericArrayAggregator(node, result, source);
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
      new TopNNumericArrayAggregator(node, result, source);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void noSuchType() throws Exception {
    when(source.iterator(any(TypeToken.class)))
      .thenReturn(Optional.empty());
    TopNNumericArrayAggregator aggregator = 
        new TopNNumericArrayAggregator(node, result, source);
    assertNull(aggregator.run());
  }
  
  @Test
  public void empty() throws Exception {
    when(source.iterator(any(TypeToken.class)))
      .thenAnswer(new Answer<Optional>() {
        @Override
        public Optional answer(InvocationOnMock invocation) throws Throwable {
          Iterator<TimeSeriesValue<?>> iterator = mock(Iterator.class);
          return Optional.of(mock(Iterator.class));
        }
      });
    TopNNumericArrayAggregator aggregator = 
        new TopNNumericArrayAggregator(node, result, source);
    assertNull(aggregator.run());
  }

  @Test
  public void runLongs() throws Exception {
    setSource(
        new MutableNumericValue(new MillisecondTimeStamp(0L), 1),
        new MutableNumericValue(new MillisecondTimeStamp(1000L), 3),
        new MutableNumericValue(new MillisecondTimeStamp(2000L), -1),
        new MutableNumericValue(new MillisecondTimeStamp(3000L), 4)
        );
    TopNNumericArrayAggregator aggregator = 
        new TopNNumericArrayAggregator(node, result, source);
    assertEquals(7, aggregator.run().longValue());
  }
  
  @Test
  public void runDoubles() throws Exception {
    setSource(
        new MutableNumericValue(new MillisecondTimeStamp(0L), 1.7),
        new MutableNumericValue(new MillisecondTimeStamp(1000L), 3.6),
        new MutableNumericValue(new MillisecondTimeStamp(2000L), -1.0),
        new MutableNumericValue(new MillisecondTimeStamp(3000L), 4.3)
        );
    TopNNumericArrayAggregator aggregator = 
        new TopNNumericArrayAggregator(node, result, source);
    assertEquals(8.6, aggregator.run().doubleValue(), 0.001);

    // NaNs non-infectious
    setSource(
        new MutableNumericValue(new MillisecondTimeStamp(0L), Double.NaN),
        new MutableNumericValue(new MillisecondTimeStamp(1000L), 3.6),
        new MutableNumericValue(new MillisecondTimeStamp(2000L), Double.NaN),
        new MutableNumericValue(new MillisecondTimeStamp(3000L), 4.3)
        );
    aggregator = new TopNNumericArrayAggregator(node, result, source);
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
    aggregator = new TopNNumericArrayAggregator(node, result, source);
    assertTrue(Double.isNaN(aggregator.run().doubleValue()));
  }
  
  @Test
  public void runLongThenDoubles() throws Exception {
    setSource(
        new MutableNumericValue(new MillisecondTimeStamp(0L), 1),
        new MutableNumericValue(new MillisecondTimeStamp(1000L), 3),
        new MutableNumericValue(new MillisecondTimeStamp(2000L), -1.0),
        new MutableNumericValue(new MillisecondTimeStamp(3000L), 4.3)
        );
    TopNNumericArrayAggregator aggregator = 
        new TopNNumericArrayAggregator(node, result, source);
    assertEquals(7.3, aggregator.run().doubleValue(), 0.001);
  }
  
  @Test
  public void runDoublesThenLongs() throws Exception {
    setSource(
        new MutableNumericValue(new MillisecondTimeStamp(0L), 1.7),
        new MutableNumericValue(new MillisecondTimeStamp(1000L), 3.8),
        new MutableNumericValue(new MillisecondTimeStamp(2000L), -1),
        new MutableNumericValue(new MillisecondTimeStamp(3000L), 4)
        );
    TopNNumericArrayAggregator aggregator = 
        new TopNNumericArrayAggregator(node, result, source);
    assertEquals(8.5, aggregator.run().doubleValue(), 0.001);
  }
  
  void setSource(final MutableNumericValue ...values) {
    source = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
            .setMetric("a")
            .build(), 
        new SecondTimeStamp(0));
    for (final MutableNumericValue value : values) {
      if (value.isInteger()) {
        ((NumericArrayTimeSeries) source).add(value.longValue());
      } else {
        ((NumericArrayTimeSeries) source).add( value.doubleValue());
      }
    }
  }
}
