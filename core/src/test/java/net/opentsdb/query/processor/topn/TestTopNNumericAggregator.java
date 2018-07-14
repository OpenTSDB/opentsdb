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
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.query.QueryResult;

public class TestTopNNumericAggregator {

  private TopN node;
  private TopNConfig config;
  private QueryResult result;
  private TimeSeries source;
  
  @Before
  public void before() throws Exception {
    node = mock(TopN.class);
    result = mock(QueryResult.class);
    source = mock(TimeSeries.class);
    
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("sum")
        .setCount(10)
        .setTop(true)
        .build();
    when(node.config()).thenReturn(config);
  }
  
  @Test
  public void ctor() throws Exception {
    TopNNumericAggregator aggregator = 
        new TopNNumericAggregator(node, result, source);
    assertSame(node, aggregator.node);
    assertSame(source, aggregator.series);
    
    // no such agg
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("nosuchagg")
        .setCount(10)
        .setTop(true)
        .build();
    when(node.config()).thenReturn(config);
    try {
      new TopNNumericAggregator(node, result, source);
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
  
  @Test
  public void noSuchType() throws Exception {
    when(source.iterator(any(TypeToken.class)))
      .thenReturn(Optional.empty());
    TopNNumericAggregator aggregator = 
        new TopNNumericAggregator(node, result, source);
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
    TopNNumericAggregator aggregator = 
        new TopNNumericAggregator(node, result, source);
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
    TopNNumericAggregator aggregator = 
        new TopNNumericAggregator(node, result, source);
    assertEquals(7, aggregator.run().longValue());
    
    // nulls
    setSource(
        new MutableNumericValue(new MillisecondTimeStamp(0L), null),
        new MutableNumericValue(new MillisecondTimeStamp(1000L), 3),
        new MutableNumericValue(new MillisecondTimeStamp(2000L), null),
        new MutableNumericValue(new MillisecondTimeStamp(3000L), 4)
        );
    aggregator = new TopNNumericAggregator(node, result, source);
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
    TopNNumericAggregator aggregator = 
        new TopNNumericAggregator(node, result, source);
    assertEquals(8.6, aggregator.run().doubleValue(), 0.001);
    
    // nulls
    setSource(
        new MutableNumericValue(new MillisecondTimeStamp(0L), null),
        new MutableNumericValue(new MillisecondTimeStamp(1000L), 3.6),
        new MutableNumericValue(new MillisecondTimeStamp(2000L), null),
        new MutableNumericValue(new MillisecondTimeStamp(3000L), 4.3)
        );
    aggregator = new TopNNumericAggregator(node, result, source);
    assertEquals(7.9, aggregator.run().doubleValue(), 0.001);
    
    // NaNs non-infectious
    setSource(
        new MutableNumericValue(new MillisecondTimeStamp(0L), Double.NaN),
        new MutableNumericValue(new MillisecondTimeStamp(1000L), 3.6),
        new MutableNumericValue(new MillisecondTimeStamp(2000L), Double.NaN),
        new MutableNumericValue(new MillisecondTimeStamp(3000L), 4.3)
        );
    aggregator = new TopNNumericAggregator(node, result, source);
    assertEquals(7.9, aggregator.run().doubleValue(), 0.001);
    
    // NaNs infectious
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("sum")
        .setCount(10)
        .setTop(true)
        .setInfectiousNan(true)
        .build();
    when(node.config()).thenReturn(config);
    aggregator = new TopNNumericAggregator(node, result, source);
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
    TopNNumericAggregator aggregator = 
        new TopNNumericAggregator(node, result, source);
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
    TopNNumericAggregator aggregator = 
        new TopNNumericAggregator(node, result, source);
    assertEquals(8.5, aggregator.run().doubleValue(), 0.001);
  }
  
  void setSource(final MutableNumericValue ...values) {
    source = new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    for (final MutableNumericValue value : values) {
      ((MockTimeSeries) source).addValue(value);
    }
  }
}
