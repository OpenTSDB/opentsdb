// This file is part of OpenTSDB.
// Copyright (C) 2015-2018  The OpenTSDB Authors.
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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.utils.UnitTestException;

public class TestTopNResult {

  private TopN node;
  private TopNConfig config;
  private QueryResult result;
  
  @Before
  public void before() throws Exception {
    node = mock(TopN.class);
    result = mock(QueryResult.class);
    
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("sum")
        .setCount(1)
        .setTop(true)
        .build();
    when(node.config()).thenReturn(config);
  }
  
  @Test
  public void evaluateTopN1with2SeriesLong() throws Exception {
    when(result.timeSeries()).thenReturn(Lists.newArrayList(
        makeSeries("web01", 
            new MutableNumericValue(new MillisecondTimeStamp(0L), 1),
            new MutableNumericValue(new MillisecondTimeStamp(1000L), 3),
            new MutableNumericValue(new MillisecondTimeStamp(2000L), 5)),
        makeSeries("web02", 
            new MutableNumericValue(new MillisecondTimeStamp(0L), 100),
            new MutableNumericValue(new MillisecondTimeStamp(1000L), 32),
            new MutableNumericValue(new MillisecondTimeStamp(2000L), 55))
        ));
    TopNResult topn = new TopNResult(node, result);
    topn.run();
    assertEquals(1, topn.results.size());
    assertEquals("web02", ((TimeSeriesStringId) topn.results.get(0).id()).tags().get("host"));
    verify(node, times(1)).onNext(topn);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    
    // bottom n
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("sum")
        .setCount(1)
        .setTop(false)
        .build();
    when(node.config()).thenReturn(config);
    topn = new TopNResult(node, result);
    topn.run();
    assertEquals(1, topn.results.size());
    assertEquals("web01", ((TimeSeriesStringId) topn.results.get(0).id()).tags().get("host"));
    verify(node, times(1)).onNext(topn);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
  }
  
  @Test
  public void evaluateTopN2with2SeriesLong() throws Exception {
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("sum")
        .setCount(2)
        .setTop(true)
        .build();
    when(node.config()).thenReturn(config);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(
        makeSeries("web01", 
            new MutableNumericValue(new MillisecondTimeStamp(0L), 1),
            new MutableNumericValue(new MillisecondTimeStamp(1000L), 3),
            new MutableNumericValue(new MillisecondTimeStamp(2000L), 5)),
        makeSeries("web02", 
            new MutableNumericValue(new MillisecondTimeStamp(0L), 100),
            new MutableNumericValue(new MillisecondTimeStamp(1000L), 32),
            new MutableNumericValue(new MillisecondTimeStamp(2000L), 55))
        ));
    TopNResult topn = new TopNResult(node, result);
    topn.run();
    assertEquals(2, topn.results.size());
    assertEquals("web02", ((TimeSeriesStringId) topn.results.get(0).id()).tags().get("host"));
    assertEquals("web01", ((TimeSeriesStringId) topn.results.get(1).id()).tags().get("host"));
    verify(node, times(1)).onNext(topn);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    
    // bottom n
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("sum")
        .setCount(2)
        .setTop(false)
        .build();
    when(node.config()).thenReturn(config);
    topn = new TopNResult(node, result);
    topn.run();
    assertEquals(2, topn.results.size());
    assertEquals("web01", ((TimeSeriesStringId) topn.results.get(0).id()).tags().get("host"));
    assertEquals("web02", ((TimeSeriesStringId) topn.results.get(1).id()).tags().get("host"));
    verify(node, times(1)).onNext(topn);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
  }
  
  @Test
  public void evaluateTopN100with2SeriesLong() throws Exception {
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("sum")
        .setCount(100)
        .setTop(true)
        .build();
    when(node.config()).thenReturn(config);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(
        makeSeries("web01", 
            new MutableNumericValue(new MillisecondTimeStamp(0L), 1),
            new MutableNumericValue(new MillisecondTimeStamp(1000L), 3),
            new MutableNumericValue(new MillisecondTimeStamp(2000L), 5)),
        makeSeries("web02", 
            new MutableNumericValue(new MillisecondTimeStamp(0L), 100),
            new MutableNumericValue(new MillisecondTimeStamp(1000L), 32),
            new MutableNumericValue(new MillisecondTimeStamp(2000L), 55))
        ));
    TopNResult topn = new TopNResult(node, result);
    topn.run();
    assertEquals(2, topn.results.size());
    assertEquals("web02", ((TimeSeriesStringId) topn.results.get(0).id()).tags().get("host"));
    assertEquals("web01", ((TimeSeriesStringId) topn.results.get(1).id()).tags().get("host"));
    verify(node, times(1)).onNext(topn);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
  }
  
  @Test
  public void evaluateTopN2with2SeriesDouble() throws Exception {
    config = (TopNConfig) TopNConfig.newBuilder()
        .setAggregator("sum")
        .setCount(100)
        .setTop(true)
        .build();
    when(node.config()).thenReturn(config);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(
        makeSeries("web01", 
            new MutableNumericValue(new MillisecondTimeStamp(0L), 1.6),
            new MutableNumericValue(new MillisecondTimeStamp(1000L), 3.33),
            new MutableNumericValue(new MillisecondTimeStamp(2000L), 5.9)),
        makeSeries("web02", 
            new MutableNumericValue(new MillisecondTimeStamp(0L), 100.7),
            new MutableNumericValue(new MillisecondTimeStamp(1000L), 32.90),
            new MutableNumericValue(new MillisecondTimeStamp(2000L), 55.01))
        ));
    TopNResult topn = new TopNResult(node, result);
    topn.run();
    assertEquals(2, topn.results.size());
    assertEquals("web02", ((TimeSeriesStringId) topn.results.get(0).id()).tags().get("host"));
    assertEquals("web01", ((TimeSeriesStringId) topn.results.get(1).id()).tags().get("host"));
    verify(node, times(1)).onNext(topn);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
  }
  
  @Test
  public void evaluateTopN1with2SeriesLongDoubleMixed() throws Exception {
    when(result.timeSeries()).thenReturn(Lists.newArrayList(
        makeSeries("web01", 
            new MutableNumericValue(new MillisecondTimeStamp(0L), 1.6),
            new MutableNumericValue(new MillisecondTimeStamp(1000L), 3),
            new MutableNumericValue(new MillisecondTimeStamp(2000L), 5)),
        makeSeries("web02", 
            new MutableNumericValue(new MillisecondTimeStamp(0L), 100.7),
            new MutableNumericValue(new MillisecondTimeStamp(1000L), 32),
            new MutableNumericValue(new MillisecondTimeStamp(2000L), 55.01))
        ));
    TopNResult topn = new TopNResult(node, result);
    topn.run();
    assertEquals(1, topn.results.size());
    assertEquals("web02", ((TimeSeriesStringId) topn.results.get(0).id()).tags().get("host"));
    verify(node, times(1)).onNext(topn);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
  }
  
  @Test
  public void evaluateTopN1with2SeriesDiffSpan() throws Exception {
    when(result.timeSeries()).thenReturn(Lists.newArrayList(
        makeSeries("web01", 
            new MutableNumericValue(new MillisecondTimeStamp(0L), 1.6),
            new MutableNumericValue(new MillisecondTimeStamp(1000L), 3),
            new MutableNumericValue(new MillisecondTimeStamp(2000L), 5)),
        makeSeries("web02", 
            new MutableNumericValue(new MillisecondTimeStamp(2000L), 55.01))
        ));
    TopNResult topn = new TopNResult(node, result);
    topn.run();
    assertEquals(1, topn.results.size());
    assertEquals("web02", ((TimeSeriesStringId) topn.results.get(0).id()).tags().get("host"));
    verify(node, times(1)).onNext(topn);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void evaluateEmptyResults() throws Exception {
    TopNResult topn = new TopNResult(node, result);
    topn.run();
    assertTrue(topn.results.isEmpty());
    verify(node, times(1)).onNext(topn);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
  }
  
  @Test
  public void exceptionInRun() throws Exception {
    when(result.timeSeries()).thenThrow(new UnitTestException());
    TopNResult topn = new TopNResult(node, result);
    topn.run();
    verify(node, never()).onNext(topn);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, times(1)).onError(any(Throwable.class));
  }
  
  MockTimeSeries makeSeries(final String host, final MutableNumericValue ...values) {
    MockTimeSeries series = new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .addTags("host", host)
        .build());
    for (final MutableNumericValue value : values) {
      series.addValue(value);
    }
    return series;
  }
}
