// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.timeshift;

import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;

public class TestTimeShift {

  private static TimeSeries SERIES;
  private static TimeShift NODE;
  private static TimeShiftFactory FACTORY;
  private static QueryPipelineContext CONTEXT;
  private QueryResult result;
  private TimeShiftConfig config;
  private QueryNode upstream;

  @BeforeClass
  public static void beforeClass() throws Exception {
    SERIES = mock(TimeSeries.class);
    when(SERIES.types()).thenReturn(Lists.newArrayList(NumericType.TYPE));
    NODE = mock(TimeShift.class);
    FACTORY = mock(TimeShiftFactory.class);
    CONTEXT = mock(QueryPipelineContext.class);
    when(NODE.factory()).thenReturn(FACTORY);
    when(FACTORY.newTypedIterator(eq(NumericType.TYPE), eq(NODE), 
        any(QueryResult.class), any(Collection.class)))
        .thenReturn(mock(TimeShiftNumericIterator.class));
  }
  
  @Before
  public void before() throws Exception {
    result = mock(QueryResult.class);
    upstream = mock(QueryNode.class);
    when(CONTEXT.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
    when(result.timeSeries()).thenReturn(Lists.newArrayList(SERIES));
    config = (TimeShiftConfig) TimeShiftConfig.newBuilder()
        .setConfig((TimeSeriesDataSourceConfig) 
            DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("system.cpu.user")
                .build())
            .setTimeShiftInterval("1d")
            .setPreviousIntervals(2)
            .setNextIntervals(1)
            .setId("m1")
            .build())
        .setId("m1-timeshift")
        .build();
    
    QueryNode result_node = mock(QueryNode.class);
    when(result.source()).thenReturn(result_node);
    QueryNodeConfig result_config = mock(QueryNodeConfig.class);
    when(result_node.config()).thenReturn(result_config);
  }
  
  @Test
  public void onNext() throws Exception {
    TimeShift shift = new TimeShift(FACTORY, CONTEXT, config);
    shift.initialize(null).join();
    assertSame(config, shift.config());
    
    when(result.dataSource()).thenReturn("m1-previous-P1D");
    shift.onNext(result);
    verify(upstream, times(1)).onNext(any(TimeShiftResult.class));
  }
  
  @Test
  public void onNextUnknown() throws Exception {
    TimeShift shift = new TimeShift(FACTORY, CONTEXT, config);
    shift.initialize(null).join();
    assertSame(config, shift.config());
    
    when(result.dataSource()).thenReturn("m1");
    shift.onNext(result);
    verify(upstream, never()).onNext(any(TimeShiftResult.class));
  }
  
}
