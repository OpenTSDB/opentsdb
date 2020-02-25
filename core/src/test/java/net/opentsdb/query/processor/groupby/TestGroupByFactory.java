// This file is part of OpenTSDB.
// Copyright (C) 2017-2020  The OpenTSDB Authors.
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import net.opentsdb.configuration.Configuration;
import org.junit.Test;

import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIteratorFactory;

public class TestGroupByFactory {
  
  @Test
  public void ctor() throws Exception {
    final GroupByFactory factory = new GroupByFactory();
    assertEquals(3, factory.types().size());
    assertTrue(factory.types().contains(NumericType.TYPE));
    Configuration configuration = mock(Configuration.class);
    TSDB tsdb = mock(TSDB.class);
    when(tsdb.getRegistry()).thenReturn(mock(Registry.class));
    when(tsdb.getConfig()).thenReturn(configuration);
    factory.initialize(tsdb, null).join(1);
    assertEquals(GroupByFactory.TYPE, factory.id());
  }
  
  @Test
  public void registerIteratorFactory() throws Exception {
    final GroupByFactory factory = new GroupByFactory();
    assertEquals(3, factory.types().size());
    
    QueryIteratorFactory mock = mock(QueryIteratorFactory.class);
    factory.registerIteratorFactory(NumericType.TYPE, mock);
    assertEquals(3, factory.types().size());
    
    try {
      factory.registerIteratorFactory(null, mock);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.registerIteratorFactory(NumericType.TYPE, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
//  @Test
//  public void newIterator() throws Exception {
//    NumericInterpolatorConfig numeric_config = 
//        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
//    .setFillPolicy(FillPolicy.NOT_A_NUMBER)
//    .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
//    .setDataType(NumericType.TYPE.toString())
//    .build();
//    
//    NumericSummaryInterpolatorConfig summary_config = 
//        (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
//    .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
//    .setDefaultRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
//    .addExpectedSummary(0)
//    .setDataType(NumericSummaryType.TYPE.toString())
//    .build();
//    
//    final GroupByConfig config = (GroupByConfig) GroupByConfig.newBuilder()
//        .setAggregator("sum")
//        .addTagKey("host")
//        .addInterpolatorConfig(numeric_config)
//        .addInterpolatorConfig(summary_config)
//        .setId("Test")
//        .build();
//    final NumericMillisecondShard source = new NumericMillisecondShard(
//        BaseTimeSeriesStringId.newBuilder()
//        .setMetric("a")
//        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
//    source.add(1000, 42);
//    
//    final GroupByResult result = mock(GroupByResult.class);
//    final QueryResult source_result = mock(QueryResult.class);
//    final TimeSpecification time_spec = mock(TimeSpecification.class);
//    
//    when(result.downstreamResult()).thenReturn(source_result);
//    when(source_result.timeSpecification()).thenReturn(time_spec);
//    when(time_spec.start()).thenReturn(new MillisecondTimeStamp(1000));
//    
//    final DefaultRollupConfig rollup_config = DefaultRollupConfig.newBuilder()
//        .addAggregationId("sum", 0)
//        .addAggregationId("count", 2)
//        .addAggregationId("avg", 5)
//        .addInterval(RollupInterval.builder()
//            .setInterval("sum")
//            .setTable("tsdb")
//            .setPreAggregationTable("tsdb")
//            .setInterval("1h")
//            .setRowSpan("1d"))
//        .build();
//    when(result.rollupConfig()).thenReturn(rollup_config);
//    final GroupBy node = mock(GroupBy.class);
//    when(node.config()).thenReturn(config);
//    final QueryPipelineContext context = mock(QueryPipelineContext.class);
//    when(node.pipelineContext()).thenReturn(context);
//    final TSDB tsdb = mock(TSDB.class);
//    when(context.tsdb()).thenReturn(tsdb);
//    final Registry registry = mock(Registry.class);
//    when(tsdb.getRegistry()).thenReturn(registry);
//    when(registry.getPlugin(eq(NumericArrayAggregatorFactory.class), anyString()))
//      .thenReturn(new ArraySumFactory());
//    final QueryInterpolatorFactory interp_factory = new DefaultInterpolatorFactory();
//    interp_factory.initialize(tsdb, null).join();
//    when(registry.getPlugin(eq(QueryInterpolatorFactory.class), anyString()))
//      .thenReturn(interp_factory);
//    when(registry.getPlugin(eq(NumericAggregatorFactory.class), anyString()))
//      .thenReturn(new SumFactory());
//    final GroupByFactory factory = new GroupByFactory();
//    
//    Iterator<TimeSeriesValue<?>> iterator = factory.newTypedIterator(
//        NumericType.TYPE, node, result, ImmutableMap.<String, TimeSeries>builder()
//        .put("a", source)
//        .build());
//    assertTrue(iterator.hasNext());
//    
//    MockTimeSeries mockts = new MockTimeSeries(
//        BaseTimeSeriesStringId.newBuilder()
//        .setMetric("a")
//        .build());
//    
//    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(30000));
//    v.resetValue(0, 42);
//    v.resetValue(2, 2);
//    mockts.addValue(v);
//    
//    iterator = factory.newTypedIterator(
//        NumericSummaryType.TYPE, node, result, ImmutableMap.<String, TimeSeries>builder()
//        .put("a", mockts)
//        .build());
//    assertTrue(iterator.hasNext());
//    
//    // array 
//    TimeSeries ts2 = new NumericArrayTimeSeries(
//        BaseTimeSeriesStringId.newBuilder()
//        .setMetric("a")
//        .build(), new MillisecondTimeStamp(1000));
//    ((NumericArrayTimeSeries) ts2).add(4);
//    ((NumericArrayTimeSeries) ts2).add(10);
//    ((NumericArrayTimeSeries) ts2).add(8);
//    ((NumericArrayTimeSeries) ts2).add(6);
//    
//    iterator = factory.newTypedIterator(
//        NumericArrayType.TYPE, node, result, ImmutableMap.<String, TimeSeries>builder()
//        .put("a", ts2)
//        .build());
//    assertTrue(iterator.hasNext());
//    
//    try {
//      factory.newTypedIterator(null, node, result, ImmutableMap.<String, TimeSeries>builder()
//          .put("a", source)
//          .build());
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      factory.newTypedIterator(NumericType.TYPE, null, result, ImmutableMap.<String, TimeSeries>builder()
//          .put("a", source)
//          .build());
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      factory.newTypedIterator(NumericType.TYPE, node, result, (Map) null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      factory.newTypedIterator(NumericType.TYPE, node, result, Collections.emptyMap());
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    iterator = factory.newTypedIterator(NumericType.TYPE, node, result,
//        Lists.<TimeSeries>newArrayList(source));
//    assertTrue(iterator.hasNext());
//    
//    try {
//      factory.newTypedIterator(null, node, result, Lists.<TimeSeries>newArrayList(source));
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      factory.newTypedIterator(NumericType.TYPE, null, result,
//          Lists.<TimeSeries>newArrayList(source));
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      factory.newTypedIterator(NumericType.TYPE, node, result, (List) null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      factory.newTypedIterator(NumericType.TYPE, node, result, Collections.emptyList());
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//  }
}