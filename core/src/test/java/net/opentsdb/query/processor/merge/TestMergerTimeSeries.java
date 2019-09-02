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
package net.opentsdb.query.processor.merge;

import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.data.types.numeric.aggregators.SumFactory;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.interpolation.DefaultInterpolatorFactory;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestMergerTimeSeries {
  
  private QueryNodeFactory factory;
  private QueryResult result;
  private Merger node;
  private MergerConfig config;
  private TimeSeriesStringId id_a;
  private TimeSeriesStringId id_b;
  private TimeSeries source_a;
  private TimeSeries source_b;
  
  @Before
  public void before() throws Exception {
    NumericInterpolatorConfig numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
    .setFillPolicy(FillPolicy.NOT_A_NUMBER)
    .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
    .setDataType(NumericType.TYPE.toString())
    .build();
    
    NumericSummaryInterpolatorConfig summary_config = 
        (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
    .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
    .setDefaultRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
    .addExpectedSummary(0)
    .setDataType(NumericSummaryType.TYPE.toString())
    .build();
    
    factory = new MergerFactory();
    node = mock(Merger.class);
    config = (MergerConfig) MergerConfig.newBuilder()
        .setAggregator("sum")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setDataSource("m1")
        .setId("GB")
        .build();
    result = mock(QueryResult.class);
    final QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(node.pipelineContext()).thenReturn(context);
    final TSDB tsdb = mock(TSDB.class);
    when(context.tsdb()).thenReturn(tsdb);
    final Registry registry = mock(Registry.class);
    when(tsdb.getRegistry()).thenReturn(registry);
    final QueryInterpolatorFactory interp_factory = new DefaultInterpolatorFactory();
    interp_factory.initialize(tsdb, null).join();
    when(registry.getPlugin(eq(QueryInterpolatorFactory.class), anyString()))
      .thenReturn(interp_factory);
    when(registry.getPlugin(eq(NumericAggregatorFactory.class), anyString()))
      .thenReturn(new SumFactory());
    
    when(node.factory()).thenReturn(factory);
    when(node.config()).thenReturn(config);
    
    id_a = BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .addTags("host", "web01")
        .build();
    
    id_b = BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .addTags("host", "web02")
        .build();
    
    source_a = mock(TimeSeries.class);
    source_b = mock(TimeSeries.class);
    when(source_a.id()).thenReturn(id_a);
    when(source_b.id()).thenReturn(id_b);
    when(source_a.types()).thenReturn(Sets.newHashSet(NumericType.TYPE));
    when(source_b.types()).thenReturn(Sets.newHashSet(NumericType.TYPE));
    when(source_a.iterator(any(TypeToken.class)))
      .thenReturn(Optional.of(mock(TypedTimeSeriesIterator.class)));
    when(source_b.iterator(any(TypeToken.class)))
    .thenReturn(Optional.of(mock(TypedTimeSeriesIterator.class)));
  }
  
  @Test
  public void ctor() throws Exception {
    new MergerTimeSeries(node, result);
    
    try {
      new MergerTimeSeries(null, result);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void addSource() throws Exception {
    MergerTimeSeries ts = new MergerTimeSeries(node, result);
    ts.addSource(source_a);
    ts.addSource(source_b);
    assertEquals(2, ts.sources().size());
    
    // add same is fine, hashed :)
    ts.addSource(source_a);
    assertEquals(2, ts.sources().size());
    
    try {
      ts.addSource(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void id() throws Exception {
    MergerTimeSeries ts = new MergerTimeSeries(node, result);
    ts.addSource(source_a);
    ts.addSource(source_b);
    
    TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    assertSame(source_a.id(), id);
  }
  
  @Test
  public void iterator() throws Exception {
    MergerTimeSeries ts = new MergerTimeSeries(node, result);
    ts.addSource(source_a);
    ts.addSource(source_b);
    
    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> opt =
        ts.iterator(NumericType.TYPE);
    assertTrue(opt.isPresent());
    final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = opt.get();
    assertFalse(iterator.hasNext());
    
    opt = ts.iterator(AnnotationType.TYPE);
    assertFalse(opt.isPresent());
    
    try {
      ts.iterator(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void iterators() throws Exception {
    MergerTimeSeries ts = new MergerTimeSeries(node, result);
    ts.addSource(source_a);
    ts.addSource(source_b);
    
    final Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>>
      iterators = ts.iterators();
    assertEquals(1, iterators.size());
    
    final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator =
        iterators.iterator().next();
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void types() throws Exception {
    MergerTimeSeries ts = new MergerTimeSeries(node, result);
    ts.addSource(source_a);
    ts.addSource(source_b);
    
    final Collection<TypeToken<? extends TimeSeriesDataType>> types = ts.types();
    assertEquals(1, types.size());
    assertTrue(types.contains(NumericType.TYPE));
  }
}
