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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory;
import net.opentsdb.query.pojo.FillPolicy;

public class TestGroupByTimeSeries {

  private QueryNodeFactory factory;
  private GroupBy node;
  private GroupByConfig config;
  private TimeSeriesId id_a;
  private TimeSeriesId id_b;
  private TimeSeries source_a;
  private TimeSeries source_b;
  
  @Before
  public void before() throws Exception {
    factory = new GroupByFactory("GroupBy");
    node = mock(GroupBy.class);
    config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("GB")
        .addTagKey("host")
        .setQueryIteratorInterpolatorFactory(new NumericInterpolatorFactory.Default())
        .setQueryIteratorInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
          .setFillPolicy(FillPolicy.NOT_A_NUMBER)
          .setRealFillPolicy(FillWithRealPolicy.NONE)
          .build())
        .build();
    
    when(node.factory()).thenReturn(factory);
    when(node.config()).thenReturn(config);
    
    id_a = BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .addTags("host", "web01")
        .build();
    
    id_b = BaseTimeSeriesId.newBuilder()
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
      .thenReturn(Optional.of(mock(Iterator.class)));
    when(source_b.iterator(any(TypeToken.class)))
    .thenReturn(Optional.of(mock(Iterator.class)));
  }
  
  @Test
  public void ctor() throws Exception {
    GroupByTimeSeries ts = new GroupByTimeSeries(node);
    try {
      ts.id();
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    try {
      new GroupByTimeSeries(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void addSource() throws Exception {
    GroupByTimeSeries ts = new GroupByTimeSeries(node);
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
    
    ts.id();
    try {
      ts.addSource(source_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void id() throws Exception {
    GroupByTimeSeries ts = new GroupByTimeSeries(node);
    ts.addSource(source_a);
    ts.addSource(source_b);
    
    TimeSeriesId id = ts.id();
    assertEquals("a", id.metric());
    assertTrue(id.tags().isEmpty());
    assertTrue(id.aggregatedTags().contains("host"));
  }
  
  @Test
  public void iterator() throws Exception {
    GroupByTimeSeries ts = new GroupByTimeSeries(node);
    ts.addSource(source_a);
    ts.addSource(source_b);
    
    Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> opt = 
        ts.iterator(NumericType.TYPE);
    assertTrue(opt.isPresent());
    final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = opt.get();
    assertFalse(iterator.hasNext());
    
    opt = ts.iterator(TypeToken.of(String.class));
    assertFalse(opt.isPresent());
    
    try {
      ts.iterator(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void iterators() throws Exception {
    GroupByTimeSeries ts = new GroupByTimeSeries(node);
    ts.addSource(source_a);
    ts.addSource(source_b);
    
    final Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> 
      iterators = ts.iterators();
    assertEquals(1, iterators.size());
    
    final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = 
        iterators.iterator().next();
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void types() throws Exception {
    GroupByTimeSeries ts = new GroupByTimeSeries(node);
    ts.addSource(source_a);
    ts.addSource(source_b);
    
    final Collection<TypeToken<?>> types = ts.types();
    assertEquals(1, types.size());
    assertTrue(types.contains(NumericType.TYPE));
  }
}
