// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.processor.downsample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryIteratorInterpolatorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;

public class TestDownsampleFactory {

  @Test
  public void ctor() throws Exception {
    final DownsampleFactory factory = new DownsampleFactory("Downsample");
    assertEquals(1, factory.types().size());
    assertTrue(factory.types().contains(NumericType.TYPE));
    assertEquals("Downsample", factory.id());
    
    try {
      new DownsampleFactory(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new DownsampleFactory("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void registerIteratorFactory() throws Exception {
    final DownsampleFactory factory = new DownsampleFactory("Downsample");
    assertEquals(1, factory.types().size());
    
    QueryIteratorFactory mock = mock(QueryIteratorFactory.class);
    factory.registerIteratorFactory(NumericType.TYPE, mock);
    assertEquals(1, factory.types().size());
    
    try {
      factory.registerIteratorFactory(null, mock);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.registerIteratorFactory(NumericType.TYPE, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void newIterator() throws Exception {
    TimeSeriesQuery q = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1970/01/01-00:00:01")
            .setEnd("1970/01/01-00:01:00")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build();
    
    QueryIteratorInterpolatorFactory interpolator_factory = 
        new NumericInterpolatorFactory.Default();
    NumericInterpolatorConfig factory_config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build();
    
    DownsampleConfig config = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .setQueryIteratorInterpolatorFactory(interpolator_factory)
        .setQueryIteratorInterpolatorConfig(factory_config)
        .build();
    
    final DownsampleFactory factory = new DownsampleFactory("Downsample");
    
    final NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(60000));
    source.add(30000, 42);
    final QueryNode node = mock(QueryNode.class);
    when(node.config()).thenReturn(config);
    
    Iterator<TimeSeriesValue<?>> iterator = factory.newIterator(
        NumericType.TYPE, node, ImmutableMap.<String, TimeSeries>builder()
        .put("a", source)
        .build());
    assertTrue(iterator.hasNext());
    
    try {
      factory.newIterator(null, node, ImmutableMap.<String, TimeSeries>builder()
          .put("a", source)
          .build());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, null, ImmutableMap.<String, TimeSeries>builder()
          .put("a", source)
          .build());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, node, (Map) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, node, Collections.emptyMap());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    iterator = factory.newIterator(NumericType.TYPE, node, 
        Lists.<TimeSeries>newArrayList(source));
    assertTrue(iterator.hasNext());
    
    try {
      factory.newIterator(null, node, Lists.<TimeSeries>newArrayList(source));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, null, 
          Lists.<TimeSeries>newArrayList(source));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, node, (List) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, node, Collections.emptyList());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
