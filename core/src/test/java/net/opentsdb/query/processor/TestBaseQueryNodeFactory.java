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
package net.opentsdb.query.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;

public class TestBaseQueryNodeFactory {

  @Test
  public void ctor() throws Exception {
    QueryNodeFactory factory = new MockNodeFactory("Mock!");
    assertEquals("Mock!", factory.id());
    assertTrue(factory.types().isEmpty());
    
    try {
      new MockNodeFactory(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MockNodeFactory("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void registerIteratorFactory() throws Exception {
    QueryIteratorFactory mock1 = mock(QueryIteratorFactory.class);
    QueryIteratorFactory mock2 = mock(QueryIteratorFactory.class);
    
    MockNodeFactory factory = new MockNodeFactory("Mock!");
    factory.registerIteratorFactory(NumericType.TYPE, mock1);
    
    assertEquals(1, factory.types().size());
    assertSame(mock1, factory.iterator_factories.get(NumericType.TYPE));
    
    factory.registerIteratorFactory(AnnotationType.TYPE, mock2);
    assertEquals(2, factory.types().size());
    assertSame(mock1, factory.iterator_factories.get(NumericType.TYPE));
    assertSame(mock2, factory.iterator_factories.get(AnnotationType.TYPE));
    
    // replace
    factory.registerIteratorFactory(NumericType.TYPE, mock2);
    assertEquals(2, factory.types().size());
    assertSame(mock2, factory.iterator_factories.get(NumericType.TYPE));
    assertSame(mock2, factory.iterator_factories.get(AnnotationType.TYPE));
    
    try {
      factory.registerIteratorFactory(null, mock2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.registerIteratorFactory(NumericType.TYPE, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void newIteratorList() throws Exception {
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = 
        mock(Iterator.class);
    QueryIteratorFactory mock1 = mock(QueryIteratorFactory.class);
    when(mock1.newIterator(any(QueryNode.class), anyCollection()))
      .thenReturn((Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>) iterator);
    QueryNode node = mock(QueryNode.class);
    MockNodeFactory factory = new MockNodeFactory("Mock!");
    
    assertNull(factory.newIterator(NumericType.TYPE, node, 
        Lists.newArrayList(mock(TimeSeries.class))));
    
    factory.registerIteratorFactory(NumericType.TYPE, mock1);
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> from_factory = 
        factory.newIterator(NumericType.TYPE, node, 
            Lists.<TimeSeries>newArrayList(mock(TimeSeries.class)));
    assertSame(iterator, from_factory);
    
    try {
      factory.newIterator(null, node, 
          Lists.newArrayList(mock(TimeSeries.class)));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, null, 
          Lists.newArrayList(mock(TimeSeries.class)));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, node, (Collection) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, node, Lists.newArrayList());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void newIteratorMap() throws Exception {
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = 
        mock(Iterator.class);
    Map<String, TimeSeries> sources = Maps.newHashMap();
    sources.put("a", mock(TimeSeries.class));
    QueryIteratorFactory mock1 = mock(QueryIteratorFactory.class);
    when(mock1.newIterator(any(QueryNode.class), anyMap()))
      .thenReturn((Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>) iterator);
    QueryNode node = mock(QueryNode.class);
    MockNodeFactory factory = new MockNodeFactory("Mock!");
    
    assertNull(factory.newIterator(NumericType.TYPE, node, sources));
    
    factory.registerIteratorFactory(NumericType.TYPE, mock1);
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> from_factory = 
        factory.newIterator(NumericType.TYPE, node, sources);
    assertSame(iterator, from_factory);
    
    try {
      factory.newIterator(null, node, sources);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, null, sources);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, node, (Map) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, node, Maps.newHashMap());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  /** Mock class to test the abstract. */
  class MockNodeFactory extends BaseQueryNodeFactory {

    public MockNodeFactory(final String id) {
      super(id);
    }

    @Override
    public QueryNode newNode(final QueryPipelineContext context,
                             final QueryNodeConfig config) {
      return mock(QueryNode.class);
    }
    
  }
}
