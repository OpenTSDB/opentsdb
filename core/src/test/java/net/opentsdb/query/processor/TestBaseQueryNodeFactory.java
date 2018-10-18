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
package net.opentsdb.query.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Map;

import net.opentsdb.data.TypedTimeSeriesIterator;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.plan.QueryPlanner;

public class TestBaseQueryNodeFactory {

  @Test
  public void ctor() throws Exception {
    QueryNodeFactory factory = new MockNodeFactory();
    assertNull(factory.id());
    assertEquals(MockNodeFactory.TYPE, factory.type());
    
    factory.initialize(mock(TSDB.class), "Mock!");
    assertEquals("Mock!", factory.id());
  }
  
  @Test
  public void registerIteratorFactory() throws Exception {
    QueryIteratorFactory mock1 = mock(QueryIteratorFactory.class);
    QueryIteratorFactory mock2 = mock(QueryIteratorFactory.class);
    
    MockNodeFactory factory = new MockNodeFactory();
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
    TypedTimeSeriesIterator iterator = mock(TypedTimeSeriesIterator.class);
    QueryIteratorFactory mock1 = mock(QueryIteratorFactory.class);
    when(mock1.newIterator(any(QueryNode.class), any(QueryResult.class), 
        anyCollection(), any(TypeToken.class)))
      .thenReturn(iterator);
    QueryNode node = mock(QueryNode.class);
    MockNodeFactory factory = new MockNodeFactory();
    
    assertNull(factory.newTypedIterator(NumericType.TYPE, node, null,
        Lists.newArrayList(mock(TimeSeries.class))));
    
    factory.registerIteratorFactory(NumericType.TYPE, mock1);
    TypedTimeSeriesIterator from_factory =
        factory.newTypedIterator(NumericType.TYPE, node, null,
            Lists.<TimeSeries>newArrayList(mock(TimeSeries.class)));
    assertSame(iterator, from_factory);
    
    try {
      factory.newTypedIterator(null, node, null,
          Lists.newArrayList(mock(TimeSeries.class)));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newTypedIterator(NumericType.TYPE, null, null,
          Lists.newArrayList(mock(TimeSeries.class)));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newTypedIterator(NumericType.TYPE, node, null,(Collection) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newTypedIterator(NumericType.TYPE, node, null,Lists.newArrayList());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void newIteratorMap() throws Exception {
    TypedTimeSeriesIterator iterator = mock(TypedTimeSeriesIterator.class);
    Map<String, TimeSeries> sources = Maps.newHashMap();
    sources.put("a", mock(TimeSeries.class));
    QueryIteratorFactory mock1 = mock(QueryIteratorFactory.class);
    when(mock1.newIterator(any(QueryNode.class), any(QueryResult.class), 
        anyMap(), any(TypeToken.class)))
      .thenReturn(iterator);
    QueryNode node = mock(QueryNode.class);
    MockNodeFactory factory = new MockNodeFactory();
    
    assertNull(factory.newTypedIterator(NumericType.TYPE, node, null,sources));
    
    factory.registerIteratorFactory(NumericType.TYPE, mock1);
    TypedTimeSeriesIterator from_factory = 
        factory.newTypedIterator(NumericType.TYPE, node, null,sources);
    assertSame(iterator, from_factory);
    
    try {
      factory.newTypedIterator(null, node, null,sources);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newTypedIterator(NumericType.TYPE, null, null,sources);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newTypedIterator(NumericType.TYPE, node, null,(Map) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newTypedIterator(NumericType.TYPE, node, null,Maps.newHashMap());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  /** Mock class to test the abstract. */
  class MockNodeFactory extends BaseQueryNodeFactory {

    public static final String TYPE = "MockNodeFactory";
    
    @Override
    public QueryNode newNode(final QueryPipelineContext context,
                             final QueryNodeConfig config) {
      return mock(QueryNode.class);
    }

    @Override
    public QueryNode newNode(QueryPipelineContext context) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public QueryNodeConfig parseConfig(ObjectMapper mapper, TSDB tsdb,
        JsonNode node) {
      return null;
    }
    
    @Override
    public void setupGraph(
        final TimeSeriesQuery query, 
        final QueryNodeConfig config, 
        final QueryPlanner plan) {
      // TODO Auto-generated method stub
    }

    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String type() {
      return TYPE;
    }
  }
}
