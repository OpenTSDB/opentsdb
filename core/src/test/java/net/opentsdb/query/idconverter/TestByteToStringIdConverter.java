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
package net.opentsdb.query.idconverter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.idconverter.ByteToStringConverterForSource.Resolver;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.UnitTestException;

public class TestByteToStringIdConverter {

  private QueryPipelineContext context;
  private QueryNode upstream;
  private ByteToStringIdConverterConfig config;
  private TimeSeriesDataSourceFactory factory_a;
  private TimeSeriesDataSourceFactory factory_b;
  private PartialTimeSeriesSet set_a;
  private PartialTimeSeriesSet set_b;
  
  @Before
  public void before() throws Exception {
    QueryContext qc = mock(QueryContext.class);
    when(qc.query()).thenReturn(mock(TimeSeriesQuery.class));
    context = spy(new TestByteToStringConverterForSource.TestContext(qc));
    upstream = mock(QueryNode.class);
    factory_a = mock(TimeSeriesDataSourceFactory.class);
    factory_b = mock(TimeSeriesDataSourceFactory.class);
    set_a = mock(PartialTimeSeriesSet.class);
    set_b = mock(PartialTimeSeriesSet.class);
    
    config = (ByteToStringIdConverterConfig)
        ByteToStringIdConverterConfig.newBuilder()
          .setId("cvtr")
          .build();
    
    doReturn(Lists.newArrayList(upstream)).when(context)
      .upstream(any(QueryNode.class));
    doReturn(Lists.newArrayList()).when(context)
      .downstream(any(QueryNode.class));
    doReturn(Lists.newArrayList()).when(context)
      .downstreamSources(any(QueryNode.class));
    when(set_a.start()).thenReturn(new SecondTimeStamp(1546300800));
    when(set_a.dataSource()).thenReturn("m1");
    when(set_b.start()).thenReturn(new SecondTimeStamp(1546300800));
    when(set_b.dataSource()).thenReturn("m2");
  }
  
  @Test
  public void ctor() throws Exception {
    ByteToStringIdConverter node = new ByteToStringIdConverter(
        mock(QueryNodeFactory.class), 
        context, 
        config);
    assertSame(config, node.config());
    assertSame(context, node.pipelineContext());
  }
  
  @Test
  public void onNextStringResult() throws Exception {
    QueryResult result = mock(QueryResult.class);
    when(result.idType()).thenAnswer(new Answer<TypeToken<? extends TimeSeriesId>>() {
      @Override
      public TypeToken<? extends TimeSeriesId> answer(
          InvocationOnMock invocation) throws Throwable {
        return Const.TS_STRING_ID;
      }
    });
    
    ByteToStringIdConverter node = new ByteToStringIdConverter(
        mock(QueryNodeFactory.class), 
        context, 
        config);
    node.initialize(null).join(250);
    node.onNext(result);
    verify(upstream, times(1)).onNext(result);
  }
  
  @Test
  public void onNextByteEmptyResult() throws Exception {
    QueryResult result = mock(QueryResult.class);
    when(result.idType()).thenAnswer(new Answer<TypeToken<? extends TimeSeriesId>>() {
      @Override
      public TypeToken<? extends TimeSeriesId> answer(
          InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    
    ByteToStringIdConverter node = new ByteToStringIdConverter(
        mock(QueryNodeFactory.class), 
        context, 
        config);
    node.initialize(null).join(250);
    node.onNext(result);
    verify(upstream, times(1)).onNext(result);
  }
  
  @Test
  public void onNextByteResult() throws Exception {
    QueryResult result = mock(QueryResult.class);
    when(result.idType()).thenAnswer(new Answer<TypeToken<? extends TimeSeriesId>>() {
      @Override
      public TypeToken<? extends TimeSeriesId> answer(
          InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    
    TimeSeries ts1 = mock(TimeSeries.class);
    TimeSeries ts2 = mock(TimeSeries.class);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2));
    
    TimeSeriesByteId bid1 = mock(TimeSeriesByteId.class);
    TimeSeriesByteId bid2 = mock(TimeSeriesByteId.class);
    
    when(ts1.id()).thenReturn(bid1);
    when(ts2.id()).thenReturn(bid2);
    
    TimeSeriesDataSourceFactory factory = mock(TimeSeriesDataSourceFactory.class);
    when(bid1.dataStore()).thenReturn(factory);
    when(bid2.dataStore()).thenReturn(factory);
    
    TimeSeriesStringId sid1 = mock(TimeSeriesStringId.class);
    TimeSeriesStringId sid2 = mock(TimeSeriesStringId.class);
    
    when(factory.resolveByteId(bid1, null)).thenReturn(
        Deferred.fromResult(sid1));
    when(factory.resolveByteId(bid2, null)).thenReturn(
        Deferred.fromResult(sid2));
    
    QueryResult[] from_upstream = new QueryResult[1];
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        from_upstream[0] = (QueryResult) invocation.getArguments()[0];
        return null;
      }
    }).when(upstream).onNext(any(QueryResult.class));
    
    ByteToStringIdConverter node = new ByteToStringIdConverter(
        mock(QueryNodeFactory.class), 
        context, 
        config);
    node.initialize(null).join(250);
    node.onNext(result);
    verify(upstream, never()).onNext(result);
    verify(upstream, never()).onError(any(Throwable.class));
    
    assertEquals(2, from_upstream[0].timeSeries().size());
    assertEquals(Const.TS_STRING_ID, from_upstream[0].idType());
    Iterator<TimeSeries> iterator = from_upstream[0].timeSeries().iterator();
    TimeSeries series = iterator.next();
    assertSame(sid1, series.id());
    series = iterator.next();
    assertSame(sid2, series.id());
  }
  
  @Test
  public void onNextByteResultException() throws Exception {
    QueryResult result = mock(QueryResult.class);
    when(result.idType()).thenAnswer(new Answer<TypeToken<? extends TimeSeriesId>>() {
      @Override
      public TypeToken<? extends TimeSeriesId> answer(
          InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    
    TimeSeries ts1 = mock(TimeSeries.class);
    TimeSeries ts2 = mock(TimeSeries.class);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2));
    
    TimeSeriesByteId bid1 = mock(TimeSeriesByteId.class);
    TimeSeriesByteId bid2 = mock(TimeSeriesByteId.class);
    
    when(ts1.id()).thenReturn(bid1);
    when(ts2.id()).thenReturn(bid2);
    
    TimeSeriesDataSourceFactory factory = mock(TimeSeriesDataSourceFactory.class);
    when(bid1.dataStore()).thenReturn(factory);
    when(bid2.dataStore()).thenReturn(factory);
    
    TimeSeriesStringId sid1 = mock(TimeSeriesStringId.class);
    
    when(factory.resolveByteId(bid1, null)).thenReturn(
        Deferred.fromResult(sid1));
    when(factory.resolveByteId(bid2, null)).thenReturn(
        Deferred.fromError(new UnitTestException()));
    
    QueryResult[] from_upstream = new QueryResult[1];
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        from_upstream[0] = (QueryResult) invocation.getArguments()[0];
        return null;
      }
    }).when(upstream).onNext(any(QueryResult.class));
    
    ByteToStringIdConverter node = new ByteToStringIdConverter(
        mock(QueryNodeFactory.class), 
        context, 
        config);
    node.initialize(null).join(250);
    node.onNext(result);
    verify(upstream, never()).onNext(result);
    verify(upstream, times(1)).onError(any(UnitTestException.class));
    
    assertNull(from_upstream[0]);
  }
  
  @Test
  public void onNextPTSEmptyTimeSeries() throws Exception {
    config = (ByteToStringIdConverterConfig)
        ByteToStringIdConverterConfig.newBuilder()
          .addDataSource("m1", factory_a)
          .addDataSource("m2", factory_b)
          .setId("cvtr")
          .build();
    
    ByteToStringIdConverter node = new ByteToStringIdConverter(
        mock(QueryNodeFactory.class), 
        context, 
        config);
    node.initialize(null).join();
    
    PartialTimeSeries pts_a = mock(PartialTimeSeries.class);
    TimeSeriesByteId id_a = getByteId(42, pts_a, set_a, factory_a);
    node.onNext(pts_a);
    verify(upstream, times(1)).onNext(pts_a);
    verify(upstream, never()).onError(any(Throwable.class));
    assertTrue(node.converters.isEmpty());
  }
  
  @Test
  public void onNextPTSNotListed() throws Exception {
    config = (ByteToStringIdConverterConfig)
        ByteToStringIdConverterConfig.newBuilder()
          .addDataSource("m1", factory_a)
          .addDataSource("m2", factory_b)
          .setId("cvtr")
          .build();
    
    ByteToStringIdConverter node = new ByteToStringIdConverter(
        mock(QueryNodeFactory.class), 
        context, 
        config);
    node.initialize(null).join();
    
    PartialTimeSeries pts_a = mock(PartialTimeSeries.class);
    when(set_a.timeSeriesCount()).thenReturn(100);
    when(set_a.dataSource()).thenReturn("m3");
    TimeSeriesByteId id_a = getByteId(42, pts_a, set_a, factory_a);
    node.onNext(pts_a);
    verify(upstream, times(1)).onNext(pts_a);
    verify(upstream, never()).onError(any(Throwable.class));
    assertTrue(node.converters.isEmpty());
  }
  
  @Test
  public void onNextPTS() throws Exception {
    config = (ByteToStringIdConverterConfig)
        ByteToStringIdConverterConfig.newBuilder()
          .addDataSource("m1", factory_a)
          .addDataSource("m2", factory_b)
          .setId("cvtr")
          .build();
    
    ByteToStringIdConverter node = new ByteToStringIdConverter(
        mock(QueryNodeFactory.class), 
        context, 
        config);
    node.initialize(null).join();
    
    PartialTimeSeries pts_a = mock(PartialTimeSeries.class);
    when(pts_a.idType()).thenAnswer(new Answer<TypeToken>() {
      @Override
      public TypeToken answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(set_a.timeSeriesCount()).thenReturn(100);
    TimeSeriesByteId id_a = getByteId(42, pts_a, set_a, factory_a);
    node.onNext(pts_a);
    verify(upstream, never()).onNext(pts_a);
    verify(upstream, never()).onError(any(Throwable.class));
    assertEquals(1, node.converters.size());
    ByteToStringConverterForSource src = node.converters.get("m1");
    Resolver resolver = src.resolvers.get(42L);
    assertSame(pts_a, resolver.series.get(0));
    
    PartialTimeSeries pts_b = mock(PartialTimeSeries.class);
    when(pts_b.idType()).thenAnswer(new Answer<TypeToken>() {
      @Override
      public TypeToken answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(set_b.timeSeriesCount()).thenReturn(100);
    TimeSeriesByteId id_b = getByteId(-1, pts_b, set_b, factory_b);
    node.onNext(pts_b);
    verify(upstream, never()).onNext(pts_a);
    verify(upstream, never()).onNext(pts_b);
    verify(upstream, never()).onError(any(Throwable.class));
    assertEquals(2, node.converters.size());
    src = node.converters.get("m1");
    resolver = src.resolvers.get(42L);
    assertSame(pts_a, resolver.series.get(0));
    
    src = node.converters.get("m2");
    resolver = src.resolvers.get(-1L);
    assertSame(pts_b, resolver.series.get(0));
  }
  
  TimeSeriesByteId getByteId(final long hash, 
                             final PartialTimeSeries pts, 
                             final PartialTimeSeriesSet set,
                             final TimeSeriesDataSourceFactory factory) {
    TimeSeriesByteId id = mock(TimeSeriesByteId.class);
    when(id.type()).thenAnswer(new Answer<TypeToken>() {
      @Override
      public TypeToken answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    if (!context.hasId(hash, Const.TS_BYTE_ID)) {
      context.addId(hash, id);
    }
    when(id.dataStore()).thenReturn(factory);
    when(factory.resolveByteId(any(TimeSeriesByteId.class), any(Span.class)))
      .thenReturn(new Deferred<TimeSeriesStringId>());
    when(pts.set()).thenReturn(set);
    when(pts.idHash()).thenReturn(hash);
    return id;
  }
}
