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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.utils.UnitTestException;

public class TestByteToStringIdConverter {

  private QueryPipelineContext context;
  private QueryNode upstream;
  private ByteToStringIdConverterConfig config;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    upstream = mock(QueryNode.class);
    config = (ByteToStringIdConverterConfig)
        ByteToStringIdConverterConfig.newBuilder()
          .setId("cvtr")
          .build();
    
    when(context.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
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
  
}
