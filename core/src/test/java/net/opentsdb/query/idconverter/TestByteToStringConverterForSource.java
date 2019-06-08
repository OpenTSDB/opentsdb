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
package net.opentsdb.query.idconverter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

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
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.query.AbstractQueryPipelineContext;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.idconverter.ByteToStringConverterForSource.Resolver;
import net.opentsdb.query.idconverter.ByteToStringConverterForSource.WrappedPartialTimeSeries;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.UnitTestException;

public class TestByteToStringConverterForSource {

  private TestContext context;
  private ByteToStringIdConverter converter;
  private TimeSeriesDataSourceFactory factory;
  private List<PartialTimeSeries> sent_up;
  private PartialTimeSeriesSet set_a;
  private PartialTimeSeriesSet set_b;
  private PartialTimeSeriesSet set_c;
  
  @Before
  public void before() throws Exception {
    QueryContext qc = mock(QueryContext.class);
    when(qc.query()).thenReturn(mock(TimeSeriesQuery.class));
    context = new TestContext(qc);
    converter = mock(ByteToStringIdConverter.class);
    factory = mock(TimeSeriesDataSourceFactory.class);
    sent_up = Lists.newArrayList();
    set_a = mock(PartialTimeSeriesSet.class);
    set_b = mock(PartialTimeSeriesSet.class);
    set_c = mock(PartialTimeSeriesSet.class);
    when(set_a.start()).thenReturn(new SecondTimeStamp(1546300800));
    when(set_b.start()).thenReturn(new SecondTimeStamp(1546304400));
    when(set_c.start()).thenReturn(new SecondTimeStamp(1546308000));
    when(converter.pipelineContext()).thenReturn(context);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        sent_up.add((PartialTimeSeries) invocation.getArguments()[0]);
        return null;
      }
    }).when(converter).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void ctor() throws Exception {
    ByteToStringConverterForSource source = 
        new ByteToStringConverterForSource(converter);
    assertSame(source.converter, converter);
    assertTrue(source.sets.isEmpty());
    assertTrue(source.resolvers.isEmpty());
  }
  
  @Test
  public void resolveOneId() throws Exception {
    ByteToStringConverterForSource source = 
        new ByteToStringConverterForSource(converter);
    PartialTimeSeries pts_a = mock(PartialTimeSeries.class);
    TimeSeriesByteId id_a = getByteId(42, pts_a, set_a);
    source.resolve(pts_a);
    
    assertNull(context.ids().get(Const.TS_STRING_ID));
    assertTrue(source.sets.isEmpty());
    assertEquals(1, source.resolvers.size());
    
    Resolver resolver = source.resolvers.get(42L);
    assertFalse(resolver.resolved.get());
    assertEquals(1, resolver.series.size());
    assertNotNull(resolver.deferred);
    assertTrue(sent_up.isEmpty());
    
    // second series while waiting for the response.
    PartialTimeSeries pts_b = mock(PartialTimeSeries.class);
    TimeSeriesByteId id_b = getByteId(42, pts_b, set_b);
    source.resolve(pts_b);
    
    assertNull(context.ids().get(Const.TS_STRING_ID));
    assertTrue(source.sets.isEmpty());
    assertEquals(1, source.resolvers.size());
    
    assertFalse(resolver.resolved.get());
    assertEquals(2, resolver.series.size());
    assertNotNull(resolver.deferred);
    assertTrue(sent_up.isEmpty());
    verify(factory, times(1)).resolveByteId(id_a, null);
    verify(factory, never()).resolveByteId(id_b, null);
    
    // resolve it!
    TimeSeriesStringId decoded = mock(TimeSeriesStringId.class);
    when(decoded.type()).thenAnswer(new Answer<TypeToken>() {
      @Override
      public TypeToken answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_STRING_ID;
      }
    });
    resolver.deferred.callback(decoded);
    
    assertEquals(1, context.ids().get(Const.TS_STRING_ID).size());
    assertSame(decoded, context.getId(42L, Const.TS_STRING_ID));
    assertEquals(2, source.sets.size());
    assertTrue(source.resolvers.isEmpty());
    assertEquals(2, sent_up.size());
    verify(factory, times(1)).resolveByteId(id_a, null);
    verify(factory, never()).resolveByteId(id_b, null);
    
    // third hits the decoded IDs.
    PartialTimeSeries pts_c = mock(PartialTimeSeries.class);
    TimeSeriesByteId id_c = getByteId(42, pts_c, set_c);
    source.resolve(pts_c);
    
    assertEquals(1, context.ids().get(Const.TS_STRING_ID).size());
    assertSame(decoded, context.getId(42L, Const.TS_STRING_ID));
    assertEquals(3, source.sets.size());
    assertTrue(source.resolvers.isEmpty());
    assertEquals(3, sent_up.size());
    verify(factory, times(1)).resolveByteId(id_a, null);
    verify(factory, never()).resolveByteId(id_b, null);
    verify(factory, never()).resolveByteId(id_c, null);
    
    assertSame(pts_a, ((WrappedPartialTimeSeries) sent_up.get(0)).source);
    assertTrue(set_a.start().compare(Op.EQ, 
        ((WrappedPartialTimeSeries) sent_up.get(0)).set().start()));
    assertSame(pts_b, ((WrappedPartialTimeSeries) sent_up.get(1)).source);
    assertTrue(set_b.start().compare(Op.EQ, 
        ((WrappedPartialTimeSeries) sent_up.get(1)).set().start()));
    assertSame(pts_c, ((WrappedPartialTimeSeries) sent_up.get(2)).source);
    assertTrue(set_c.start().compare(Op.EQ, 
        ((WrappedPartialTimeSeries) sent_up.get(2)).set().start()));
  }
  
  @Test
  public void resolveMultipleIds() throws Exception {
    ByteToStringConverterForSource source = 
        new ByteToStringConverterForSource(converter);
    PartialTimeSeries pts_a = mock(PartialTimeSeries.class);
    TimeSeriesByteId id_a = getByteId(42, pts_a, set_a);
    source.resolve(pts_a);
    
    assertNull(context.ids().get(Const.TS_STRING_ID));
    assertTrue(source.sets.isEmpty());
    assertEquals(1, source.resolvers.size());
    
    Resolver resolver = source.resolvers.get(42L);
    assertFalse(resolver.resolved.get());
    assertEquals(1, resolver.series.size());
    assertNotNull(resolver.deferred);
    assertTrue(sent_up.isEmpty());
    
    // diff series, same set
    PartialTimeSeries pts_b = mock(PartialTimeSeries.class);
    TimeSeriesByteId id_b = getByteId(-1, pts_b, set_a);
    source.resolve(pts_b);
    
    assertNull(context.ids().get(Const.TS_STRING_ID));
    assertTrue(source.sets.isEmpty());
    assertEquals(2, source.resolvers.size());
    
    assertFalse(source.resolvers.get(42L).resolved.get());
    assertEquals(1, source.resolvers.get(42L).series.size());
    assertNotNull(source.resolvers.get(42L).deferred);
    assertFalse(source.resolvers.get(-1L).resolved.get());
    assertEquals(1, source.resolvers.get(-1L).series.size());
    assertNotNull(source.resolvers.get(-1L).deferred);
    assertTrue(sent_up.isEmpty());
    verify(factory, times(1)).resolveByteId(id_a, null);
    verify(factory, times(1)).resolveByteId(id_b, null);
    
    // resolve the first one
    TimeSeriesStringId decoded_a = mock(TimeSeriesStringId.class);
    when(decoded_a.type()).thenAnswer(new Answer<TypeToken>() {
      @Override
      public TypeToken answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_STRING_ID;
      }
    });
    source.resolvers.get(42L).deferred.callback(decoded_a);
    
    assertEquals(1, context.ids().get(Const.TS_STRING_ID).size());
    assertSame(decoded_a, context.getId(42L, Const.TS_STRING_ID));
    assertEquals(1, source.sets.size());
    assertEquals(1, source.resolvers.size());
    assertEquals(1, sent_up.size());
    verify(factory, times(1)).resolveByteId(id_a, null);
    verify(factory, times(1)).resolveByteId(id_b, null);
    
    // resolve the first one
    TimeSeriesStringId decoded_b = mock(TimeSeriesStringId.class);
    when(decoded_b.type()).thenAnswer(new Answer<TypeToken>() {
      @Override
      public TypeToken answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_STRING_ID;
      }
    });
    source.resolvers.get(-1L).deferred.callback(decoded_b);
    
    assertEquals(2, context.ids().get(Const.TS_STRING_ID).size());
    assertSame(decoded_a, context.getId(42L, Const.TS_STRING_ID));
    assertSame(decoded_b, context.getId(-1L, Const.TS_STRING_ID));
    assertEquals(1, source.sets.size());
    assertTrue(source.resolvers.isEmpty());
    assertEquals(2, sent_up.size());
    verify(factory, times(1)).resolveByteId(id_a, null);
    verify(factory, times(1)).resolveByteId(id_b, null);
    
    // followup hits decoded
    PartialTimeSeries pts_c = mock(PartialTimeSeries.class);
    TimeSeriesByteId id_c = getByteId(42, pts_c, set_c);
    source.resolve(pts_c);
    
    assertEquals(2, context.ids().get(Const.TS_STRING_ID).size());
    assertEquals(2, source.sets.size());
    assertTrue(source.resolvers.isEmpty());
    assertEquals(3, sent_up.size());
    verify(factory, times(1)).resolveByteId(id_a, null);
    verify(factory, times(1)).resolveByteId(id_b, null);
    verify(factory, never()).resolveByteId(id_c, null);
    
    assertSame(pts_a, ((WrappedPartialTimeSeries) sent_up.get(0)).source);
    assertTrue(set_a.start().compare(Op.EQ, 
        ((WrappedPartialTimeSeries) sent_up.get(0)).set().start()));
    assertSame(pts_b, ((WrappedPartialTimeSeries) sent_up.get(1)).source);
    assertTrue(set_a.start().compare(Op.EQ, 
        ((WrappedPartialTimeSeries) sent_up.get(1)).set().start()));
    assertSame(pts_c, ((WrappedPartialTimeSeries) sent_up.get(2)).source);
    assertTrue(set_c.start().compare(Op.EQ, 
        ((WrappedPartialTimeSeries) sent_up.get(2)).set().start()));
  }
  
  @Test
  public void resolveException() throws Exception {
    ByteToStringConverterForSource source = 
        new ByteToStringConverterForSource(converter);
    PartialTimeSeries pts_a = mock(PartialTimeSeries.class);
    TimeSeriesByteId id_a = getByteId(42, pts_a, set_a);
    source.resolve(pts_a);
    
    assertNull(context.ids().get(Const.TS_STRING_ID));
    assertTrue(source.sets.isEmpty());
    assertEquals(1, source.resolvers.size());
    
    Resolver resolver = source.resolvers.get(42L);
    assertFalse(resolver.resolved.get());
    assertEquals(1, resolver.series.size());
    assertNotNull(resolver.deferred);
    assertTrue(sent_up.isEmpty());
    
    // second series while waiting for the response.
    PartialTimeSeries pts_b = mock(PartialTimeSeries.class);
    TimeSeriesByteId id_b = getByteId(42, pts_b, set_b);
    source.resolve(pts_b);
    
    assertNull(context.ids().get(Const.TS_STRING_ID));
    assertTrue(source.sets.isEmpty());
    assertEquals(1, source.resolvers.size());
    
    assertFalse(resolver.resolved.get());
    assertEquals(2, resolver.series.size());
    assertNotNull(resolver.deferred);
    assertTrue(sent_up.isEmpty());
    verify(factory, times(1)).resolveByteId(id_a, null);
    verify(factory, never()).resolveByteId(id_b, null);
    
    // throw the exception
    resolver.deferred.callback(new UnitTestException());
    
    assertNull(context.ids().get(Const.TS_STRING_ID));
    assertTrue(source.sets.isEmpty());
    assertTrue(source.resolvers.isEmpty());
    
    assertTrue(resolver.resolved.get());
    assertNull(resolver.series);
    assertNull(resolver.deferred);
    assertTrue(sent_up.isEmpty());
    verify(factory, times(1)).resolveByteId(id_a, null);
    verify(factory, never()).resolveByteId(id_b, null);
    verify(converter, times(1)).onError(any(UnitTestException.class));
  }
  
  TimeSeriesByteId getByteId(final long hash, 
                             final PartialTimeSeries pts, 
                             final PartialTimeSeriesSet set) {
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
    when(pts.idType()).thenAnswer(new Answer<TypeToken>() {
      @Override
      public TypeToken answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(pts.idHash()).thenReturn(hash);
    return id;
  }

  static class TestContext extends AbstractQueryPipelineContext {

    public TestContext(final QueryContext context) {
      super(context);
    }

    @Override
    public Deferred<Void> initialize(final Span span) {
      return Deferred.fromResult(null);
    }
    
    public Map<TypeToken<? extends TimeSeriesId>, Map<Long, TimeSeriesId>> ids() {
      return ids;
    }
    
  }
}
