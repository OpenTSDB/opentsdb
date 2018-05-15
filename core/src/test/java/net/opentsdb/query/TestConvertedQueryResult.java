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
package net.opentsdb.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.UnitTestException;

public class TestConvertedQueryResult {

  private QueryResult result;
  private List<TimeSeries> sources;
  private TimeSeriesByteId id1;
  private TimeSeriesByteId id2;
  
  @Test
  public void convertNode() throws Exception {
    QueryNode node = mock(QueryNode.class);
    
    try {
      ConvertedQueryResult.convert(null, node, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ConvertedQueryResult.convert(result, (QueryNode) null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    setByteIds();
    final boolean[] validated = new boolean[1];
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        final QueryResult converted = (QueryResult) invocation.getArguments()[0];
        assertEquals(2, converted.timeSeries().size());
        Iterator<TimeSeries> it = converted.timeSeries().iterator();
        int i = 0;
        while (it.hasNext()) {
          verify((TimeSeriesByteId) sources.get(0).id(), times(1)).decode(false, null);
          TimeSeries ts = it.next();
          assertEquals("sys.cpu", ((TimeSeriesStringId) ts.id()).metric());
          if (i++ == 0) {
            assertEquals("web01", ((TimeSeriesStringId) ts.id()).tags().get("host"));
          } else {
            assertEquals("web02", ((TimeSeriesStringId) ts.id()).tags().get("host"));
          }
        }
        validated[0] = true;
        return null;
      }
    }).when(node).onNext(any(QueryResult.class));
    ConvertedQueryResult.convert(result, node, null);
    assertTrue(validated[0]);
    
    // return exception
    setByteIds();
    when(((TimeSeriesByteId) sources.get(0).id()).decode(false, null))
      .thenReturn(Deferred.fromError(new UnitTestException()));
    node = mock(QueryNode.class);
    ConvertedQueryResult.convert(result, node, null);
    verify(node, times(1)).onError(any(Throwable.class));
    
    // throw exception
    setByteIds();
    when(((TimeSeriesByteId) sources.get(0).id()).decode(false, null))
      .thenThrow(new UnitTestException());
    node = mock(QueryNode.class);
    ConvertedQueryResult.convert(result, node, null);
    verify(node, times(1)).onError(any(Throwable.class));
    
    // no conversion happens here.
    validated[0] = false;
    setStringIds();
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        final QueryResult converted = (QueryResult) invocation.getArguments()[0];
        assertEquals(2, converted.timeSeries().size());
        Iterator<TimeSeries> it = converted.timeSeries().iterator();
        int i = 0;
        while (it.hasNext()) {
          TimeSeries ts = it.next();
          assertEquals("sys.cpu", ((TimeSeriesStringId) ts.id()).metric());
          if (i++ == 0) {
            assertEquals("web01", ((TimeSeriesStringId) ts.id()).tags().get("host"));
          } else {
            assertEquals("web02", ((TimeSeriesStringId) ts.id()).tags().get("host"));
          }
        }
        validated[0] = true;
        return null;
      }
    }).when(node).onNext(any(QueryResult.class));
    ConvertedQueryResult.convert(result, node, null);
    assertTrue(validated[0]);
  }
  
  @Test
  public void convertSink() throws Exception {
    QuerySink sink = mock(QuerySink.class);
    
    try {
      ConvertedQueryResult.convert(null, sink, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ConvertedQueryResult.convert(result, (QuerySink) null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    setByteIds();
    final boolean[] validated = new boolean[1];
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        final QueryResult converted = (QueryResult) invocation.getArguments()[0];
        assertEquals(2, converted.timeSeries().size());
        Iterator<TimeSeries> it = converted.timeSeries().iterator();
        int i = 0;
        while (it.hasNext()) {
          verify((TimeSeriesByteId) sources.get(0).id(), times(1)).decode(false, null);
          TimeSeries ts = it.next();
          assertEquals("sys.cpu", ((TimeSeriesStringId) ts.id()).metric());
          if (i++ == 0) {
            assertEquals("web01", ((TimeSeriesStringId) ts.id()).tags().get("host"));
          } else {
            assertEquals("web02", ((TimeSeriesStringId) ts.id()).tags().get("host"));
          }
        }
        validated[0] = true;
        return null;
      }
    }).when(sink).onNext(any(QueryResult.class));
    ConvertedQueryResult.convert(result, sink, null);
    assertTrue(validated[0]);
    
    // return exception
    setByteIds();
    when(((TimeSeriesByteId) sources.get(0).id()).decode(false, null))
      .thenReturn(Deferred.fromError(new UnitTestException()));
    sink = mock(QuerySink.class);
    ConvertedQueryResult.convert(result, sink, null);
    verify(sink, times(1)).onError(any(Throwable.class));
    
    // throw exception
    setByteIds();
    when(((TimeSeriesByteId) sources.get(0).id()).decode(false, null))
      .thenThrow(new UnitTestException());
    sink = mock(QuerySink.class);
    ConvertedQueryResult.convert(result, sink, null);
    verify(sink, times(1)).onError(any(Throwable.class));
    
    // no conversion happens here.
    validated[0] = false;
    setStringIds();
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        final QueryResult converted = (QueryResult) invocation.getArguments()[0];
        assertEquals(2, converted.timeSeries().size());
        Iterator<TimeSeries> it = converted.timeSeries().iterator();
        int i = 0;
        while (it.hasNext()) {
          TimeSeries ts = it.next();
          assertEquals("sys.cpu", ((TimeSeriesStringId) ts.id()).metric());
          if (i++ == 0) {
            assertEquals("web01", ((TimeSeriesStringId) ts.id()).tags().get("host"));
          } else {
            assertEquals("web02", ((TimeSeriesStringId) ts.id()).tags().get("host"));
          }
        }
        validated[0] = true;
        return null;
      }
    }).when(sink).onNext(any(QueryResult.class));
    ConvertedQueryResult.convert(result, sink, null);
    assertTrue(validated[0]);
  }
  
  private void setByteIds() throws Exception {
    sources = Lists.newArrayList();
    TimeSeries ts1 = mock(TimeSeries.class);
    TimeSeries ts2 = mock(TimeSeries.class);
    
    id1 = mock(TimeSeriesByteId.class);
    id2 = mock(TimeSeriesByteId.class);
    
    when(ts1.id()).thenReturn(id1);
    when(ts2.id()).thenReturn(id2);
    
    TimeSeriesStringId sid1 = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu")
        .addTags("host", "web01")
        .build();
    TimeSeriesStringId sid2 = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu")
        .addTags("host", "web02")
        .build();
    
    when(id1.decode(anyBoolean(), any(Span.class)))
      .thenReturn(Deferred.fromResult(sid1));
    when(id2.decode(anyBoolean(), any(Span.class)))
      .thenReturn(Deferred.fromResult(sid2));
    
    sources.add(ts1);
    sources.add(ts2);
    
    result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(sources);
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
  }
  
  private void setStringIds() throws Exception {
    sources = Lists.newArrayList();
    TimeSeries ts1 = mock(TimeSeries.class);
    TimeSeries ts2 = mock(TimeSeries.class);
    
    id1 = mock(TimeSeriesByteId.class);
    id2 = mock(TimeSeriesByteId.class);
    
    TimeSeriesStringId sid1 = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu")
        .addTags("host", "web01")
        .build();
    TimeSeriesStringId sid2 = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu")
        .addTags("host", "web02")
        .build();
    
    when(ts1.id()).thenReturn(sid1);
    when(ts2.id()).thenReturn(sid2);
    
    sources.add(ts1);
    sources.add(ts2);
    
    result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(sources);
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_STRING_ID;
      }
    });
  }
}
