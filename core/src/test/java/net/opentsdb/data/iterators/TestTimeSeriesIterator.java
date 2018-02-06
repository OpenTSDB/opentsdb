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
package net.opentsdb.data.iterators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;

public class TestTimeSeriesIterator {

  private TimeSeriesId id;
  private TimeSeriesIterator<?> source;
  private TimeSeriesIterator<?> source_clone;
  private QueryContext context;
  private TypeToken<?> type;
  
  @Before
  public void before() throws Exception {
    id = mock(TimeSeriesId.class);
    source = mock(TimeSeriesIterator.class);
    context = mock(QueryContext.class);
    type = TypeToken.of(NumericType.class);
    source_clone = mock(TimeSeriesIterator.class);
    
    when(source.id()).thenReturn(id);
    when(source.order()).thenReturn(42);
    when(source.initialize()).thenReturn(Deferred.fromResult(null));
    when(source.fetchNext()).thenReturn(Deferred.fromResult(null));
    when(source.close()).thenReturn(Deferred.fromResult(null));
    when(source.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return type;
      }
    });
    when(source.getShallowCopy(any(QueryContext.class)))
      .thenAnswer(new Answer<TimeSeriesIterator<?>>() {
        @Override
        public TimeSeriesIterator<?> answer(InvocationOnMock invocation)
            throws Throwable {
          return source_clone;
        }
      });
  }
  
  @Test
  public void ctor() throws Exception {
    MockIterator it = new MockIterator(id);
    assertSame(id, it.id());
    assertEquals(-1, it.order());
    assertNull(it.source);
    assertNull(it.context);
    assertNull(it.parent);
    
    try {
      new MockIterator(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void ctorContext() throws Exception {
    MockIterator it = new MockIterator(id, context);
    assertNull(it.source);
    assertSame(context, it.context);
    assertNull(it.parent);
    verify(context, times(1)).register(it);
    
    it = new MockIterator(id);
    assertNull(it.source);
    assertNull(it.context);
    assertNull(it.parent);
  }
  
  @Test
  public void ctorSource() throws Exception {
    MockIterator it = new MockIterator(id, context);
    assertSame(id, it.id());
    assertEquals(-1, it.order());
    assertNull(it.source);
    assertSame(context, it.context);
    assertNull(it.parent);
    verify(context, times(1)).register(it);
    
    MockIterator child = new MockIterator(context, it);
    assertSame(id, child.id());
    assertEquals(-1, child.order());
    assertSame(it, child.source);
    assertSame(context, child.context);
    assertSame(child, it.parent);
    verify(context, times(1)).register(child);
    
    child = new MockIterator(null, it);
    assertSame(it, child.source);
    assertNull(child.context);
    assertSame(child, it.parent);
    verify(context, never()).register(child);
    
    try {
      new MockIterator(context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    type = TypeToken.of(AnnotationType.class);
    try {
      new MockIterator(context, source);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void initialize() throws Exception {
    MockIterator it = new MockIterator(context, source);
    Deferred<Object> deferred = it.initialize();
    assertNull(deferred.join());
    verify(source, times(1)).initialize();
    
    it = new MockIterator(id);
    deferred = it.initialize();
    try {
      deferred.join();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
  
  @Test
  public void setContext() throws Exception {
    final MockIterator it = new MockIterator(id);
    final QueryContext ctx2 = mock(QueryContext.class);
    assertNull(it.context);
    verify(context, never()).register(it);
    verify(ctx2, never()).register(it);
    
    it.setContext(context);
    assertSame(context, it.context);
    verify(context, times(1)).register(it);
    verify(context, never()).unregister(it);
    verify(ctx2, never()).register(it);
    verify(ctx2, never()).unregister(it);
    
    it.setContext(ctx2);
    assertSame(ctx2, it.context);
    verify(context, times(1)).register(it);
    verify(context, times(1)).unregister(it);
    verify(ctx2, times(1)).register(it);
    verify(ctx2, never()).unregister(it);
  }

  @Test
  public void status() throws Exception {
    final MockIterator it = new MockIterator(context, source);
    it.status();
    verify(source, times(1)).status();
  }
  
  @Test
  public void next() throws Exception {
    final MockIterator it = new MockIterator(context, source);
    it.next();
    verify(source, times(1)).next();
  }
  
  @Test
  public void fetchNext() throws Exception {
    MockIterator it = new MockIterator(context, source);
    Deferred<Object> deferred = it.fetchNext();
    assertNull(deferred.join());
    verify(source, times(1)).fetchNext();
    
    it = new MockIterator(id);
    deferred = it.fetchNext();
    try {
      deferred.join();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
  
  @Test
  public void getCopy() throws Exception {
    MockIterator it = new MockIterator(context, source);
    TimeSeriesIterator<?> copy = it.getShallowCopy(context);
    assertNotSame(copy, it);
    assertSame(context, copy.context);
    assertSame(source_clone, copy.source);
    assertNull(copy.parent);
    verify(context, times(1)).register(it);
    verify(context, times(1)).register(copy);
    
    it = new MockIterator(id, context);
    copy = it.getShallowCopy(context);
    assertNotSame(copy, it);
    assertSame(context, copy.context);
    assertNull(copy.source);
    assertNull(copy.parent);
    verify(context, times(1)).register(it);
    verify(context, times(1)).register(copy);
    
    it = new MockIterator(id);
    copy = it.getShallowCopy(context);
    assertNotSame(copy, it);
    assertSame(context, copy.context);
    assertNull(copy.source);
    assertNull(copy.parent);
    verify(context, never()).register(it);
    verify(context, times(1)).register(copy);
  }
  
  @Test
  public void close() throws Exception {
    MockIterator it = new MockIterator(context, source);
    Deferred<Object> deferred = it.close();
    assertNull(deferred.join());
    verify(source, times(1)).close();
    
    it = new MockIterator(id);
    deferred = it.close();
    try {
      deferred.join();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
  
  /**
   * Mock implementation for testing.
   */
  static class MockIterator extends TimeSeriesIterator<NumericType> {
    
    public MockIterator(final TimeSeriesId id) {
      super(id);
    }
    
    public MockIterator(final TimeSeriesId id, final QueryContext context) {
      super(id, context);
    }
    
    public MockIterator(final QueryContext context, 
                        final TimeSeriesIterator<?> source) {
      super(context, source);
    }
    
    @Override
    public TypeToken<? extends TimeSeriesDataType> type() {
      return NumericType.TYPE;
    }

    @Override
    public IteratorStatus status() {
      return source.status();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public TimeSeriesValue<NumericType> next() {
      return (TimeSeriesValue<NumericType>) source.next();
    }

    @Override
    public TimeSeriesIterator<NumericType> getShallowCopy(final QueryContext context) {
      final MockIterator copy = new MockIterator(id, context);
      if (source != null) {
        copy.source = source.getShallowCopy(context);
      }
      return copy;
    }

    @Override
    public TimeSeriesIterator<NumericType> getDeepCopy(QueryContext context,
        TimeStamp start, TimeStamp end) {
      // TODO Auto-generated method stub
      return null;
    }
    
    @Override
    public TimeStamp startTime() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public TimeStamp endTime() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected void updateContext() {
      // TODO Auto-generated method stub
      
    }

    @Override
    public TimeSeriesValue<NumericType> peek() {
      // TODO Auto-generated method stub
      return null;
    }

    
  }
}
