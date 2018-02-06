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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.annotation.MockAnnotationIterator;
import net.opentsdb.data.types.numeric.MockNumericIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;

public class TestDefaultTimeSeriesIterators {

  private TimeSeriesId id;
  private QueryContext context;
  
  @Before
  public void before() throws Exception {
    id = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.idle")
        .build();
    context = mock(QueryContext.class);
  }
  
  @Test
  public void ctor() throws Exception {
    final DefaultTimeSeriesIterators iterators = 
        new DefaultTimeSeriesIterators(id);
    assertSame(id, iterators.id());
    assertTrue(iterators.iterators().isEmpty());
    assertEquals(-1, iterators.order());

    try {
      new DefaultTimeSeriesIterators(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void initialize() throws Exception {
    final TimeSeriesIterator<NumericType> num_it = mock(TimeSeriesIterator.class);
    when(num_it.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return NumericType.TYPE;
      }
    });
    when(num_it.id()).thenReturn(id);
    when(num_it.initialize()).thenReturn(Deferred.<Object>fromResult(null));
    TimeSeriesIterator<AnnotationType> note_it = mock(TimeSeriesIterator.class);
    when(note_it.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return AnnotationType.TYPE;
      }
    });
    when(note_it.id()).thenReturn(id);
    when(note_it.initialize()).thenReturn(Deferred.<Object>fromResult(null));
    
    DefaultTimeSeriesIterators iterators = new DefaultTimeSeriesIterators(id);
    iterators.addIterator(num_it);
    iterators.addIterator(note_it);
    assertNull(iterators.initialize().join());
    verify(num_it, times(1)).initialize();
    verify(note_it, times(1)).initialize();
    
    // exception
    final IllegalStateException ex = new IllegalStateException("Boo!");
    when(note_it.initialize()).thenReturn(Deferred.fromError(ex));
    
    iterators = new DefaultTimeSeriesIterators(id);
    iterators.addIterator(num_it);
    iterators.addIterator(note_it);
    
    final Deferred<Object> deferred = iterators.initialize();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) { 
      assertSame(ex, e.getCause());
    }
    verify(num_it, times(2)).initialize();
    verify(note_it, times(2)).initialize();
  }
  
  @Test
  public void addAndIterate() throws Exception {
    TimeSeriesIterator<NumericType> num_it = new MockNumericIterator(id);
    TimeSeriesIterator<AnnotationType> note_it = new MockAnnotationIterator(id);
    
    DefaultTimeSeriesIterators iterators = new DefaultTimeSeriesIterators(id);
    iterators.addIterator(num_it);
    iterators.addIterator(note_it);
    
    assertEquals(2, iterators.iterators().size());
    assertSame(num_it, iterators.iterators().get(0));
    assertSame(note_it, iterators.iterators().get(1));
    
    assertSame(num_it, iterators.iterator(NumericType.TYPE));
    assertSame(note_it, iterators.iterator(AnnotationType.TYPE));
    
    final Iterator<TimeSeriesIterator<?>> iterator = iterators.iterator();
    assertSame(num_it, iterator.next());
    assertSame(note_it, iterator.next());
    assertFalse(iterator.hasNext());
    
    try {
      iterators.addIterator(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // already have one, thanks!
    TimeSeriesIterator<NumericType> num_it2 = new MockNumericIterator(id);
    try {
      iterators.addIterator(num_it2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // clear and add different ID
    TimeSeriesId id2 = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    note_it = new MockAnnotationIterator(id2);
    iterators = new DefaultTimeSeriesIterators(id);
    iterators.addIterator(num_it);
    try {
      iterators.addIterator(note_it);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // different order
    note_it = new MockAnnotationIterator(id, 42);
    try {
      iterators.addIterator(note_it);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void close() throws Exception {
    final TimeSeriesIterator<NumericType> num_it = mock(TimeSeriesIterator.class);
    when(num_it.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return NumericType.TYPE;
      }
    });
    when(num_it.id()).thenReturn(id);
    when(num_it.close()).thenReturn(Deferred.<Object>fromResult(null));
    TimeSeriesIterator<AnnotationType> note_it = mock(TimeSeriesIterator.class);
    when(note_it.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return AnnotationType.TYPE;
      }
    });
    when(note_it.id()).thenReturn(id);
    when(note_it.close()).thenReturn(Deferred.<Object>fromResult(null));
    
    DefaultTimeSeriesIterators iterators = new DefaultTimeSeriesIterators(id);
    iterators.addIterator(num_it);
    iterators.addIterator(note_it);
    assertNull(iterators.close().join());
    verify(num_it, times(1)).close();
    verify(note_it, times(1)).close();
    
    // exception
    final IllegalStateException ex = new IllegalStateException("Boo!");
    when(note_it.close()).thenReturn(Deferred.fromError(ex));
    
    iterators = new DefaultTimeSeriesIterators(id);
    iterators.addIterator(num_it);
    iterators.addIterator(note_it);
    
    final Deferred<Object> deferred = iterators.close();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) { 
      assertSame(ex, e.getCause());
    }
    verify(num_it, times(2)).close();
    verify(note_it, times(2)).close();
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void getCopy() throws Exception {
    final TimeSeriesIterator<NumericType> num_it = mock(TimeSeriesIterator.class);
    final TimeSeriesIterator<NumericType> num_it_clone = 
        mock(TimeSeriesIterator.class);
    when(num_it.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return NumericType.TYPE;
      }
    });
    when(num_it_clone.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return NumericType.TYPE;
      }
    });
    when(num_it.id()).thenReturn(id);
    when(num_it_clone.id()).thenReturn(id);
    when(num_it.getShallowCopy(context)).thenAnswer(new Answer<TimeSeriesIterator<?>>() {
      @Override
      public TimeSeriesIterator<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return num_it_clone;
      }
    });
    
    final TimeSeriesIterator<AnnotationType> note_it = 
        mock(TimeSeriesIterator.class);
    final TimeSeriesIterator<AnnotationType> note_it_clone = 
        mock(TimeSeriesIterator.class);
    when(note_it.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return AnnotationType.TYPE;
      }
    });
    when(note_it_clone.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return AnnotationType.TYPE;
      }
    });
    when(note_it.id()).thenReturn(id);
    when(note_it_clone.id()).thenReturn(id);
    when(note_it.getShallowCopy(context)).thenAnswer(new Answer<TimeSeriesIterator<?>>() {
      @Override
      public TimeSeriesIterator<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return note_it_clone;
      }
    });
    
    DefaultTimeSeriesIterators iterators = new DefaultTimeSeriesIterators(id);
    iterators.addIterator(num_it);
    iterators.addIterator(note_it);
    
    TimeSeriesIterators clone = iterators.getCopy(context);
    assertNotSame(clone, iterators);
    assertEquals(2, clone.iterators().size());
    assertNotSame(num_it, clone.iterators().get(0));
    assertSame(num_it_clone, clone.iterators().get(0));
    assertNotSame(note_it, clone.iterators().get(1));
    assertSame(note_it_clone, clone.iterators().get(1));
  }
}
