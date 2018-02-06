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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TimeSeriesProcessor.class })
public class TestTimeSeriesProcessor {
  
  private TimeSeriesId id;
  
  @Before
  public void before() throws Exception {
    id = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.idle")
        .build();
  }
  
  @Test
  public void ctor() throws Exception {
    MockImplementation processor = new MockImplementation();
    assertNull(processor.config);
    assertNotNull(processor.iterators);
    assertTrue(processor.iterators.flattenedIterators().isEmpty());
    assertNull(processor.context);
    assertNotNull(processor.init_deferred);
    try {
      processor.init_deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertSame(processor.init_deferred, processor.initializationDeferred());
    
    final QueryContext context = mock(QueryContext.class);
    processor = new MockImplementation(context);
    assertNull(processor.config);
    assertNotNull(processor.iterators);
    assertTrue(processor.iterators.flattenedIterators().isEmpty());
    assertSame(context, processor.context);
    verify(context, times(1)).register(processor);
    assertNotNull(processor.init_deferred);
    try {
      processor.init_deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertSame(processor.init_deferred, processor.initializationDeferred());
    
    final TimeSeriesProcessorConfig<?> config = mock(TimeSeriesProcessorConfig.class);
    processor = new MockImplementation(config);
    assertSame(config, processor.config);
    assertNotNull(processor.iterators);
    assertTrue(processor.iterators.flattenedIterators().isEmpty());
    assertNull(processor.context);
    assertNotNull(processor.init_deferred);
    try {
      processor.init_deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertSame(processor.init_deferred, processor.initializationDeferred());
    
    processor = new MockImplementation(context, config);
    assertSame(config, processor.config);
    assertNotNull(processor.iterators);
    assertTrue(processor.iterators.flattenedIterators().isEmpty());
    assertSame(context, processor.context);
    verify(context, times(1)).register(processor);
    assertNotNull(processor.init_deferred);
    try {
      processor.init_deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertSame(processor.init_deferred, processor.initializationDeferred());
  }
  
  @Test
  public void initialize() throws Exception {
    final DefaultIteratorGroups mock_iterators = 
        mock(DefaultIteratorGroups.class);
    when(mock_iterators.initialize()).thenReturn(Deferred.fromResult(null));
    PowerMockito.whenNew(DefaultIteratorGroups.class).withNoArguments()
      .thenReturn(mock_iterators);
    MockImplementation processor = new MockImplementation();
    Deferred<Object> deferred = processor.initializationDeferred();
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    deferred = processor.initialize();
    assertSame(deferred, processor.init_deferred);
    assertNull(deferred.join());
    verify(mock_iterators, times(1)).initialize();
    
    final RuntimeException ex = new RuntimeException("Boo!");
    when(mock_iterators.initialize()).thenAnswer(new Answer<Deferred<Object>>() {
      @Override
      public Deferred<Object> answer(InvocationOnMock invocation)
          throws Throwable {
        return Deferred.fromError(ex);
      }
    });
    processor = new MockImplementation();
    deferred = processor.initialize();
    try {
      deferred.join();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      assertSame(e, ex);
    }
    verify(mock_iterators, times(2)).initialize();
  }
  
  @Test
  public void setContext() throws Exception {
    final MockImplementation processor = new MockImplementation();
    assertNull(processor.context);
    
    final QueryContext context = mock(QueryContext.class);
    processor.setContext(context);
    verify(context, times(1)).register(processor);
    
    final QueryContext context2 = mock(QueryContext.class);
    processor.setContext(context2);
    verify(context, times(1)).unregister(processor);
    verify(context2, times(1)).register(processor);
    
    processor.setContext(null);
    verify(context2, times(1)).unregister(processor);
  }
  
  @Test
  public void addSeries() throws Exception {
    final TimeSeriesGroupId group = new SimpleStringGroupId("Freys");
    final TimeSeriesIterator<?> iterator = mock(TimeSeriesIterator.class);
    when(iterator.id()).thenReturn(id);
    when(iterator.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return NumericType.TYPE;
      }
    });
    
    final MockImplementation processor = new MockImplementation();
    assertTrue(processor.iterators().flattenedIterators().isEmpty());
    processor.addSeries(group, iterator);
    assertEquals(1, processor.iterators().flattenedIterators().size());
    assertSame(iterator, processor.iterators().flattenedIterators().get(0));
    
    try {
      processor.addSeries(null, iterator);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      processor.addSeries(group, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getClone() throws Exception {
    final QueryContext context = mock(QueryContext.class);
    final TimeSeriesGroupId group = new SimpleStringGroupId("Freys");
    final TimeSeriesIterator<?> iterator = mock(TimeSeriesIterator.class);
    when(iterator.id()).thenReturn(id);
    final TimeSeriesIterator<?> iterator_clone = mock(TimeSeriesIterator.class);
    when(iterator_clone.id()).thenReturn(id);
    when(iterator.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return NumericType.TYPE;
      }
    });
    when(iterator.getShallowCopy(context)).thenAnswer(new Answer<TimeSeriesIterator<?>>() {
      @Override
      public TimeSeriesIterator<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return iterator_clone;
      }
    });
    
    final MockImplementation processor = new MockImplementation();
    processor.addSeries(group, iterator);
    
    final MockImplementation clone = (MockImplementation) processor.getClone(context);
    assertSame(context, clone.context);
    assertNotSame(clone, processor);
    assertNotSame(clone.iterators, processor.iterators);
    assertEquals(1, clone.iterators().flattenedIterators().size());
    assertSame(iterator_clone, clone.iterators().flattenedIterators().get(0));
    verify(context, times(1)).register(clone);
  }
  
  @Test
  public void close() throws Exception {
    final DefaultIteratorGroups mock_iterators = 
        mock(DefaultIteratorGroups.class);
    when(mock_iterators.close()).thenReturn(Deferred.fromResult(null));
    PowerMockito.whenNew(DefaultIteratorGroups.class).withNoArguments()
      .thenReturn(mock_iterators);
    MockImplementation processor = new MockImplementation();
    Deferred<Object> deferred = processor.close();
    assertNull(deferred.join());
    verify(mock_iterators, times(1)).close();
    
    final RuntimeException ex = new RuntimeException("Boo!");
    when(mock_iterators.close()).thenAnswer(new Answer<Deferred<Object>>() {
      @Override
      public Deferred<Object> answer(InvocationOnMock invocation)
          throws Throwable {
        return Deferred.fromError(ex);
      }
    });
    processor = new MockImplementation();
    deferred = processor.close();
    try {
      deferred.join();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      assertSame(e, ex);
    }
    verify(mock_iterators, times(2)).close();
  }
  
  @Test
  public void initializationCallback() throws Exception {
    final DefaultIteratorGroups mock_iterators = 
        mock(DefaultIteratorGroups.class);
    when(mock_iterators.initialize()).thenReturn(Deferred.fromResult(null));
    PowerMockito.whenNew(DefaultIteratorGroups.class).withNoArguments()
      .thenReturn(mock_iterators);
    final MockImplementation processor = new MockImplementation();
    final Deferred<Object> deferred = processor.initializationDeferred();
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    processor.initializationCallback().call(null);
    assertNull(deferred.join());
    verify(mock_iterators, times(1)).initialize();
  }
  
  /**
   * Mock implementation.
   */
  class MockImplementation extends TimeSeriesProcessor {
    
    public MockImplementation() {
      super();
    }
    
    public MockImplementation(final QueryContext context) {
      super(context);
    }
    
    public MockImplementation(final TimeSeriesProcessorConfig<?> config) {
      super(config);
    }
    
    public MockImplementation(final QueryContext context, 
        final TimeSeriesProcessorConfig<?> config) {
      super(context, config);
    }
    
    @Override
    public TimeSeriesProcessor getClone(final QueryContext context) {
      final MockImplementation clone = new MockImplementation(context, config);
      clone.iterators = iterators.getCopy(context);
      return clone;
    }
    
  }
}
