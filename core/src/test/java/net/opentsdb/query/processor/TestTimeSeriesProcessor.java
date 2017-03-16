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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;
import com.stumbleupon.async.TimeoutException;

import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.iterators.GroupedIterators;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TimeSeriesProcessor.class })
public class TestTimeSeriesProcessor {
  
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

  @SuppressWarnings("unchecked")
  @Test
  public void initialize() throws Exception {
    final GroupedIterators mock_iterators = mock(GroupedIterators.class);
    PowerMockito.whenNew(GroupedIterators.class).withNoArguments()
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
    verify(mock_iterators, times(1)).initializeIterators(any(List.class));
    
    final RuntimeException ex = new RuntimeException("Boo!");
    doThrow(ex).when(mock_iterators)
      .initializeIterators(any(List.class));
    processor = new MockImplementation();
    deferred = processor.initialize();
    try {
      deferred.join();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      assertSame(e, ex);
    }
    verify(mock_iterators, times(2)).initializeIterators(any(List.class));
    
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(final InvocationOnMock invocation) throws Throwable {
        final List<Deferred<Object>> deferreds = 
            (List<Deferred<Object>>) invocation.getArguments()[0];
        deferreds.add(Deferred.fromError(ex));
        return null;
      }
    }).when(mock_iterators).initializeIterators(any(List.class));
    deferred = processor.initialize();
    try {
      deferred.join();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      assertSame(e, ex);
    }
    verify(mock_iterators, times(3)).initializeIterators(any(List.class));
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
    final TimeSeriesIterator<?> iterator_clone = mock(TimeSeriesIterator.class);
    when(iterator.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return NumericType.TYPE;
      }
    });
    when(iterator.getCopy(context)).thenAnswer(new Answer<TimeSeriesIterator<?>>() {
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
  
  @SuppressWarnings("unchecked")
  @Test
  public void close() throws Exception {
    final GroupedIterators mock_iterators = mock(GroupedIterators.class);
    PowerMockito.whenNew(GroupedIterators.class).withNoArguments()
      .thenReturn(mock_iterators);
    MockImplementation processor = new MockImplementation();
    Deferred<Object> deferred = processor.close();
    assertNull(deferred.join());
    verify(mock_iterators, times(1)).close(any(List.class));
    
    final RuntimeException ex = new RuntimeException("Boo!");
    doThrow(ex).when(mock_iterators).close(any(List.class));
    processor = new MockImplementation();
    deferred = processor.close();
    try {
      deferred.join();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      assertSame(e, ex);
    }
    verify(mock_iterators, times(2)).close(any(List.class));
    
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(final InvocationOnMock invocation) throws Throwable {
        final List<Deferred<Object>> deferreds = 
            (List<Deferred<Object>>) invocation.getArguments()[0];
        deferreds.add(Deferred.fromError(ex));
        return null;
      }
    }).when(mock_iterators).close(any(List.class));
    deferred = processor.close();
    try {
      deferred.join();
      fail("Expected RuntimeException");
    } catch (DeferredGroupException e) {
      assertSame(e.getCause(), ex);
    }
    verify(mock_iterators, times(3)).close(any(List.class));
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void initializationCallback() throws Exception {
    final GroupedIterators mock_iterators = mock(GroupedIterators.class);
    PowerMockito.whenNew(GroupedIterators.class).withNoArguments()
      .thenReturn(mock_iterators);
    final MockImplementation processor = new MockImplementation();
    final Deferred<Object> deferred = processor.initializationDeferred();
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    processor.initializationCallback().call(null);
    assertNull(deferred.join());
    verify(mock_iterators, times(1)).initializeIterators(any(List.class));
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
      clone.iterators = iterators.getClone(context);
      return clone;
    }
    
  }
}
