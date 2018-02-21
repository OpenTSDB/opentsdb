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
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.annotation.MockAnnotationIterator;
import net.opentsdb.data.types.numeric.MockNumericIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;

public class TestDefaultIteratorGroup {

  private TimeSeriesGroupId group_id;
  private TimeSeriesStringId id_a;
  private TimeSeriesStringId id_b;
  private QueryContext context;
  
  @Before
  public void before() throws Exception {
    group_id = new SimpleStringGroupId("Freys");
    id_a = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.idle")
        .build();
    id_b = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    context = mock(QueryContext.class);
  }
  
  @Test
  public void ctor() throws Exception {
    DefaultIteratorGroup group = new DefaultIteratorGroup(group_id);
    assertTrue(group.iterators().isEmpty());
    assertEquals(-1, group.order());
    
    try {
      new DefaultIteratorGroup(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void initialize() throws Exception {
    final TimeSeriesIterators its_a = mock(TimeSeriesIterators.class);
    final TimeSeriesIterators its_b = mock(TimeSeriesIterators.class);
    when(its_a.initialize()).thenReturn(Deferred.fromResult(null));
    when(its_b.initialize()).thenReturn(Deferred.fromResult(null));
    
    final DefaultIteratorGroup group = new DefaultIteratorGroup(group_id);
    group.addIterators(its_a);
    group.addIterators(its_b);
    
    assertNull(group.initialize().join());
    verify(its_a, times(1)).initialize();
    verify(its_b, times(1)).initialize();
    
    // exception
    final IllegalStateException ex = new IllegalStateException("Boo!");
    when(its_a.initialize()).thenReturn(Deferred.fromError(ex));
    
    final Deferred<Object> deferred = group.initialize();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) { 
      assertSame(ex, e.getCause());
    }
    verify(its_a, times(2)).initialize();
    verify(its_b, times(2)).initialize();
  }
  
  @Test
  public void addAndIterate() throws Exception {
    TimeSeriesIterator<NumericType> num_it_a = new MockNumericIterator(id_a);
    TimeSeriesIterator<NumericType> num_it_b = new MockNumericIterator(id_b);
    TimeSeriesIterator<AnnotationType> note_it_a = new MockAnnotationIterator(id_a);
    
    DefaultIteratorGroup group = new DefaultIteratorGroup(group_id);
    group.addIterator(num_it_a);
    group.addIterator(num_it_b);
    group.addIterator(note_it_a);
    
    assertEquals(3, group.flattenedIterators().size());
    assertEquals(2, group.iterators().size());
    assertSame(num_it_a, group.flattenedIterators().get(0));
    assertSame(note_it_a, group.flattenedIterators().get(1));
    assertSame(num_it_b, group.flattenedIterators().get(2));
    
    Iterator<TimeSeriesIterators> it = group.iterator();
    TimeSeriesIterators iterators = it.next();
    assertEquals(2, iterators.iterators().size());
    assertSame(num_it_a, iterators.iterators().get(0));
    assertSame(note_it_a, iterators.iterators().get(1));
    
    iterators = it.next();
    assertEquals(1, iterators.iterators().size());
    assertSame(num_it_b, iterators.iterators().get(0));
    assertFalse(it.hasNext());
    
    try {
      // already have a numeric in that set (not throwing due to dupe)
      group.addIterator(num_it_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // add iterator set
    group = new DefaultIteratorGroup(group_id);
    iterators = new DefaultTimeSeriesIterators(id_a);
    iterators.addIterator(num_it_a);
    iterators.addIterator(note_it_a);
    group.addIterators(iterators);
    
    iterators = new DefaultTimeSeriesIterators(id_b);
    iterators.addIterator(num_it_b);
    group.addIterators(iterators);
    
    assertEquals(3, group.flattenedIterators().size());
    assertEquals(2, group.iterators().size());
    assertSame(num_it_a, group.flattenedIterators().get(0));
    assertSame(note_it_a, group.flattenedIterators().get(1));
    assertSame(num_it_b, group.flattenedIterators().get(2));
    
    List<TimeSeriesIterator<?>> typed = group.iterators(NumericType.TYPE);
    assertEquals(2, typed.size());
    assertSame(num_it_a, typed.get(0));
    assertSame(num_it_b, typed.get(1));
    
    try {
      group.iterators(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      group.addIterator(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // diff order
    num_it_b = new MockNumericIterator(id_b, 42);
    group = new DefaultIteratorGroup(group_id);
    group.addIterator(num_it_a);
    try {
      group.addIterator(num_it_b);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // add iterator set with diff order
    group = new DefaultIteratorGroup(group_id);
    iterators = new DefaultTimeSeriesIterators(id_a);
    iterators.addIterator(num_it_a);
    iterators.addIterator(note_it_a);
    group.addIterators(iterators);
    
    iterators = new DefaultTimeSeriesIterators(id_b);
    iterators.addIterator(num_it_b);
    
    try {
      group.addIterators(iterators);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void close() throws Exception {
    final TimeSeriesIterators its_a = mock(TimeSeriesIterators.class);
    final TimeSeriesIterators its_b = mock(TimeSeriesIterators.class);
    when(its_a.close()).thenReturn(Deferred.fromResult(null));
    when(its_b.close()).thenReturn(Deferred.fromResult(null));
    
    final DefaultIteratorGroup group = new DefaultIteratorGroup(group_id);
    group.addIterators(its_a);
    group.addIterators(its_b);
    
    assertNull(group.close().join());
    verify(its_a, times(1)).close();
    verify(its_b, times(1)).close();
    
    // exception
    final IllegalStateException ex = new IllegalStateException("Boo!");
    when(its_a.close()).thenReturn(Deferred.fromError(ex));
    
    final Deferred<Object> deferred = group.close();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) { 
      assertSame(ex, e.getCause());
    }
    verify(its_a, times(2)).close();
    verify(its_b, times(2)).close();
  }
  
  @Test
  public void getCopy() throws Exception {
    final TimeSeriesIterators its_a = mock(TimeSeriesIterators.class);
    final TimeSeriesIterators its_a_clone = mock(TimeSeriesIterators.class);
    final TimeSeriesIterators its_b = mock(TimeSeriesIterators.class);
    final TimeSeriesIterators its_b_clone = mock(TimeSeriesIterators.class);
    
    when(its_a.getCopy(context)).thenReturn(its_a_clone);
    when(its_b.getCopy(context)).thenReturn(its_b_clone);
    
    final DefaultIteratorGroup group = new DefaultIteratorGroup(group_id);
    group.addIterators(its_a);
    group.addIterators(its_b);
    assertEquals(2, group.iterators().size());
    
    final IteratorGroup clone = group.getCopy(context);
    assertNotSame(clone, group);
    assertEquals(2, clone.iterators().size());
    assertEquals(its_a_clone, clone.iterators().get(0));
    assertEquals(its_b_clone, clone.iterators().get(1));
  }
}
