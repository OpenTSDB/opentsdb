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
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.annotation.MockAnnotationIterator;
import net.opentsdb.data.types.numeric.MockNumericIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;

public class TestDefaultIteratorGroups {

  private TimeSeriesGroupId group_id_a;
  private TimeSeriesGroupId group_id_b;
  private TimeSeriesId id_a;
  private TimeSeriesId id_b;
  private QueryContext context;
  
  @Before
  public void before() throws Exception {
    group_id_a = new SimpleStringGroupId("Freys");
    group_id_b = new SimpleStringGroupId("Lanisters");
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
    DefaultIteratorGroups groups = new DefaultIteratorGroups();
    assertTrue(groups.groups().isEmpty());
    assertEquals(-1, groups.order());
  }
  
  @Test
  public void initialize() throws Exception {
    final IteratorGroup group_a = mock(IteratorGroup.class);
    when(group_a.id()).thenReturn(group_id_a);
    final IteratorGroup group_b = mock(IteratorGroup.class);
    when(group_b.id()).thenReturn(group_id_b);
    when(group_a.initialize()).thenReturn(Deferred.fromResult(null));
    when(group_b.initialize()).thenReturn(Deferred.fromResult(null));
    
    DefaultIteratorGroups groups = new DefaultIteratorGroups();
    groups.addGroup(group_a);
    groups.addGroup(group_b);
    
    assertNull(groups.initialize().join());
    verify(group_a, times(1)).initialize();
    verify(group_b, times(1)).initialize();
    
    // exception
    final IllegalStateException ex = new IllegalStateException("Boo!");
    when(group_b.initialize()).thenReturn(Deferred.fromError(ex));
    
    final Deferred<Object> deferred = groups.initialize();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) { 
      assertSame(ex, e.getCause());
    }
    verify(group_a, times(2)).initialize();
    verify(group_b, times(2)).initialize();
  }
  
  @Test
  public void addAndIterate() throws Exception {
    TimeSeriesIterator<NumericType> num_it_a = new MockNumericIterator(id_a);
    TimeSeriesIterator<NumericType> num_it_b = new MockNumericIterator(id_b);
    TimeSeriesIterator<AnnotationType> note_it_a = new MockAnnotationIterator(id_a);
    
    DefaultIteratorGroups groups = new DefaultIteratorGroups();
    groups.addIterator(group_id_a, num_it_a);
    groups.addIterator(group_id_a, num_it_b);
    groups.addIterator(group_id_a, note_it_a);
    
    groups.addIterator(group_id_b, num_it_a);
    
    assertEquals(2, groups.groups().size());
    assertEquals(4, groups.flattenedIterators().size());
    // since it's a map the order of flattened iterators is inconsistent.
    IteratorGroup group = groups.group(group_id_a);
    assertEquals(3, group.flattenedIterators().size());
    assertSame(num_it_a, group.flattenedIterators().get(0));
    assertSame(note_it_a, group.flattenedIterators().get(1));
    assertSame(num_it_b, group.flattenedIterators().get(2));
    
    group = groups.group(group_id_b);
    assertEquals(1, group.flattenedIterators().size());
    assertSame(num_it_a, group.flattenedIterators().get(0));
    
    Iterator<Entry<TimeSeriesGroupId, IteratorGroup>> iterator = 
        groups.iterator();
    Entry<TimeSeriesGroupId, IteratorGroup> entry = iterator.next();
    assertTrue(entry.getKey().equals(group_id_a) || 
               entry.getKey().equals(group_id_b));
    entry = iterator.next();
    assertTrue(entry.getKey().equals(group_id_a) || 
               entry.getKey().equals(group_id_b));
    assertFalse(iterator.hasNext());
    
    // add groups instead of individual iterators
    groups = new DefaultIteratorGroups();
    
    group = new DefaultIteratorGroup(group_id_a);
    group.addIterator(num_it_a);
    group.addIterator(num_it_b);
    group.addIterator(note_it_a);
    groups.addGroup(group);
    
    group = new DefaultIteratorGroup(group_id_b);
    group.addIterator(num_it_a);
    groups.addGroup(group);
    
    assertEquals(2, groups.groups().size());
    assertEquals(4, groups.flattenedIterators().size());
  }
  
  @Test
  public void addAndIterateDiffOrders() throws Exception {
    TimeSeriesIterator<NumericType> num_it_a = new MockNumericIterator(id_a);
    TimeSeriesIterator<NumericType> num_it_b = new MockNumericIterator(id_b, 42);
    TimeSeriesIterator<AnnotationType> note_it_a = new MockAnnotationIterator(id_a);
    
    DefaultIteratorGroups groups = new DefaultIteratorGroups();
    groups.addIterator(group_id_a, num_it_a);
    try {
      groups.addIterator(group_id_a, num_it_b);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    groups.addIterator(group_id_a, note_it_a);
    
    groups.addIterator(group_id_b, num_it_a);
    
    assertEquals(2, groups.groups().size());
    assertEquals(3, groups.flattenedIterators().size());
    // since it's a map the order of flattened iterators is inconsistent.
    IteratorGroup group = groups.group(group_id_a);
    assertEquals(2, group.flattenedIterators().size());
    assertSame(num_it_a, group.flattenedIterators().get(0));
    assertSame(note_it_a, group.flattenedIterators().get(1));
    
    group = groups.group(group_id_b);
    assertEquals(1, group.flattenedIterators().size());
    assertSame(num_it_a, group.flattenedIterators().get(0));
    
    Iterator<Entry<TimeSeriesGroupId, IteratorGroup>> iterator = 
        groups.iterator();
    Entry<TimeSeriesGroupId, IteratorGroup> entry = iterator.next();
    assertTrue(entry.getKey().equals(group_id_a) || 
               entry.getKey().equals(group_id_b));
    entry = iterator.next();
    assertTrue(entry.getKey().equals(group_id_a) || 
               entry.getKey().equals(group_id_b));
    assertFalse(iterator.hasNext());
    
    // add groups instead of individual iterators
    groups = new DefaultIteratorGroups();
    
    num_it_b = new MockNumericIterator(id_b);
    group = new DefaultIteratorGroup(group_id_a);
    group.addIterator(num_it_a);
    group.addIterator(num_it_b);
    group.addIterator(note_it_a);
    groups.addGroup(group);
    
    group = new DefaultIteratorGroup(group_id_b);
    num_it_a = new MockNumericIterator(id_a, 42);
    group.addIterator(num_it_a);
    try {
      groups.addGroup(group);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    assertEquals(1, groups.groups().size());
    assertEquals(3, groups.flattenedIterators().size());
  }

  @Test
  public void addAndIterateExceptions() throws Exception {
    TimeSeriesIterator<NumericType> num_it_a = new MockNumericIterator(id_a);
    DefaultIteratorGroups groups = new DefaultIteratorGroups();
    
    try {
      groups.addIterator(null, num_it_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      groups.addIterator(group_id_a, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      groups.addGroup(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      groups.group(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void close() throws Exception {
    final IteratorGroup group_a = mock(IteratorGroup.class);
    when(group_a.id()).thenReturn(group_id_a);
    final IteratorGroup group_b = mock(IteratorGroup.class);
    when(group_b.id()).thenReturn(group_id_b);
    when(group_a.close()).thenReturn(Deferred.fromResult(null));
    when(group_b.close()).thenReturn(Deferred.fromResult(null));
    
    DefaultIteratorGroups groups = new DefaultIteratorGroups();
    groups.addGroup(group_a);
    groups.addGroup(group_b);
    
    assertNull(groups.close().join());
    verify(group_a, times(1)).close();
    verify(group_b, times(1)).close();
    
    // exception
    final IllegalStateException ex = new IllegalStateException("Boo!");
    when(group_b.close()).thenReturn(Deferred.fromError(ex));
    
    final Deferred<Object> deferred = groups.close();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) { 
      assertSame(ex, e.getCause());
    }
    verify(group_a, times(2)).close();
    verify(group_b, times(2)).close();
  }

  @Test
  public void getCopy() throws Exception {
    final IteratorGroup group_a = mock(IteratorGroup.class);
    final IteratorGroup group_a_clone = mock(IteratorGroup.class);
    when(group_a.id()).thenReturn(group_id_a);
    when(group_a.getCopy(context)).thenReturn(group_a_clone);
    when(group_a_clone.id()).thenReturn(group_id_a);
    final IteratorGroup group_b = mock(IteratorGroup.class);
    final IteratorGroup group_b_clone = mock(IteratorGroup.class);
    when(group_b.id()).thenReturn(group_id_b);
    when(group_b.getCopy(context)).thenReturn(group_b_clone);
    when(group_b_clone.id()).thenReturn(group_id_b);
    
    DefaultIteratorGroups groups = new DefaultIteratorGroups();
    groups.addGroup(group_a);
    groups.addGroup(group_b);
    assertEquals(2, groups.groups().size());
    
    IteratorGroups clone = groups.getCopy(context);
    assertEquals(2, clone.groups().size());
    assertNotSame(clone, groups);
    assertSame(group_a_clone, clone.group(group_id_a));
    assertSame(group_b_clone, clone.group(group_id_b));
  }
}
