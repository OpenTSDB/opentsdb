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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;

public class TestGroupedIterators {
  private TimeSeriesIterator<?> num_1;
  private TimeSeriesIterator<?> num_2;
  private TimeSeriesIterator<?> annotation;
  private TimeSeriesGroupId group_1;
  private TimeSeriesGroupId group_2;
  private Deferred<Object> deferred;
  
  @Before
  public void before() throws Exception {
    num_1 = mock(TimeSeriesIterator.class);
    num_2 = mock(TimeSeriesIterator.class);
    annotation = mock(TimeSeriesIterator.class);
    group_1 = new SimpleStringGroupId("House Snow");
    group_2 = new SimpleStringGroupId("House Lanister");
    
    when(num_1.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return NumericType.TYPE;
      }
    });
    when(num_2.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return NumericType.TYPE;
      }
    });
    when(annotation.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return AnnotationType.TYPE;
      }
    });
    when(num_1.initialize()).thenReturn(deferred);
    when(num_2.initialize()).thenReturn(deferred);
    when(annotation.initialize()).thenReturn(deferred);
    when(num_1.close()).thenReturn(deferred);
    when(num_2.close()).thenReturn(deferred);
    when(annotation.close()).thenReturn(deferred);
  }
  
  @Test
  public void ctor() throws Exception {
    GroupedIterators group = new GroupedIterators();
    assertTrue(group.iteratorLists().isEmpty());
    
    group = new GroupedIterators(4);
    assertTrue(group.iteratorLists().isEmpty());
  }
  
  @Test
  public void addIterator() throws Exception {
    GroupedIterators group = new GroupedIterators();
    group.addIterator(group_1, num_1);
    assertEquals(1, group.iteratorLists().size());
    GroupedAndTypedIteratorLists lists = group.getGroup(group_1);
    assertSame(group_1, lists.id());
    
    group.addIterator(group_1, num_2);
    assertEquals(1, group.iteratorLists().size());
    
    group.addIterator(group_1, annotation);
    assertEquals(1, group.iteratorLists().size());
    
    group.addIterator(group_2, num_1);
    assertEquals(2, group.iteratorLists().size());
    lists = group.getGroup(group_2);
    assertSame(group_2, lists.id());
    
    try {
      group.addIterator(null, num_1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      group.addIterator(group_1, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void addGroup() throws Exception {
    final GroupedIterators group = new GroupedIterators();
    final GroupedAndTypedIteratorLists lists = 
        new GroupedAndTypedIteratorLists(group_1);
    lists.addIterator(num_1);
    group.addGroup(lists);
    assertEquals(1, group.iteratorLists().size());
    
    try {
      group.addGroup(lists);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test (expected = UnsupportedOperationException.class)
  public void iteratorsUnModifiable() throws Exception {
    final GroupedIterators group = new GroupedIterators();
    group.addIterator(group_1, num_1);
    
    final Iterator<Entry<TimeSeriesGroupId, GroupedAndTypedIteratorLists>> iterator
      = group.iterator();
    assertTrue(iterator.hasNext());
    iterator.remove();
  }

  @Test
  public void iterator() throws Exception {
    GroupedIterators group = new GroupedIterators();
    group.addIterator(group_1, num_1);
    group.addIterator(group_1, num_2);
    group.addIterator(group_1, annotation);
    group.addIterator(group_2, num_1);
    
    final Iterator<Entry<TimeSeriesGroupId, GroupedAndTypedIteratorLists>> iterator
      = group.iterator();
    assertTrue(iterator.hasNext());
    Entry<TimeSeriesGroupId, GroupedAndTypedIteratorLists> entry = iterator.next();
    if (entry.getKey().equals(group_1)) {
      assertEquals(2, entry.getValue().iterators().size());
    } else {
      assertEquals(1, entry.getValue().iterators().size());
    }
    assertTrue(iterator.hasNext());
    entry = iterator.next();
    if (entry.getKey().equals(group_1)) {
      assertEquals(2, entry.getValue().iterators().size());
    } else {
      assertEquals(1, entry.getValue().iterators().size());
    }
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void flattenedIterators() throws Exception {
    GroupedIterators group = new GroupedIterators();
    group.addIterator(group_1, num_1);
    group.addIterator(group_1, annotation);
    group.addIterator(group_2, num_2);
    // order is indeterminate.
    assertEquals(3, group.flattenedIterators().size());
  }
  
  @Test
  public void initializeIterators() throws Exception {
    GroupedIterators group = new GroupedIterators();
    group.addIterator(group_1, num_1);
    group.addIterator(group_1, annotation);
    group.addIterator(group_2, num_2);
    
    final List<Deferred<Object>> deferreds = Lists.newArrayListWithExpectedSize(3);
    group.initializeIterators(deferreds);
    assertEquals(3, deferreds.size());
    verify(num_1, times(1)).initialize();
    verify(num_2, times(1)).initialize();
    verify(annotation, times(1)).initialize();
    
    group = new GroupedIterators();
    deferreds.clear();
    group.initializeIterators(deferreds);
    assertTrue(deferreds.isEmpty());
  }
  
  @Test
  public void setContext() throws Exception {
    GroupedIterators group = new GroupedIterators();
    group.addIterator(group_1, num_1);
    group.addIterator(group_1, annotation);
    group.addIterator(group_2, num_2);
    
    final QueryContext context = mock(QueryContext.class);
    
    group.setContext(context);
    verify(num_1, times(1)).setContext(context);
    verify(num_2, times(1)).setContext(context);
    verify(annotation, times(1)).setContext(context);
    
    group = new GroupedIterators();
    group.setContext(context);
    // no-op.
  }
  
  @Test
  public void close() throws Exception {
    GroupedIterators group = new GroupedIterators();
    group.addIterator(group_1, num_1);
    group.addIterator(group_1, annotation);
    group.addIterator(group_2, num_2);
    
    final List<Deferred<Object>> deferreds = Lists.newArrayListWithExpectedSize(3);
    group.close(deferreds);
    assertEquals(3, deferreds.size());
    verify(num_1, times(1)).close();
    verify(num_2, times(1)).close();
    verify(annotation, times(1)).close();
    
    group = new GroupedIterators();
    deferreds.clear();
    group.initializeIterators(deferreds);
    assertTrue(deferreds.isEmpty());
  }
  
  @Test
  public void getClone() throws Exception {
    final TimeSeriesIterator<?> num_1_clone = mock(TimeSeriesIterator.class);
    final TimeSeriesIterator<?> num_2_clone = mock(TimeSeriesIterator.class);
    final TimeSeriesIterator<?> annotation_clone = mock(TimeSeriesIterator.class);
    final QueryContext context = mock(QueryContext.class);
    when(num_1.getCopy(any(QueryContext.class)))
      .thenAnswer(new Answer<TimeSeriesIterator<?>>() {
      @Override
      public TimeSeriesIterator<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return num_1_clone;
      }
    });
    when(num_1_clone.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return NumericType.TYPE;
      }
    });
    when(num_2.getCopy(any(QueryContext.class)))
      .thenAnswer(new Answer<TimeSeriesIterator<?>>() {
      @Override
      public TimeSeriesIterator<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return num_2_clone;
      }
    });
    when(num_2_clone.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return NumericType.TYPE;
      }
    });
    when(annotation.getCopy(any(QueryContext.class)))
      .thenAnswer(new Answer<TimeSeriesIterator<?>>() {
      @Override
      public TimeSeriesIterator<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return annotation_clone;
      }
    });
    when(annotation_clone.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return AnnotationType.TYPE;
      }
    });
    
    GroupedIterators group = new GroupedIterators();
    group.addIterator(group_1, num_1);
    group.addIterator(group_1, annotation);
    group.addIterator(group_2, num_2);
    
    GroupedIterators clone = group.getClone(context);
    assertNotSame(clone, group);
    assertNotSame(clone.getGroup(group_1), group.getGroup(group_1));
    assertNotSame(clone.getGroup(group_2), group.getGroup(group_2));
    verify(num_1, times(1)).getCopy(context);
    verify(num_2, times(1)).getCopy(context);
    verify(annotation, times(1)).getCopy(context);
  }
  
}
