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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;

public class TestGroupedAndTypedIteratorLists {
  private TimeSeriesIterator<?> num_1;
  private TimeSeriesIterator<?> num_2;
  private TimeSeriesIterator<?> annotation;
  private TimeSeriesGroupId group;
  
  @Before
  public void before() throws Exception {
    num_1 = mock(TimeSeriesIterator.class);
    num_2 = mock(TimeSeriesIterator.class);
    annotation = mock(TimeSeriesIterator.class);
    group = new SimpleStringGroupId("House Snow");
    
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
  }

  @Test
  public void ctor() throws Exception {
    final GroupedAndTypedIteratorLists collection = 
        new GroupedAndTypedIteratorLists(group);
    assertSame(group, collection.id());
    assertTrue(collection.iterators().isEmpty());
    assertNull(collection.iterators(NumericType.TYPE));
    
    try {
      new GroupedAndTypedIteratorLists(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void addIterator() throws Exception {
    final GroupedAndTypedIteratorLists collection = 
        new GroupedAndTypedIteratorLists(group);
    assertTrue(collection.iterators().isEmpty());
    
    collection.addIterator(num_1);
    assertEquals(1, collection.iterators().size());
    TypedIteratorList list = collection.iterators(NumericType.TYPE);
    assertSame(num_1, list.iterators().get(0));
    
    collection.addIterator(num_2);
    assertEquals(1, collection.iterators().size());
    list = collection.iterators(NumericType.TYPE);
    assertSame(num_1, list.iterators().get(0));
    assertSame(num_2, list.iterators().get(1));
    
    collection.addIterator(annotation);
    assertEquals(2, collection.iterators().size());
    list = collection.iterators(NumericType.TYPE);
    assertSame(num_1, list.iterators().get(0));
    assertSame(num_2, list.iterators().get(1));
    list = collection.iterators(AnnotationType.TYPE);
    assertSame(annotation, list.iterators().get(0));
    
    try {
      collection.addIterator(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test (expected = UnsupportedOperationException.class)
  public void iteratorsUnModifiable() throws Exception {
    final GroupedAndTypedIteratorLists collection = 
        new GroupedAndTypedIteratorLists(group);
    collection.addIterator(num_1);
    collection.addIterator(annotation);
    
    collection.iterators().add(new TypedIteratorList());
  }
  
  @Test
  public void iterator() throws Exception {
    final GroupedAndTypedIteratorLists collection = 
        new GroupedAndTypedIteratorLists(group);
    collection.addIterator(num_1);
    collection.addIterator(annotation);
    
    Iterator<Entry<TypeToken<?>, TypedIteratorList>> iterator = collection.iterator();
    // order is indeterminate.
    assertTrue(iterator.hasNext());
    assertNotNull(iterator.next());
    assertTrue(iterator.hasNext());
    assertNotNull(iterator.next());
    assertFalse(iterator.hasNext());
    
    iterator = collection.iterator();
    assertTrue(iterator.hasNext());
    try {
      iterator.remove();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }

  @Test
  public void getClone() throws Exception {
    final TimeSeriesIterator<?> num_1_clone = mock(TimeSeriesIterator.class);
    final TimeSeriesIterator<?> annotation_clone = mock(TimeSeriesIterator.class);
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
    
    GroupedAndTypedIteratorLists collection = 
        new GroupedAndTypedIteratorLists(group);
    collection.addIterator(num_1);
    collection.addIterator(annotation);
    
    GroupedAndTypedIteratorLists clone = 
        collection.getClone(mock(QueryContext.class));
    assertNotSame(clone, collection);
    assertNotSame(collection.iterators(NumericType.TYPE), 
        clone.iterators(NumericType.TYPE));
    assertNotSame(collection.iterators(AnnotationType.TYPE), 
        clone.iterators(AnnotationType.TYPE));
    
    collection = new GroupedAndTypedIteratorLists(group);
    clone = collection.getClone(mock(QueryContext.class));
    assertNotSame(clone, collection);
    assertTrue(clone.iterators().isEmpty());
  }
}
