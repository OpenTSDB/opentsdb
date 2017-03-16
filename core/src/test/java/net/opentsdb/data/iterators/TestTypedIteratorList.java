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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;

public class TestTypedIteratorList {
  private TimeSeriesIterator<?> num_1;
  private TimeSeriesIterator<?> num_2;
  private TimeSeriesIterator<?> annotation;
  
  @Before
  public void before() throws Exception {
    num_1 = mock(TimeSeriesIterator.class);
    num_2 = mock(TimeSeriesIterator.class);
    annotation = mock(TimeSeriesIterator.class);
    
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
    TypedIteratorList list = new TypedIteratorList();
    assertTrue(list.iterators().isEmpty());
    assertNull(list.type());
    
    list = new TypedIteratorList(4);
    assertTrue(list.iterators().isEmpty());
    assertNull(list.type());
  }
  
  @Test
  public void addIterator() throws Exception {
    final TypedIteratorList list = new TypedIteratorList();
    assertTrue(list.iterators().isEmpty());
    assertNull(list.type());
    
    list.addIterator(num_1);
    assertEquals(1, list.iterators().size());
    assertSame(num_1, list.iterators().get(0));
    assertEquals(NumericType.TYPE, list.type());
    
    list.addIterator(num_2);
    assertEquals(2, list.iterators().size());
    assertSame(num_1, list.iterators().get(0));
    assertSame(num_2, list.iterators().get(1));
    assertEquals(NumericType.TYPE, list.type());
    
    try {
      list.addIterator(annotation);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertEquals(2, list.iterators().size());
    assertSame(num_1, list.iterators().get(0));
    assertSame(num_2, list.iterators().get(1));
    assertEquals(NumericType.TYPE, list.type());
    
    try {
      list.addIterator(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertEquals(2, list.iterators().size());
    assertSame(num_1, list.iterators().get(0));
    assertSame(num_2, list.iterators().get(1));
    assertEquals(NumericType.TYPE, list.type());
    
    // dupe
    list.addIterator(num_2);
    assertEquals(3, list.iterators().size());
    assertSame(num_1, list.iterators().get(0));
    assertSame(num_2, list.iterators().get(1));
    assertSame(num_2, list.iterators().get(2));
    assertEquals(NumericType.TYPE, list.type());
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void iteratorsUnModifiable() throws Exception {
    final TypedIteratorList list = new TypedIteratorList();
    list.addIterator(num_1);
    list.iterators().add(num_2);
  }
  
  @Test
  public void iterator() throws Exception {
    final TypedIteratorList list = new TypedIteratorList();
    Iterator<TimeSeriesIterator<?>> iterator = list.iterator();
    assertFalse(iterator.hasNext());    
    
    // now add data
    list.addIterator(num_1);
    list.addIterator(num_2);
    
    iterator = list.iterator();
    assertTrue(iterator.hasNext());
    assertSame(num_1, iterator.next());
    assertTrue(iterator.hasNext());
    assertSame(num_2, iterator.next());
    assertFalse(iterator.hasNext());
    
    iterator = list.iterator();
    assertTrue(iterator.hasNext());
    try {
      iterator.remove();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
  
  @Test
  public void getClone() throws Exception {
    final TimeSeriesIterator<?> num_1_clone = mock(TimeSeriesIterator.class);
    final TimeSeriesIterator<?> num_2_clone = mock(TimeSeriesIterator.class);
    when(num_1.getCopy(any(QueryContext.class)))
      .thenAnswer(new Answer<TimeSeriesIterator<?>>() {
      @Override
      public TimeSeriesIterator<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return num_1_clone;
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
    
    final TypedIteratorList list = new TypedIteratorList();
    list.addIterator(num_1);
    list.addIterator(num_2);
    
    final TypedIteratorList clone = list.getClone(mock(QueryContext.class));
    assertEquals(2, clone.iterators().size());
    assertSame(num_1_clone, clone.iterators().get(0));
    assertSame(num_2_clone, clone.iterators().get(1));
  }
}
