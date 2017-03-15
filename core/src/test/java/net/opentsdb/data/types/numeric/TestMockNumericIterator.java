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
package net.opentsdb.data.types.numeric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.iterators.IteratorStatus;

/**
 * Yes we are testing a mock. Gotta make sure it's happy.
 */
public class TestMockNumericIterator {

  private TimeSeriesId id;
  private List<List<MutableNumericType>> data;
  
  @Before
  public void before() throws Exception {
    id = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("Khalisi")
        .build();
    
    data = Lists.newArrayListWithCapacity(3);
    
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id, new MillisecondTimeStamp(1000), 1, 1));
    set.add(new MutableNumericType(id, new MillisecondTimeStamp(2000), 2, 1));
    set.add(new MutableNumericType(id, new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id, new MillisecondTimeStamp(4000), 4, 1));
    set.add(new MutableNumericType(id, new MillisecondTimeStamp(5000), 5, 1));
    set.add(new MutableNumericType(id, new MillisecondTimeStamp(6000), 6, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(id, new MillisecondTimeStamp(7000), 7, 1));
    data.add(set);
  }
  
//  @Test
//  public void standAlone() throws Exception {
//    MockNumericIterator it = new MockNumericIterator(id);
//    it.data = data;
//    
//    assertNull(it.initialize().join());
//    assertSame(id, it.id());
//    
//    int i = 1;
//    assertEquals(IteratorStatus.HAS_DATA, it.status());
//    while (it.status() == IteratorStatus.HAS_DATA) {
//      final TimeSeriesValue<NumericType> v = it.next();
//      assertEquals(i, v.timestamp().epoch());
//      assertTrue(v.value().isInteger());
//      assertEquals(i, v.value().longValue());
//      assertEquals(1, v.realCount());
//      i++;
//    }
//    assertEquals(IteratorStatus.END_OF_CHUNK, it.status());
//    assertNull(it.fetchNext().join());
//    
//    assertEquals(IteratorStatus.HAS_DATA, it.status());
//    while (it.status() == IteratorStatus.HAS_DATA) {
//      final TimeSeriesValue<NumericType> v = it.next();
//      assertEquals(i, v.timestamp().epoch());
//      assertTrue(v.value().isInteger());
//      assertEquals(i, v.value().longValue());
//      assertEquals(1, v.realCount());
//      i++;
//    }
//    
//    assertEquals(IteratorStatus.END_OF_CHUNK, it.status());
//    assertNull(it.fetchNext().join());
//    
//    assertEquals(IteratorStatus.HAS_DATA, it.status());
//    while (it.status() == IteratorStatus.HAS_DATA) {
//      final TimeSeriesValue<NumericType> v = it.next();
//      assertEquals(i, v.timestamp().epoch());
//      assertTrue(v.value().isInteger());
//      assertEquals(i, v.value().longValue());
//      assertEquals(1, v.realCount());
//      i++;
//    }
//    assertEquals(IteratorStatus.END_OF_CHUNK, it.status());
//    assertNull(it.fetchNext().join());
//    assertEquals(IteratorStatus.END_OF_DATA, it.status());
//    assertNull(it.close().join());
//  }

  @Test
  public void initializeException() throws Exception {
    final MockNumericIterator it = new MockNumericIterator(id);
    it.data = data;
    it.ex = new RuntimeException("Boo!");
    try {
      it.initialize().join();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
  }
  
  @Test
  public void nextException() throws Exception {
    final MockNumericIterator it = new MockNumericIterator(id);
    it.data = data;
    it.ex = new RuntimeException("Boo!");
    try {
      it.next();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
  }
  
  @Test
  public void fetchNextException() throws Exception {
    final MockNumericIterator it = new MockNumericIterator(id);
    it.data = data;
    it.ex = new RuntimeException("Boo!");
    try {
      it.fetchNext().join();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
  }
  
//  @Test
//  public void getCopy() throws Exception {
//    final MockNumericIterator it = new MockNumericIterator(id);
//    it.data = data;
//    
//    assertNull(it.initialize().join());
//    assertSame(id, it.id());
//    
//    int i = 1;
//    assertEquals(IteratorStatus.HAS_DATA, it.status());
//    while (it.status() == IteratorStatus.HAS_DATA) {
//      final TimeSeriesValue<NumericType> v = it.next();
//      assertEquals(i, v.timestamp().epoch());
//      assertTrue(v.value().isInteger());
//      assertEquals(i, v.value().longValue());
//      assertEquals(1, v.realCount());
//      i++;
//    }
//    assertEquals(IteratorStatus.END_OF_CHUNK, it.status());
//    // left the parent in an END_OF_CHUNK state to verify the copy starts over.
//    
//    final MockNumericIterator copy = (MockNumericIterator) it.getCopy();
//    assertSame(id, copy.id());
//    
//    i = 1;
//    assertEquals(IteratorStatus.HAS_DATA, copy.status());
//    while (copy.status() == IteratorStatus.HAS_DATA) {
//      final TimeSeriesValue<NumericType> v = copy.next();
//      assertEquals(i, v.timestamp().epoch());
//      assertTrue(v.value().isInteger());
//      assertEquals(i, v.value().longValue());
//      assertEquals(1, v.realCount());
//      i++;
//    }
//    assertEquals(IteratorStatus.END_OF_CHUNK, copy.status());
//  }
  
  @Test
  public void closeException() throws Exception {
    final MockNumericIterator it = new MockNumericIterator(id);
    it.data = data;
    it.ex = new RuntimeException("Boo!");
    try {
      it.close().join();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
  }
  
  // NOTE That the processor tests are under the TestIteratorGroup class. No need
  // to duplicate em.
}
