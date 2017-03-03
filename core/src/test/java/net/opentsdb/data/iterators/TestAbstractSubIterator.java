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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.processor.TimeSeriesProcessor;

public class TestAbstractSubIterator {

  private TimeSeriesId id;
  private TimeSeriesIterator<?> source;
  private TimeStamp timestamp;
  
  @Before
  public void before() throws Exception {
    id = mock(TimeSeriesId.class);
    source = mock(TimeSeriesIterator.class);
    timestamp = mock(TimeStamp.class);
    
    when(source.id()).thenReturn(id);
    when(source.initialize()).thenReturn(Deferred.fromResult(null));
    when(source.status()).thenReturn(IteratorStatus.END_OF_DATA);
    when(source.nextTimestamp()).thenReturn(timestamp);
    when(source.fetchNext()).thenReturn(Deferred.fromResult(null));
    when(source.close()).thenReturn(Deferred.fromResult(null));
  }
  
  @Test
  public void ctor() throws Exception {
    MockIterator it = new MockIterator(source);
    assertSame(source, it.source);
    assertNull(it.processor);
    assertNull(it.parent_copy);
    
    it = new MockIterator(null);
    assertNull(it.source);
    assertNull(it.processor);
    assertNull(it.parent_copy);
  }
  
  @Test
  public void initialize() throws Exception {
    MockIterator it = new MockIterator(source);
    Deferred<Object> deferred = it.initialize();
    assertNull(deferred.join());
    verify(source, times(1)).initialize();
    
    it = new MockIterator(null);
    deferred = it.initialize();
    try {
      deferred.join();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
  
  @Test
  public void id() throws Exception {
    MockIterator it = new MockIterator(source);
    assertSame(id, it.id());
    
    it = new MockIterator(null);
    try {
      it.id();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
  
  @Test
  public void setProcessor() throws Exception {
    final TimeSeriesProcessor proc = mock(TimeSeriesProcessor.class);
    final MockIterator it = new MockIterator(source);
    it.setProcessor(proc);
    assertSame(proc, it.processor);
  }
  
  @Test
  public void status() throws Exception {
    MockIterator it = new MockIterator(source);
    assertEquals(IteratorStatus.END_OF_DATA, it.status());
    
    it = new MockIterator(null);
    try {
      it.status();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
  
  @Test
  public void next() throws Exception {
    final MockIterator it = new MockIterator(source);
    it.next();
    verify(source, times(1)).next();
  }
  
  @Test
  public void advance() throws Exception {
    MockIterator it = new MockIterator(source);
    it.advance();
    verify(source, times(1)).advance();
    
    it = new MockIterator(null);
    try {
      it.advance();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
  
  @Test
  public void nextTimestamp() throws Exception {
    MockIterator it = new MockIterator(source);
    assertSame(timestamp, it.nextTimestamp());
    
    it = new MockIterator(null);
    try {
      it.nextTimestamp();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }

  @Test
  public void fetchNext() throws Exception {
    MockIterator it = new MockIterator(source);
    Deferred<Object> deferred = it.fetchNext();
    assertNull(deferred.join());
    verify(source, times(1)).fetchNext();
    
    it = new MockIterator(null);
    deferred = it.fetchNext();
    try {
      deferred.join();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
  
  @Test
  public void getCopy() throws Exception {
    final MockIterator it = new MockIterator(source);
    final TimeSeriesIterator<?> copy = it.getCopy();
    assertNotSame(copy, it);
    assertSame(it, copy.getCopyParent());
  }
  
  @Test
  public void close() throws Exception {
    MockIterator it = new MockIterator(source);
    Deferred<Object> deferred = it.close();
    assertNull(deferred.join());
    verify(source, times(1)).close();
    
    it = new MockIterator(null);
    deferred = it.close();
    try {
      deferred.join();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
  
  /**
   * Mock implementation for testing.
   */
  static class MockIterator extends AbstractSubIterator<TimeSeriesValue<NumericType>> {

    public MockIterator(TimeSeriesIterator<?> source) {
      super(source);
    }


    @Override
    public TypeToken<?> type() {
      return NumericType.TYPE;
    }

    @Override
    public TimeSeriesValue<?> next() {
      return source.next();
    }

    @Override
    public TimeSeriesIterator<TimeSeriesValue<?>> getCopy() {
      final MockIterator copy = new MockIterator(
          source != null ? source.getCopy() : null);
      copy.parent_copy = this;
      return copy;
    }
    
  }
}
