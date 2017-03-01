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
// see <http://www.gnu.org/licenses/>.package net.opentsdb.data;
package net.opentsdb.query.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;

public class TestAbstractBaseProcessor {
  
  private TimeSeriesProcessor source;
  private TimeSeriesProcessorConfig<TestImp> config;
  private TimeStamp max;
  
  @Before
  public void before() throws Exception {
    source = mock(TimeSeriesProcessor.class);
    config = new TestConfig();
    max = new MillisecondTimeStamp(0);
    max.setMax();
  }

  @Test
  public void ctor() throws Exception {
    TestImp processor = new TestImp(source, null);
    assertSame(source, processor.source());
    assertNull(processor.config());
    
    processor = new TestImp(source, config);
    assertSame(source, processor.source());
    assertSame(config, processor.config());
    
    try {
      new TestImp(null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void initialize() throws Exception {
    final TestImp processor = new TestImp(source, null);
    verify(source, never()).initialize();
    processor.initialize();
    verify(source, times(1)).initialize();
  }
  
  @Test
  public void iterators() throws Exception {
    final TestImp processor = new TestImp(source, null);
    assertNull(processor.iterators());
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void addSeries() throws Exception {
    final TestImp processor = new TestImp(source, null);
    processor.addSeries(mock(TimeSeriesGroupId.class), 
        mock(TimeSeriesIterator.class));
  }
  
  @Test
  public void syncTimestamp() throws Exception {
    final TestImp processor = new TestImp(source, null);
    processor.syncTimestamp();
    verify(source, never()).syncTimestamp();
    assertEquals(max.msEpoch(), processor.syncTimestamp().msEpoch());
  }
  
  @Test
  public void status() throws Exception {
    final TestImp processor = new TestImp(source, null);
    processor.status();
    verify(source, never()).status();
    assertEquals(IteratorStatus.END_OF_DATA, processor.status());
  }
  
  @Test
  public void next() throws Exception {
    final TestImp processor = new TestImp(source, null);
    processor.next();
    verify(source, times(1)).next();
  }
  
  @Test
  public void markStatus() throws Exception {
    final TestImp processor = new TestImp(source, null);
    assertEquals(IteratorStatus.END_OF_DATA, processor.status());
    processor.markStatus(IteratorStatus.HAS_DATA);
    verify(source, never()).markStatus(IteratorStatus.HAS_DATA);
    assertEquals(IteratorStatus.HAS_DATA, processor.status());
  }
  
  @Test
  public void setSyncTime() throws Exception {
    final TestImp processor = new TestImp(source, null);
    assertEquals(max.msEpoch(), processor.next_sync_time.msEpoch());
    assertEquals(max.msEpoch(), processor.syncTimestamp().msEpoch());
    
    TimeStamp ts = new MillisecondTimeStamp(1000);
    processor.setSyncTime(ts);    
    verify(source, never()).setSyncTime(ts);
    assertEquals(ts.msEpoch(), processor.next_sync_time.msEpoch());
    assertEquals(max.msEpoch(), processor.syncTimestamp().msEpoch());
  }
  
  @Test
  public void fetchNext() throws Exception {
    final TestImp processor = new TestImp(source, null);
    processor.fetchNext();
    verify(source, times(1)).fetchNext();
  }
  
  @Test
  public void close() throws Exception {
    final TestImp processor = new TestImp(source, null);
    processor.close();
    verify(source, times(1)).close();
  }
  
  @Test
  public void getCopy() throws Exception {
    final TestImp processor = new TestImp(source, null);
    assertNull(processor.getCopyParent());
    
    final TimeSeriesProcessor copy = processor.getCopy();
    assertSame(processor, copy.getCopyParent());
  }
  
  class TestImp extends AbstractBaseProcessor<TestImp> {

    public TestImp(TimeSeriesProcessor source,
        TimeSeriesProcessorConfig<TestImp> config) {
      super(source, config);
    }
    
    private TestImp(TimeSeriesProcessor source,
        TimeSeriesProcessorConfig<TestImp> config, TimeSeriesProcessor parent) {
      super(source, config, parent);
    }

    @Override
    public TimeSeriesProcessor getCopy() {
      return new TestImp(source, config, this);
    }
    
    TimeSeriesProcessor source() {
      return source;
    }
    
    TimeSeriesProcessorConfig<TestImp> config() {
      return config;
    }
  }
  
  class TestConfig implements TimeSeriesProcessorConfig<TestImp> {
    
  }
}
