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
package net.opentsdb.data.types.numeric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;

public class TestUglyByteNumericSerdes {

  private TimeStamp start;
  private TimeStamp end;
  
  @Before
  public void before() throws Exception {
    start = new MillisecondTimeStamp(1486045800000L);
    end = new MillisecondTimeStamp(1486046000000L);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void fullSerdes() throws Exception {
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setAlias("a")
        .setMetric("sys.cpu.user")
        .addTags("host", "web01")
        .addTags("dc", "phx")
    .build();
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end);
    shard.add(1486045801000L, 42);
    shard.add(1486045871000L, 9866.854);
    shard.add(1486045881000L, -128);
    
    final UglyByteNumericSerdes serdes = new UglyByteNumericSerdes();
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    serdes.serialize(null, null, output, shard);
    output.close();
    byte[] data = output.toByteArray();
    
    final ByteArrayInputStream input = new ByteArrayInputStream(data);
    final TimeSeriesIterator<?> iterator = serdes.deserialize(null, input);
    assertEquals(id, iterator.id());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    TimeSeriesValue<NumericType> v = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.0001);
    
    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
  }
  
  @Test
  public void emptyValues() throws Exception {
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setAlias("a")
        .setMetric("sys.cpu.user")
        .addTags("host", "web01")
        .addTags("dc", "phx")
    .build();
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end);
    
    final UglyByteNumericSerdes serdes = new UglyByteNumericSerdes();
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    serdes.serialize(null, null, output, shard);
    output.close();
    byte[] data = output.toByteArray();
    
    final ByteArrayInputStream input = new ByteArrayInputStream(data);
    final TimeSeriesIterator<?> iterator = serdes.deserialize(null, input);
    assertEquals(id, iterator.id());
    
    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
  }

  @Test
  public void exceptions() throws Exception {
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setAlias("a")
        .setMetric("sys.cpu.user")
        .addTags("host", "web01")
        .addTags("dc", "phx")
    .build();
    
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end);
    
    final UglyByteNumericSerdes serdes = new UglyByteNumericSerdes();
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    
    try {
      serdes.serialize(null, null, null, shard);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      serdes.serialize(null, null, output, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    NumericMillisecondShard mock = mock(NumericMillisecondShard.class);
    try {
      serdes.serialize(null, null, output, mock);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      serdes.deserialize(null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    final ByteArrayInputStream input = new ByteArrayInputStream(new byte[] { });
    try {
      // thrown by NumericType "Span cannot be negative."
      serdes.deserialize(null, input);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
