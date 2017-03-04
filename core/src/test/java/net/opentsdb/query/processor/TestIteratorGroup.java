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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.MockNumericIterator;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;

public class TestIteratorGroup {

  private TimeSeriesGroupId group_id;
  private TimeSeriesId id_a;
  private TimeSeriesId id_b;
  
  private List<List<MutableNumericType>> data_a;
  private List<List<MutableNumericType>> data_b;
  
  private MockNumericIterator it_a;
  private MockNumericIterator it_b;
  
  @Before
  public void before() throws Exception {
    group_id = new SimpleStringGroupId("Dothraki");
    id_a = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("Khaleesi")
        .build();
    id_b = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("Khalasar")
        .build();
    
    data_a = Lists.newArrayListWithCapacity(2);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(1000), 1, 1));
    //set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(2000), 2, 1));
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(3000), 3, 1));
    data_a.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(4000), 4, 1));
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(5000), 5, 1));
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(6000), 6, 1));
    data_a.add(set);

    data_b = Lists.newArrayListWithCapacity(2);
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(1000), 1, 1));
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(2000), 2, 1));
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(3000), 3, 1));
    data_b.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(4000), 4, 1));
    //set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(5000), 5, 1));
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(6000), 6, 1));
    data_b.add(set);
    
    it_a = spy(new MockNumericIterator(id_a));
    it_a.data = data_a;
    
    it_b = spy(new MockNumericIterator(id_b));
    it_b.data = data_b;
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void standardOperation() throws Exception {
    assertNull(it_a.processor);
    assertNull(it_b.processor);
    
    final TimeSeriesProcessor group = new IteratorGroup();
    assertEquals(Long.MAX_VALUE, group.syncTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
    assertTrue(group.iterators().isEmpty());
    
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    assertSame(group, it_a.processor);
    assertSame(group, it_b.processor);
    assertEquals(1000, group.syncTimestamp().msEpoch());
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    assertNull(group.initialize().join());
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    assertEquals(1, iterators.size());
    assertEquals(2, iterators.get(group_id).get(NumericType.TYPE).size());
    // ordering maintained.
    assertSame(id_a, iterators.get(group_id).get(NumericType.TYPE).get(0).id());
    assertSame(id_b, iterators.get(group_id).get(NumericType.TYPE).get(1).id());
    
    // lets iterate!
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    // end of chunk, fetch next
    assertEquals(IteratorStatus.END_OF_CHUNK, group.status());
    assertNull(group.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    // end of data
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
    assertNull(group.close().join());
    
    verify(it_a, times(1)).close();
    verify(it_b, times(1)).close();
    
    verify(it_a, times(1)).fetchNext();
    verify(it_b, times(1)).fetchNext();
    
    verify(it_a, never()).initialize();
    verify(it_b, never()).initialize();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void chunkEndedEarlyBothRecover() throws Exception {
    assertNull(it_a.processor);
    assertNull(it_b.processor);
    
    data_a = Lists.newArrayListWithCapacity(2);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(1000), 1, 1));
    //set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(2000), 2, 1));
    //set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(3000), 3, 1));
    data_a.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(4000), 4, 1));
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(5000), 5, 1));
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(6000), 6, 1));
    data_a.add(set);
    it_a.data = data_a;
    
    final TimeSeriesProcessor group = new IteratorGroup();
    assertEquals(Long.MAX_VALUE, group.syncTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
    assertTrue(group.iterators().isEmpty());
    
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    assertSame(group, it_a.processor);
    assertSame(group, it_b.processor);
    assertEquals(1000, group.syncTimestamp().msEpoch());
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    assertNull(group.initialize().join());
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    assertEquals(1, iterators.size());
    assertEquals(2, iterators.get(group_id).get(NumericType.TYPE).size());
    // ordering maintained.
    assertSame(id_a, iterators.get(group_id).get(NumericType.TYPE).get(0).id());
    assertSame(id_b, iterators.get(group_id).get(NumericType.TYPE).get(1).id());

    // lets iterate!
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    // end of chunk, fetch next
    assertEquals(IteratorStatus.END_OF_CHUNK, group.status());
    assertNull(group.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    // end of data
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
    assertNull(group.close().join());
    
    verify(it_a, times(1)).close();
    verify(it_b, times(1)).close();
    
    verify(it_a, times(1)).fetchNext();
    verify(it_b, times(1)).fetchNext();
    
    verify(it_a, never()).initialize();
    verify(it_b, never()).initialize();
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void chunkEndedEarlySameRecoversOtherUnaligned() throws Exception {
    assertNull(it_a.processor);
    assertNull(it_b.processor);
    
    data_a = Lists.newArrayListWithCapacity(2);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(1000), 1, 1));
    //set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(2000), 2, 1));
    //set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(3000), 3, 1));
    data_a.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(4000), 4, 1));
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(5000), 5, 1));
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(6000), 6, 1));
    data_a.add(set);
    it_a.data = data_a;
    
    data_b = Lists.newArrayListWithCapacity(2);
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(1000), 1, 1));
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(2000), 2, 1));
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(3000), 3, 1));
    data_b.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    //set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(4000), 4, 1));
    //set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(5000), 5, 1));
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(6000), 6, 1));
    data_b.add(set);
    it_b.data = data_b;
    
    final TimeSeriesProcessor group = new IteratorGroup();
    assertEquals(Long.MAX_VALUE, group.syncTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
    assertTrue(group.iterators().isEmpty());
    
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    assertSame(group, it_a.processor);
    assertSame(group, it_b.processor);
    assertEquals(1000, group.syncTimestamp().msEpoch());
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    assertNull(group.initialize().join());
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    assertEquals(1, iterators.size());
    assertEquals(2, iterators.get(group_id).get(NumericType.TYPE).size());
    // ordering maintained.
    assertSame(id_a, iterators.get(group_id).get(NumericType.TYPE).get(0).id());
    assertSame(id_b, iterators.get(group_id).get(NumericType.TYPE).get(1).id());

    // lets iterate!
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    // end of chunk, fetch next
    assertEquals(IteratorStatus.END_OF_CHUNK, group.status());
    assertNull(group.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    // end of data
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
    assertNull(group.close().join());
    
    verify(it_a, times(1)).close();
    verify(it_b, times(1)).close();
    
    verify(it_a, times(1)).fetchNext();
    verify(it_b, times(1)).fetchNext();
    
    verify(it_a, never()).initialize();
    verify(it_b, never()).initialize();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void chunkEndedEarlyOtherRecoversSameUnaligned() throws Exception {
    assertNull(it_a.processor);
    assertNull(it_b.processor);
    
    data_a = Lists.newArrayListWithCapacity(2);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(1000), 1, 1));
    //set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(2000), 2, 1));
    //set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(3000), 3, 1));
    data_a.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    //set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(4000), 4, 1));
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(5000), 5, 1));
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(6000), 6, 1));
    data_a.add(set);
    it_a.data = data_a;
    
    final TimeSeriesProcessor group = new IteratorGroup();
    assertEquals(Long.MAX_VALUE, group.syncTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
    assertTrue(group.iterators().isEmpty());
    
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    assertSame(group, it_a.processor);
    assertSame(group, it_b.processor);
    assertEquals(1000, group.syncTimestamp().msEpoch());
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    assertNull(group.initialize().join());
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    assertEquals(1, iterators.size());
    assertEquals(2, iterators.get(group_id).get(NumericType.TYPE).size());
    // ordering maintained.
    assertSame(id_a, iterators.get(group_id).get(NumericType.TYPE).get(0).id());
    assertSame(id_b, iterators.get(group_id).get(NumericType.TYPE).get(1).id());

    // lets iterate!
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue())); // NaN fill!
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    // end of chunk, fetch next
    assertEquals(IteratorStatus.END_OF_CHUNK, group.status());
    assertNull(group.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(4, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(6, v.value().longValue());
    
    // end of data
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
    assertNull(group.close().join());
    
    verify(it_a, times(1)).close();
    verify(it_b, times(1)).close();
    
    verify(it_a, times(1)).fetchNext();
    verify(it_b, times(1)).fetchNext();
    
    verify(it_a, never()).initialize();
    verify(it_b, never()).initialize();
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state1() throws Exception {
    ProcessorTestsHelpers.setState1(it_a, it_b);
    final TimeSeriesProcessor group = new IteratorGroup();
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state2() throws Exception {
    ProcessorTestsHelpers.setState2(it_a, it_b);
    final TimeSeriesProcessor group = new IteratorGroup();
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state3() throws Exception {
    ProcessorTestsHelpers.setState3(it_a, it_b);
    
    final TimeSeriesProcessor group = new IteratorGroup();
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());

    assertEquals(IteratorStatus.END_OF_DATA, group.status());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state4() throws Exception {
    ProcessorTestsHelpers.setState4(it_a, it_b);
    
    final TimeSeriesProcessor group = new IteratorGroup();
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());

    assertEquals(IteratorStatus.END_OF_DATA, group.status());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state5() throws Exception {
    ProcessorTestsHelpers.setState5(it_a, it_b);
    
    final TimeSeriesProcessor group = new IteratorGroup();
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());

    assertEquals(IteratorStatus.END_OF_DATA, group.status());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state6() throws Exception {
    ProcessorTestsHelpers.setState6(it_a, it_b);
    
    final TimeSeriesProcessor group = new IteratorGroup();
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    group.fetchNext();
    assertEquals(IteratorStatus.END_OF_CHUNK, group.status());
    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());

    assertEquals(IteratorStatus.END_OF_DATA, group.status());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state7() throws Exception {
    ProcessorTestsHelpers.setState7(it_a, it_b);
    
    final TimeSeriesProcessor group = new IteratorGroup();
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    group.fetchNext();
    assertEquals(IteratorStatus.END_OF_CHUNK, group.status());
    group.fetchNext();
    assertEquals(IteratorStatus.END_OF_CHUNK, group.status());
    // just takes an extra call.
    group.fetchNext();
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state8() throws Exception {
    ProcessorTestsHelpers.setState8(it_a, it_b);
    
    final TimeSeriesProcessor group = new IteratorGroup();
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    
    assertEquals(IteratorStatus.END_OF_CHUNK, group.status());
    group.fetchNext();
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    group.fetchNext();
    assertEquals(IteratorStatus.END_OF_CHUNK, group.status());
    group.fetchNext();
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state9() throws Exception {
    ProcessorTestsHelpers.setState9(it_a, it_b);
    
    final TimeSeriesProcessor group = new IteratorGroup();
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    
    assertEquals(IteratorStatus.END_OF_CHUNK, group.status());
    group.fetchNext();
    assertEquals(IteratorStatus.END_OF_CHUNK, group.status());
    group.fetchNext();
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));

    group.fetchNext();
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state10() throws Exception {
    ProcessorTestsHelpers.setState10(it_a, it_b);
    
    final TimeSeriesProcessor group = new IteratorGroup();
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));

    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void state11() throws Exception {
    ProcessorTestsHelpers.setState11(it_a, it_b);
    
    final TimeSeriesProcessor group = new IteratorGroup();
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.END_OF_CHUNK, group.status());
    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_CHUNK, group.status());
    group.fetchNext();
    group.next();
    v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void exceptionStatusOnNext() throws Exception {
    final TimeSeriesProcessor group = new IteratorGroup();

    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();

    // lets iterate!
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    // inject an exception
    it_b.ex = new RuntimeException("Boo!");
    group.next();
    assertEquals(IteratorStatus.EXCEPTION, group.status());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void exceptionThrowOnNext() throws Exception {
    final TimeSeriesProcessor group = new IteratorGroup();

    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();

    // lets iterate!
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    // inject an exception
    it_b.ex = new RuntimeException("Boo!");
    it_b.throw_ex = true;
    group.next();
    assertEquals(IteratorStatus.EXCEPTION, group.status());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void exceptionStatusOnIteratorNext() throws Exception {
    final TimeSeriesProcessor group = new IteratorGroup();

    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();

    // lets iterate!
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    // inject an exception
    it_b.ex = new RuntimeException("Boo!");
    
    try {
      iterators.get(group_id).get(NumericType.TYPE).get(1).next();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      assertSame(e, it_b.ex);
    }
    
    // still has data as the next() call doesn't modify the processor.
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    assertEquals(IteratorStatus.EXCEPTION, group.status());
  }

  @Test
  public void addSeries() throws Exception {
    final TimeSeriesGroupId id_b = new SimpleStringGroupId("Exiles");
    TimeSeriesProcessor group = new IteratorGroup();
    group.addSeries(group_id, it_a);
    group.addSeries(id_b, it_b);
    
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();
    
    assertEquals(1, iterators.get(group_id).get(NumericType.TYPE).size());
    assertEquals(1, iterators.get(id_b).get(NumericType.TYPE).size());
    
    // add the same
    try {
      group.addSeries(group_id, it_a);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    group = new IteratorGroup();
    try {
      group.addSeries(null, it_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    group = new IteratorGroup();
    try {
      group.addSeries(group_id, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void markStatus() throws Exception {
    final TimeSeriesProcessor group = new IteratorGroup();
    assertEquals(IteratorStatus.END_OF_DATA, group.status());
    group.markStatus(IteratorStatus.HAS_DATA);
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    
    try {
      group.markStatus(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void setSyncTime() throws Exception {
    final TimeSeriesProcessor group = new IteratorGroup();
    try {
      group.setSyncTime(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void fetchNextException() throws Exception {
    final TimeSeriesProcessor group = new IteratorGroup();
    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    it_b.ex = new RuntimeException("Boo!");
    
    try {
      group.fetchNext().join();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
    assertEquals(IteratorStatus.EXCEPTION, group.status());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void getCopy() throws Exception {
    final TimeSeriesProcessor group = new IteratorGroup();

    // add series
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators = group.iterators();

    // lets iterate!
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    group.next();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
          iterators.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        iterators.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    
    // copy!
    final TimeSeriesProcessor copy = group.getCopy();
    final Map<TimeSeriesGroupId, Map<TypeToken<?>, 
    List<TimeSeriesIterator<?>>>> its_copy = copy.iterators();
    
    assertEquals(IteratorStatus.HAS_DATA, copy.status());
    copy.next();
    v = (TimeSeriesValue<NumericType>) 
        its_copy.get(group_id).get(NumericType.TYPE).get(0).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) 
        its_copy.get(group_id).get(NumericType.TYPE).get(1).next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(IteratorStatus.HAS_DATA, group.status());
    
    assertNotSame(iterators.get(group_id).get(NumericType.TYPE).get(0), 
        its_copy.get(group_id).get(NumericType.TYPE).get(0));
    assertNotSame(iterators.get(group_id).get(NumericType.TYPE).get(1), 
        its_copy.get(group_id).get(NumericType.TYPE).get(1));
  }
  
  @Test
  public void closeException() throws Exception {
    final TimeSeriesProcessor group = new IteratorGroup();
    group.addSeries(group_id, it_a);
    group.addSeries(group_id, it_b);
    
    it_a.ex = new RuntimeException("Boo!");
    try {
      group.close().join();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
  }
}
