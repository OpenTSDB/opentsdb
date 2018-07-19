// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import net.opentsdb.data.TimeSeriesDatum;

public class TestTimeSeriesSharedTagsAndTimeData {

  @Test
  public void fromCollection() throws Exception {
    TimeStamp ts = new SecondTimeStamp(1262304000);
    Map<String, String> tags = ImmutableMap.<String, String>builder()
        .put("host", "web01")
        .build();
    
    TimeSeriesDatum datum_a = mock(TimeSeriesDatum.class);
    TimeSeriesDatumStringId id_a = mock(TimeSeriesDatumStringId.class);
    TimeSeriesValue value_a = mock(TimeSeriesValue.class);
    TimeSeriesDataType type_a = mock(TimeSeriesDataType.class);
    TimeSeriesDatum datum_b = mock(TimeSeriesDatum.class);
    TimeSeriesDatumStringId id_b = mock(TimeSeriesDatumStringId.class);
    TimeSeriesValue value_b = mock(TimeSeriesValue.class);
    TimeSeriesDataType type_b = mock(TimeSeriesDataType.class);
    
    when(datum_a.value()).thenReturn(value_a);
    when(value_a.value()).thenReturn(type_a);
    when(value_a.timestamp()).thenReturn(ts);
    when(id_a.tags()).thenReturn(tags);
    when(id_a.metric()).thenReturn("sys.cpu.user");
    when(datum_a.id()).thenReturn(id_a);
    
    when(datum_b.value()).thenReturn(value_b);
    when(value_b.value()).thenReturn(type_b);
    when(value_b.timestamp()).thenReturn(ts);
    when(id_b.tags()).thenReturn(tags);
    when(id_b.metric()).thenReturn("sys.cpu.sys");
    when(datum_b.id()).thenReturn(id_b);
    
    List<TimeSeriesDatum> data = Lists.newArrayList();
    data.add(datum_a);
    data.add(datum_b);
    
    TimeSeriesSharedTagsAndTimeData shared = 
        TimeSeriesSharedTagsAndTimeData.fromCollection(data);
    Iterator<TimeSeriesDatum> iterator = shared.iterator();
    assertTrue(iterator.hasNext());
    assertSame(data.get(0), iterator.next());
    
    assertTrue(iterator.hasNext());
    assertSame(data.get(1), iterator.next());
    
    assertFalse(iterator.hasNext());
    
    // methods
    assertSame(ts, shared.timestamp());
    assertSame(tags, shared.tags());
    assertEquals(2, shared.data().size());
    assertSame(type_a, shared.data().get("sys.cpu.user").iterator().next());
    assertSame(type_b, shared.data().get("sys.cpu.sys").iterator().next());
    
    // diff ts
    TimeStamp ts2 = new SecondTimeStamp(1262304060);
    when(value_b.timestamp()).thenReturn(ts2);
    data = Lists.newArrayList();
    data.add(datum_a);
    data.add(datum_b);
    try {
      TimeSeriesSharedTagsAndTimeData.fromCollection(data);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    when(value_b.timestamp()).thenReturn(ts);
    
    // diff tags
    Map<String, String> tags2 = ImmutableMap.<String, String>builder()
        .put("host", "web02")
        .build();
    when(id_b.tags()).thenReturn(tags2);
    try {
      TimeSeriesSharedTagsAndTimeData.fromCollection(data);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    when(id_b.tags()).thenReturn(tags);

    // null second value
    when(datum_b.value()).thenReturn(null);
    try {
      TimeSeriesSharedTagsAndTimeData.fromCollection(data);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    when(datum_b.value()).thenReturn(value_b);
    
    // null first value
    when(datum_a.value()).thenReturn(null);
    try {
      TimeSeriesSharedTagsAndTimeData.fromCollection(data);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    when(datum_a.value()).thenReturn(value_a);
    
    // nulls for types are ok though
    when(value_b.value()).thenReturn(null);
    shared = TimeSeriesSharedTagsAndTimeData.fromCollection(data);
    iterator = shared.iterator();
    assertTrue(iterator.hasNext());
    assertSame(data.get(0), iterator.next());
    
    assertTrue(iterator.hasNext());
    assertSame(data.get(1), iterator.next());
    
    assertFalse(iterator.hasNext());
    
    // empty
    data.clear();
    try {
      TimeSeriesSharedTagsAndTimeData.fromCollection(data);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TimeSeriesSharedTagsAndTimeData.fromCollection(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
}
