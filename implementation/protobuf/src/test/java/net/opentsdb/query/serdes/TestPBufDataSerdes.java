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
package net.opentsdb.query.serdes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.ZoneId;
import java.util.Iterator;

import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.BaseTimeSeriesDatumStringId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesDatumStringId;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.ZonedNanoTimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;

public class TestPBufDataSerdes {

  @Test
  public void serdesDatum() throws Exception {
    PBufDataSerdes serdes = new PBufDataSerdes();
    
    TimeSeriesDatumStringId id = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    MutableNumericValue value = new MutableNumericValue(
        new SecondTimeStamp(1262304000), 42);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serdes.serialize(null, TimeSeriesDatum.wrap(id, value), baos, null);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    
    TimeSeriesDatum des = serdes.deserializeDatum(null, bais, null);
    assertEquals("sys.cpu.user", ((TimeSeriesDatumStringId) des.id()).metric());
    assertEquals("web01", ((TimeSeriesDatumStringId) des.id()).tags().get("host"));
    assertEquals("tyrion", ((TimeSeriesDatumStringId) des.id()).tags().get("owner"));
    assertEquals(1262304000, des.value().timestamp().epoch());
    assertEquals(42, ((TimeSeriesValue<NumericType>) des.value()).value().longValue());
    
    // double
    value = new MutableNumericValue(
        new ZonedNanoTimeStamp(1262304000, 500, ZoneId.of("America/Denver")), 0.00425);
    new ByteArrayOutputStream();
    serdes.serialize(null, TimeSeriesDatum.wrap(id, value), baos, null);
    bais = new ByteArrayInputStream(baos.toByteArray());
    des = serdes.deserializeDatum(null, bais, null);
    assertEquals("sys.cpu.user", ((TimeSeriesDatumStringId) des.id()).metric());
    assertEquals("web01", ((TimeSeriesDatumStringId) des.id()).tags().get("host"));
    assertEquals("tyrion", ((TimeSeriesDatumStringId) des.id()).tags().get("owner"));
    assertEquals(1262304000, des.value().timestamp().epoch());
    assertEquals(500, des.value().timestamp().nanos());
    assertEquals(ZoneId.of("America/Denver"), des.value().timestamp().timezone());
    assertEquals(0.00425, ((TimeSeriesValue<NumericType>) des.value()).value().doubleValue(), 0.00001);
  }

  @Test
  public void serdesShared() throws Exception {
    PBufDataSerdes serdes = new PBufDataSerdes();
    
    TimeSeriesDatumStringId id_a = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    TimeSeriesDatumStringId id_b = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric("sys.cpu.sys")
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    TimeSeriesDatumStringId id_c = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric("sys.cpu.idle")
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    MutableNumericValue value_a = new MutableNumericValue(
        new SecondTimeStamp(1262304000), 42);
    MutableNumericValue value_b = new MutableNumericValue(
        new SecondTimeStamp(1262304000), 42.75);
    MutableNumericValue value_c = new MutableNumericValue(
        new SecondTimeStamp(1262304000), -1024);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serdes.serialize(null, TimeSeriesSharedTagsAndTimeData.fromCollection(
        Lists.newArrayList(
            TimeSeriesDatum.wrap(id_a, value_a), 
            TimeSeriesDatum.wrap(id_b, value_b), 
            TimeSeriesDatum.wrap(id_c, value_c))
        ), baos, null);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    
    TimeSeriesSharedTagsAndTimeData des = serdes.deserializeShared(null, bais, null);
    assertEquals(1262304000, des.timestamp().epoch());
    assertEquals(2, des.tags().size());
    assertEquals("web01", des.tags().get("host"));
    assertEquals("tyrion", des.tags().get("owner"));
    
    assertEquals(42, ((NumericType) des.data().get("sys.cpu.user")
        .iterator().next()).longValue());
    assertEquals(42.75, ((NumericType) des.data().get("sys.cpu.sys")
        .iterator().next()).doubleValue(), 0.0001);
    assertEquals(-1024, ((NumericType) des.data().get("sys.cpu.idle")
        .iterator().next()).longValue());
    
    // try the iterator
    // ArrayListMultimap gaurantees order, neat.
    Iterator<TimeSeriesDatum> iterator = des.iterator();
    assertTrue(iterator.hasNext());
    
    TimeSeriesDatum datum = iterator.next();
    assertEquals("sys.cpu.user", ((TimeSeriesDatumStringId) datum.id()).metric());
    assertEquals(2, ((TimeSeriesDatumStringId) datum.id()).tags().size());
    assertEquals("web01", ((TimeSeriesDatumStringId) datum.id()).tags().get("host"));
    assertEquals("tyrion", ((TimeSeriesDatumStringId) datum.id()).tags().get("owner"));
    assertEquals(1262304000, datum.value().timestamp().epoch());
    assertEquals(42, ((TimeSeriesValue<NumericType>) datum.value()).value().longValue());
    assertTrue(iterator.hasNext());
    
    datum = iterator.next();
    assertEquals("sys.cpu.sys", ((TimeSeriesDatumStringId) datum.id()).metric());
    assertEquals(2, ((TimeSeriesDatumStringId) datum.id()).tags().size());
    assertEquals("web01", ((TimeSeriesDatumStringId) datum.id()).tags().get("host"));
    assertEquals("tyrion", ((TimeSeriesDatumStringId) datum.id()).tags().get("owner"));
    assertEquals(1262304000, datum.value().timestamp().epoch());
    assertEquals(42.75, ((TimeSeriesValue<NumericType>) datum.value()).value().doubleValue(), 0.0001);
    assertTrue(iterator.hasNext());
    
    datum = iterator.next();
    assertEquals("sys.cpu.idle", ((TimeSeriesDatumStringId) datum.id()).metric());
    assertEquals(2, ((TimeSeriesDatumStringId) datum.id()).tags().size());
    assertEquals("web01", ((TimeSeriesDatumStringId) datum.id()).tags().get("host"));
    assertEquals("tyrion", ((TimeSeriesDatumStringId) datum.id()).tags().get("owner"));
    assertEquals(1262304000, datum.value().timestamp().epoch());
    assertEquals(-1024, ((TimeSeriesValue<NumericType>) datum.value()).value().longValue());
    assertFalse(iterator.hasNext());
  }
  
}
