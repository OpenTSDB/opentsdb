/*
 * // This file is part of OpenTSDB.
 * // Copyright (C) 2021  The OpenTSDB Authors.
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //   http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */

package net.opentsdb.data.influx;

import com.google.common.collect.Lists;
import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesDatumStringId;
import net.opentsdb.data.MockLowLevelMetricData;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesDatumStringId;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.data.ZonedNanoTimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestInfluxLineProtocolConverter {

  @Test
  public void serializeDatum() throws Exception {
    TimeSeriesDatum datum = TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
              .setMetric("sys.cpu.user")
              .addTags("host", "web01")
              .addTags("dc", "DEN")
              .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123456789L, Const.UTC),
                    42));
    InfluxLineProtocolConverter converter = new InfluxLineProtocolConverter();
    byte[] buffer = new byte[converter.serializationSize(datum)];
    int written = converter.serialize(datum, buffer, 0);
    String line = new String(buffer);
    // hash map order is non-deterministic for tags so we have two possibilities.
    String expected1 = "sys.cpu,host=web01,dc=DEN user=42 1625668592123456789";
    String expected2 = "sys.cpu,dc=DEN,host=web01 user=42 1625668592123456789";
    boolean matched = line.equals(expected1);
    if (!matched) {
      assertEquals(line, expected2);
    }
    assertEquals(buffer.length, written);

    // stream version
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    written = converter.serialize(datum, stream);
    buffer = stream.toByteArray();
    matched = line.equals(expected1);
    if (!matched) {
      assertEquals(line, expected2);
    }
    assertEquals(buffer.length, written);

    datum = TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.cpu.user")
                    .addTags("host", "web01")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 0L, Const.UTC),
                    -52.42));
    buffer = new byte[converter.serializationSize(datum)];
    written = converter.serialize(datum, buffer, 0);
    line = new String(buffer);
    expected1 = "sys.cpu,host=web01,dc=DEN user=-52.42 1625668592000000000";
    expected2 = "sys.cpu,dc=DEN,host=web01 user=-52.42 1625668592000000000";
    matched = line.equals(expected1);
    if (!matched) {
      assertEquals(line, expected2);
    }
    assertEquals(buffer.length, written);

    stream = new ByteArrayOutputStream();
    written = converter.serialize(datum, stream);
    buffer = stream.toByteArray();
    matched = line.equals(expected1);
    if (!matched) {
      assertEquals(line, expected2);
    }
    assertEquals(buffer.length, written);

    datum = TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.cpu.user")
                    .addTags("host", "web01")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123L, Const.UTC),
                    42e-24));
    buffer = new byte[converter.serializationSize(datum)];
    written = converter.serialize(datum, buffer, 0);
    line = new String(buffer);
    expected1 = "sys.cpu,host=web01,dc=DEN user=4.2E-23 1625668592000000123";
    expected2 = "sys.cpu,dc=DEN,host=web01 user=4.2E-23 1625668592000000123";
    matched = line.equals(expected1);
    if (!matched) {
      assertEquals(line, expected2);
    }
    assertEquals(buffer.length, written);

    stream = new ByteArrayOutputStream();
    written = converter.serialize(datum, stream);
    buffer = stream.toByteArray();
    matched = line.equals(expected1);
    if (!matched) {
      assertEquals(line, expected2);
    }
    assertEquals(buffer.length, written);
  }

  @Test
  public void serializeSharedData() throws Exception {
    List<TimeSeriesDatum> list = Lists.newArrayList();
    list.add(TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.cpu.user")
                    .addTags("host", "web01")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123456789L, Const.UTC),
                    42)));
    list.add(TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.mem.avail")
                    .addTags("host", "web01")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123456789L, Const.UTC),
                    24)));
    list.add(TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.cpu.sys")
                    .addTags("host", "web01")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123456789L, Const.UTC),
                    2.12)));
    list.add(TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.cpu.idle")
                    .addTags("host", "web01")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123456789L, Const.UTC),
                    28)));
    list.add(TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.mem.slab")
                    .addTags("host", "web01")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123456789L, Const.UTC),
                    36.24)));
    TimeSeriesSharedTagsAndTimeData data =
            TimeSeriesSharedTagsAndTimeData.fromCollection(list);
    InfluxLineProtocolConverter converter = new InfluxLineProtocolConverter();
    byte[] buffer = new byte[converter.serializationSize(data)];
    int written = converter.serialize(data, buffer, 0);
    String ilp = new String(buffer, 0, written);

    String expA1 = "sys.cpu,host=web01,dc=DEN user=42,sys=2.12,idle=28 1625668592123456789";
    String expA2 = "sys.cpu,dc=DEN,host=web01 user=42,sys=2.12,idle=28 1625668592123456789";
    String expB1 = "sys.mem,host=web01,dc=DEN avail=24,slab=36.24 1625668592123456789";
    String expB2 = "sys.mem,dc=DEN,host=web01 avail=24,slab=36.24 1625668592123456789";
    boolean matched = ilp.contains(expA1);
    if (!matched) {
      assertTrue(ilp.contains(expA2));
    }
    matched = ilp.contains(expB1);
    if (!matched) {
      assertTrue(ilp.contains(expB2));
    }

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    written = converter.serialize(data, stream);
    buffer = stream.toByteArray();
    assertEquals(written, buffer.length);
    ilp = new String(buffer, 0, written);
    matched = ilp.contains(expA1);
    if (!matched) {
      assertTrue(ilp.contains(expA2));
    }
    matched = ilp.contains(expB1);
    if (!matched) {
      assertTrue(ilp.contains(expB2));
    }

    // NOTE Since it's a UT we _can_ modify the timestamp.
    ZonedNanoTimeStamp ts = (ZonedNanoTimeStamp) data.timestamp();
    ts.update(1625668592L, 0);
    buffer = new byte[converter.serializationSize(data)];
    written = converter.serialize(data, buffer, 0);
    ilp = new String(buffer, 0, written);

    expA1 = "sys.cpu,host=web01,dc=DEN user=42,sys=2.12,idle=28 1625668592000000000";
    expA2 = "sys.cpu,dc=DEN,host=web01 user=42,sys=2.12,idle=28 1625668592000000000";
    expB1 = "sys.mem,host=web01,dc=DEN avail=24,slab=36.24 1625668592000000000";
    expB2 = "sys.mem,dc=DEN,host=web01 avail=24,slab=36.24 1625668592000000000";

    matched = ilp.contains(expA1);
    if (!matched) {
      assertTrue(ilp.contains(expA2));
    }
    matched = ilp.contains(expB1);
    if (!matched) {
      assertTrue(ilp.contains(expB2));
    }

    stream = new ByteArrayOutputStream();
    written = converter.serialize(data, stream);
    buffer = stream.toByteArray();
    assertEquals(written, buffer.length);
    ilp = new String(buffer, 0, written);
    matched = ilp.contains(expA1);
    if (!matched) {
      assertTrue(ilp.contains(expA2));
    }
    matched = ilp.contains(expB1);
    if (!matched) {
      assertTrue(ilp.contains(expB2));
    }
  }

  @Test
  public void serializeLowLevel() throws Exception {
    List<TimeSeriesDatum> list = Lists.newArrayList();
    list.add(TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.cpu.user")
                    .addTags("host", "web01")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123456789L, Const.UTC),
                    42)));
    list.add(TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.mem.avail")
                    .addTags("host", "web01")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123456789L, Const.UTC),
                    24)));
    list.add(TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.cpu.sys")
                    .addTags("host", "web01")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123456789L, Const.UTC),
                    2.12)));
    list.add(TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.cpu.idle")
                    .addTags("host", "web01")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123456789L, Const.UTC),
                    28)));
    list.add(TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.mem.slab")
                    .addTags("host", "web01")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123456789L, Const.UTC),
                    36.24)));
    MockLowLevelMetricData mock = new MockLowLevelMetricData();
    mock.add(list);
    InfluxLineProtocolConverter converter = new InfluxLineProtocolConverter();
    byte[] buffer = new byte[converter.serializationSize(mock)];
    int written = converter.serialize(mock, buffer, 0);
    String ilp = new String(buffer, 0, written);

    String expA1 = "sys.cpu,host=web01,dc=DEN user=42,sys=2.12,idle=28 1625668592123456789";
    String expA2 = "sys.cpu,dc=DEN,host=web01 user=42,sys=2.12,idle=28 1625668592123456789";
    String expB1 = "sys.mem,host=web01,dc=DEN avail=24,slab=36.24 1625668592123456789";
    String expB2 = "sys.mem,dc=DEN,host=web01 avail=24,slab=36.24 1625668592123456789";
    boolean matched = ilp.contains(expA1);
    if (!matched) {
      assertTrue(ilp.contains(expA2));
    }
    matched = ilp.contains(expB1);
    if (!matched) {
      assertTrue(ilp.contains(expB2));
    }

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    written = converter.serialize(mock, stream);
    buffer = stream.toByteArray();
    assertEquals(written, buffer.length);
    ilp = new String(buffer, 0, written);
    matched = ilp.contains(expA1);
    if (!matched) {
      assertTrue(ilp.contains(expA2));
    }
    matched = ilp.contains(expB1);
    if (!matched) {
      assertTrue(ilp.contains(expB2));
    }

    // diff tags
    list.set(2, TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.cpu.sys")
                    .addTags("host", "db04")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123456789L, Const.UTC),
                    2.12)));
    mock = new MockLowLevelMetricData();
    mock.add(list);
    buffer = new byte[converter.serializationSize(mock)];
    written = converter.serialize(mock, buffer, 0);
    ilp = new String(buffer, 0, written);
    expA1 = "sys.cpu,host=web01,dc=DEN user=42,idle=28 1625668592123456789";
    expA2 = "sys.cpu,dc=DEN,host=web01 user=42,idle=28 1625668592123456789";
    String expC1 = "sys.cpu,host=db04,dc=DEN sys=2.12 1625668592123456789";
    String expC2 = "sys.cpu,dc=DEN,host=db04 sys=2.12 1625668592123456789";
    matched = ilp.contains(expA1);
    if (!matched) {
      assertTrue(ilp.contains(expA2));
    }
    matched = ilp.contains(expB1);
    if (!matched) {
      assertTrue(ilp.contains(expB2));
    }
    matched = ilp.contains(expC1);
    if (!matched) {
      assertTrue(ilp.contains(expC2));
    }

    // diff timestamp
    list.set(2, TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.cpu.sys")
                    .addTags("host", "web01")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123456789L, Const.UTC),
                    2.12)));
    list.add(TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.cpu.sys")
                    .addTags("host", "web01")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668652L, 0L, Const.UTC),
                    99.9)));
    mock = new MockLowLevelMetricData();
    mock.add(list);
    buffer = new byte[converter.serializationSize(mock)];
    written = converter.serialize(mock, buffer, 0);
    ilp = new String(buffer, 0, written);
    expA1 = "sys.cpu,host=web01,dc=DEN user=42,sys=2.12,idle=28 1625668592123456789";
    expA2 = "sys.cpu,dc=DEN,host=web01 user=42,sys=2.12,idle=28 1625668592123456789";
    expC1 = "sys.cpu,host=web01,dc=DEN sys=99.9 1625668652000000000";
    expC2 = "sys.cpu,dc=DEN,host=web01 sys=99.9 1625668652000000000";
    matched = ilp.contains(expA1);
    if (!matched) {
      assertTrue(ilp.contains(expA2));
    }
    matched = ilp.contains(expB1);
    if (!matched) {
      assertTrue(ilp.contains(expB2));
    }
    matched = ilp.contains(expC1);
    if (!matched) {
      assertTrue(ilp.contains(expC2));
    }

    // diff timestamp AND tags
    list.set(2, TimeSeriesDatum.wrap(
            BaseTimeSeriesDatumStringId.newBuilder()
                    .setMetric("sys.cpu.sys")
                    .addTags("host", "web04")
                    .addTags("dc", "DEN")
                    .build(),
            new MutableNumericValue(
                    new ZonedNanoTimeStamp(1625668592L, 123456789L, Const.UTC),
                    2.12)));
    mock = new MockLowLevelMetricData();
    mock.add(list);
    buffer = new byte[converter.serializationSize(mock)];
    written = converter.serialize(mock, buffer, 0);
    ilp = new String(buffer, 0, written);
    expA1 = "sys.cpu,host=web01,dc=DEN user=42,idle=28 1625668592123456789";
    expA2 = "sys.cpu,dc=DEN,host=web01 user=42,idle=28 1625668592123456789";
    expC1 = "sys.cpu,host=web01,dc=DEN sys=99.9 1625668652000000000";
    expC2 = "sys.cpu,dc=DEN,host=web01 sys=99.9 1625668652000000000";
    String expD1 = "sys.cpu,host=web04,dc=DEN sys=2.12 1625668592123456789";
    String expD2 = "sys.cpu,dc=DEN,host=web04 sys=2.12 1625668592123456789";
    matched = ilp.contains(expA1);
    if (!matched) {
      assertTrue(ilp.contains(expA2));
    }
    matched = ilp.contains(expB1);
    if (!matched) {
      assertTrue(ilp.contains(expB2));
    }
    matched = ilp.contains(expC1);
    if (!matched) {
      assertTrue(ilp.contains(expC2));
    }
    matched = ilp.contains(expD1);
    if (!matched) {
      assertTrue(ilp.contains(expD2));
    }
  }
}
