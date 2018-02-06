// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;

import org.junit.Test;

import net.opentsdb.data.TimeStamp.RelationalOperator;

public class TestZonedNanoTimeStamp {
  final static ZoneId UTC = ZoneId.of("UTC");
  // 30 minute offset
  final static ZoneId AF = ZoneId.of("Asia/Kabul");
  // 45 minute offset w DST
  final static ZoneId NZ = ZoneId.of("Pacific/Chatham");
  // 12h offset w/o DST
  final static ZoneId TV = ZoneId.of("Pacific/Funafuti");
  // 12h offset w DST
  final static ZoneId FJ = ZoneId.of("Pacific/Fiji");
  // Fri, 15 May 2015 14:21:13.432 UTC
  final static long NON_DST_TS = 1431699673432L;
  // Tue, 15 Dec 2015 04:02:25.123 UTC
  final static long DST_TS = 1450152145123L;
  
  @Test
  public void ctors() throws Exception {
    ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(0), UTC);
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(zdt);
    assertEquals(0, znt.epoch());
    assertEquals(0, znt.msEpoch());
    assertEquals(0, znt.nanos());
    assertEquals(UTC, znt.timezone());
    assertEquals(ChronoUnit.NANOS, znt.units());
    
    zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(0), AF);
    znt = new ZonedNanoTimeStamp(zdt);
    assertEquals(0, znt.epoch());
    assertEquals(0, znt.msEpoch());
    assertEquals(0, znt.nanos());
    assertEquals(AF, znt.timezone());
    
    zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(0), NZ);
    znt = new ZonedNanoTimeStamp(zdt);
    assertEquals(0, znt.epoch());
    assertEquals(0, znt.msEpoch());
    assertEquals(0, znt.nanos());
    assertEquals(NZ, znt.timezone());
    
    zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(NON_DST_TS), NZ);
    znt = new ZonedNanoTimeStamp(zdt);
    assertEquals(1431699673, znt.epoch());
    assertEquals(1431699673432L, znt.msEpoch());
    assertEquals(432000000, znt.nanos());
    assertEquals(NZ, znt.timezone());
    
    zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(1450152145, 123456789), NZ);
    znt = new ZonedNanoTimeStamp(zdt);
    assertEquals(1450152145, znt.epoch());
    assertEquals(1450152145123L, znt.msEpoch());
    assertEquals(123456789, znt.nanos());
    assertEquals(NZ, znt.timezone());
    
    zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(-1450152145, 123456789), NZ);
    znt = new ZonedNanoTimeStamp(zdt);
    assertEquals(-1450152145, znt.epoch());
    assertEquals(-1450152144877L, znt.msEpoch()); // <-- NOTE!
    assertEquals(123456789, znt.nanos());
    assertEquals(NZ, znt.timezone());
    
    try {
      new ZonedNanoTimeStamp(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    znt = new ZonedNanoTimeStamp(0, UTC);
    assertEquals(0, znt.epoch());
    assertEquals(0, znt.msEpoch());
    assertEquals(0, znt.nanos());
    assertEquals(UTC, znt.timezone());
    
    znt = new ZonedNanoTimeStamp(1450152145000L, UTC);
    assertEquals(1450152145, znt.epoch());
    assertEquals(1450152145000L, znt.msEpoch());
    assertEquals(0, znt.nanos());
    assertEquals(UTC, znt.timezone());
    
    znt = new ZonedNanoTimeStamp(1450152145123L, UTC);
    assertEquals(1450152145, znt.epoch());
    assertEquals(1450152145123L, znt.msEpoch());
    assertEquals(123000000, znt.nanos());
    assertEquals(UTC, znt.timezone());
    
    znt = new ZonedNanoTimeStamp(-1450152145123L, NZ);
    assertEquals(-1450152146, znt.epoch());
    assertEquals(-1450152145123L, znt.msEpoch()); // <-- NOTE!
    assertEquals(877000000, znt.nanos()); // <-- NOTE!
    assertEquals(NZ, znt.timezone());
    
    try {
      new ZonedNanoTimeStamp(0, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    znt = new ZonedNanoTimeStamp(0, 0, UTC);
    assertEquals(0, znt.epoch());
    assertEquals(0, znt.msEpoch());
    assertEquals(0, znt.nanos());
    assertEquals(UTC, znt.timezone());
    
    znt = new ZonedNanoTimeStamp(1450152145, 0, UTC);
    assertEquals(1450152145, znt.epoch());
    assertEquals(1450152145000L, znt.msEpoch());
    assertEquals(0, znt.nanos());
    assertEquals(UTC, znt.timezone());
    
    znt = new ZonedNanoTimeStamp(1450152145, 123456789, UTC);
    assertEquals(1450152145, znt.epoch());
    assertEquals(1450152145123L, znt.msEpoch());
    assertEquals(123456789, znt.nanos());
    assertEquals(UTC, znt.timezone());
    
    znt = new ZonedNanoTimeStamp(-1450152145, 123456789, NZ);
    assertEquals(-1450152145, znt.epoch());
    assertEquals(-1450152144877L, znt.msEpoch()); // <-- NOTE!
    assertEquals(123456789, znt.nanos());
    assertEquals(NZ, znt.timezone());
    
    try {
      new ZonedNanoTimeStamp(0, 0, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void update() throws Exception {
    ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(0), AF);
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(zdt);
    assertEquals(0, znt.epoch());
    assertEquals(0, znt.msEpoch());
    assertEquals(0, znt.nanos());
    assertEquals(AF, znt.timezone());
    
    znt.updateEpoch(1431699673);
    assertEquals(1431699673, znt.epoch());
    assertEquals(1431699673000L, znt.msEpoch());
    assertEquals(0, znt.nanos());
    assertEquals(AF, znt.timezone());
    
    znt.updateMsEpoch(NON_DST_TS);
    assertEquals(1431699673, znt.epoch());
    assertEquals(NON_DST_TS, znt.msEpoch());
    assertEquals(432000000, znt.nanos());
    assertEquals(AF, znt.timezone());
    
    znt.update(1483272000, 456);
    assertEquals(1483272000, znt.epoch());
    assertEquals(1483272000000L, znt.msEpoch());
    assertEquals(456, znt.nanos());
    assertEquals(AF, znt.timezone());
    
    zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(DST_TS), NZ);
    ZonedNanoTimeStamp other = new ZonedNanoTimeStamp(zdt);
    znt.update(other);
    assertEquals(1450152145, znt.epoch());
    assertEquals(DST_TS, znt.msEpoch());
    assertEquals(123000000, znt.nanos());
    assertEquals(NZ, znt.timezone());
    
    try {
      znt.update(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getCopy() throws Exception {
    ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(0), AF);
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(zdt);
    assertEquals(0, znt.epoch());
    assertEquals(0, znt.msEpoch());
    assertEquals(0, znt.nanos());
    assertEquals(AF, znt.timezone());
    
    TimeStamp clone = znt.getCopy();
    assertTrue(clone != znt);
    assertEquals(0, clone.epoch());
    assertEquals(0, clone.msEpoch());
    assertEquals(0, clone.nanos());
    assertEquals(AF, clone.timezone());
    
    znt.updateMsEpoch(DST_TS);
    assertEquals(1450152145, znt.epoch());
    assertEquals(DST_TS, znt.msEpoch());
    assertEquals(123000000, znt.nanos());
    assertEquals(AF, znt.timezone());
    assertEquals(0, clone.epoch());
    assertEquals(0, clone.msEpoch());
    assertEquals(0, clone.nanos());
    assertEquals(AF, clone.timezone());
    
    clone.updateMsEpoch(NON_DST_TS);
    assertEquals(1431699673, clone.epoch());
    assertEquals(NON_DST_TS, clone.msEpoch());
    assertEquals(432000000, clone.nanos());
    assertEquals(AF, clone.timezone());
    assertEquals(1450152145, znt.epoch());
    assertEquals(DST_TS, znt.msEpoch());
    assertEquals(123000000, znt.nanos());
    assertEquals(AF, znt.timezone());
  }
  
  @Test
  public void compare() throws Exception {
    ZonedNanoTimeStamp ts1 = new ZonedNanoTimeStamp(1483272000, 500, AF);
    ZonedNanoTimeStamp ts2 = new ZonedNanoTimeStamp(1483272000, 501, AF);
    
    assertTrue(ts1.compare(RelationalOperator.LT, ts2));
    assertTrue(ts1.compare(RelationalOperator.LTE, ts2));
    assertFalse(ts1.compare(RelationalOperator.GT, ts2));
    assertFalse(ts1.compare(RelationalOperator.GTE, ts2));
    assertFalse(ts1.compare(RelationalOperator.EQ, ts2));
    assertTrue(ts1.compare(RelationalOperator.NE, ts2));
    
    assertFalse(ts2.compare(RelationalOperator.LT, ts1));
    assertFalse(ts2.compare(RelationalOperator.LTE, ts1));
    assertTrue(ts2.compare(RelationalOperator.GT, ts1));
    assertTrue(ts2.compare(RelationalOperator.GTE, ts1));
    assertFalse(ts2.compare(RelationalOperator.EQ, ts1));
    assertTrue(ts2.compare(RelationalOperator.NE, ts1));
    
    // compare the seconds now
    ts1 = new ZonedNanoTimeStamp(1483272000, 500, AF);
    ts2 = new ZonedNanoTimeStamp(1483272001, 500, AF);
    
    assertTrue(ts1.compare(RelationalOperator.LT, ts2));
    assertTrue(ts1.compare(RelationalOperator.LTE, ts2));
    assertFalse(ts1.compare(RelationalOperator.GT, ts2));
    assertFalse(ts1.compare(RelationalOperator.GTE, ts2));
    assertFalse(ts1.compare(RelationalOperator.EQ, ts2));
    assertTrue(ts1.compare(RelationalOperator.NE, ts2));
    
    assertFalse(ts2.compare(RelationalOperator.LT, ts1));
    assertFalse(ts2.compare(RelationalOperator.LTE, ts1));
    assertTrue(ts2.compare(RelationalOperator.GT, ts1));
    assertTrue(ts2.compare(RelationalOperator.GTE, ts1));
    assertFalse(ts2.compare(RelationalOperator.EQ, ts1));
    assertTrue(ts2.compare(RelationalOperator.NE, ts1));
    
    ts1 = new ZonedNanoTimeStamp(1483272000, 100500, AF);
    ts2 = new ZonedNanoTimeStamp(1483272001, 500, AF);
    
    assertTrue(ts1.compare(RelationalOperator.LT, ts2));
    assertTrue(ts1.compare(RelationalOperator.LTE, ts2));
    assertFalse(ts1.compare(RelationalOperator.GT, ts2));
    assertFalse(ts1.compare(RelationalOperator.GTE, ts2));
    assertFalse(ts1.compare(RelationalOperator.EQ, ts2));
    assertTrue(ts1.compare(RelationalOperator.NE, ts2));
    
    assertFalse(ts2.compare(RelationalOperator.LT, ts1));
    assertFalse(ts2.compare(RelationalOperator.LTE, ts1));
    assertTrue(ts2.compare(RelationalOperator.GT, ts1));
    assertTrue(ts2.compare(RelationalOperator.GTE, ts1));
    assertFalse(ts2.compare(RelationalOperator.EQ, ts1));
    assertTrue(ts2.compare(RelationalOperator.NE, ts1));
    
    ts1 = new ZonedNanoTimeStamp(1483272000, 500, AF);
    ts2 = new ZonedNanoTimeStamp(1483272000, 500, AF);
    
    assertFalse(ts1.compare(RelationalOperator.LT, ts2));
    assertTrue(ts1.compare(RelationalOperator.LTE, ts2));
    assertFalse(ts1.compare(RelationalOperator.GT, ts2));
    assertTrue(ts1.compare(RelationalOperator.GTE, ts2));
    assertTrue(ts1.compare(RelationalOperator.EQ, ts2));
    assertFalse(ts1.compare(RelationalOperator.NE, ts2));
    
    try {
      ts1.compare(null, ts2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ts1.compare(RelationalOperator.LT, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void setMax() throws Exception {
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(1483272000, 500, AF);
    znt.setMax();
    assertEquals(2147483647, znt.epoch());
    assertEquals(2147483647999L, znt.msEpoch());
    assertEquals(999999999, znt.nanos());
    assertEquals(AF, znt.timezone());
  }
  
  @Test
  public void add() throws Exception {
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(1483272000, 500, AF);
    assertEquals(1483272000, znt.epoch());
    assertEquals(1483272000000L, znt.msEpoch());
    assertEquals(500, znt.nanos());
    assertEquals(AF, znt.timezone());
    
    znt.add(Duration.of(2, ChronoUnit.HOURS));
    assertEquals(1483279200, znt.epoch());
    assertEquals(1483279200000L, znt.msEpoch());
    assertEquals(500, znt.nanos());
    assertEquals(AF, znt.timezone());
    
    try {
      znt.add(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void snapToPreviousIntervalNanos() throws Exception {
    // nanos don't care about time zones unless we fall up to resolutions greater
    // than minutes.
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123456236, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(5, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123456235, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(10, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123456230, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(20, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123456220, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(200, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123456200, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(250, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123456000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(500, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123456000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(750, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123456000, znt.nanos());
    
    // funky intervals based on micro
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(289, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123456042, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(3, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123456234, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(999, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123455544, znt.nanos());
    
    // over the interval
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(1000, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123456000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(5000, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123455000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(1234, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123455346, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(1000000, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123000000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(1000000000, ChronoUnit.NANOS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // just a bizarre case
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(6843581857L, ChronoUnit.NANOS);
    assertEquals(1450152140, znt.epoch());
    assertEquals(530745571, znt.nanos());
    
    // test some affected by timezones
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(3600 * 1000000000L, ChronoUnit.NANOS);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, AF);
    znt.snapToPreviousInterval(3600 * 1000000000L, ChronoUnit.NANOS);
    assertEquals(1450150200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, NZ);
    znt.snapToPreviousInterval(3600 * 1000000000L, ChronoUnit.NANOS);
    assertEquals(1450149300, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // another bizarre one.
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, NZ);
    znt.snapToPreviousInterval(3600 * 1843581857L, ChronoUnit.NANOS);
    assertEquals(1450147832, znt.epoch());
    assertEquals(52166800, znt.nanos());
  }
  
  @Test
  public void snapToPreviousIntervalMicros() throws Exception {
    // micros don't care about time zones unless we fall up to resolutions greater
    // than minutes.
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.MICROS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123456000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(5, ChronoUnit.MICROS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123455000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(10, ChronoUnit.MICROS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123450000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(20, ChronoUnit.MICROS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123440000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(250, ChronoUnit.MICROS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123250000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(500, ChronoUnit.MICROS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123000000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(750, ChronoUnit.MICROS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123000000, znt.nanos());
    
    // funky intervals based on milli
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(289, ChronoUnit.MICROS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123403000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(3, ChronoUnit.MICROS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123456000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(999, ChronoUnit.MICROS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(122877000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(1000, ChronoUnit.MICROS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123000000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(5000, ChronoUnit.MICROS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(120000000, znt.nanos());
    
    // why? I don't know.
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(1234, ChronoUnit.MICROS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123400000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(1000000, ChronoUnit.MICROS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(60000000, ChronoUnit.MICROS);
    assertEquals(1450152120, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // just a bizarre case
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(6843581857L, ChronoUnit.MICROS);
    assertEquals(1450151287, znt.epoch());
    assertEquals(163714000, znt.nanos());
    
    // test some affected by timezones
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(3600 * 1000000L, ChronoUnit.MICROS);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, AF);
    znt.snapToPreviousInterval(3600 * 1000000L, ChronoUnit.MICROS);
    assertEquals(1450150200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, NZ);
    znt.snapToPreviousInterval(3600 * 1000000L, ChronoUnit.MICROS);
    assertEquals(1450149300, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, NZ);
    znt.snapToPreviousInterval(3600 * 1843581L, ChronoUnit.MICROS);
    assertEquals(1450147832, znt.epoch());
    assertEquals(24400000, znt.nanos());
  }
  
  @Test
  public void snapToPreviousIntervalMillis() throws Exception {
    // millis don't care about time zones unless we fall up to resolutions greater
    // than minutes.
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.MILLIS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(123000000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(5, ChronoUnit.MILLIS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(120000000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(10, ChronoUnit.MILLIS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(120000000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 123456236, UTC);
    znt.snapToPreviousInterval(20, ChronoUnit.MILLIS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(120000000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(200, ChronoUnit.MILLIS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(400000000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(250, ChronoUnit.MILLIS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(500000000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(500, ChronoUnit.MILLIS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(500000000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(750, ChronoUnit.MILLIS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(500000000, znt.nanos());
    
    // funky intervals based on seconds
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(289, ChronoUnit.MILLIS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(432000000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(3, ChronoUnit.MILLIS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(521000000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(999, ChronoUnit.MILLIS);
    assertEquals(1450152144, znt.epoch());
    assertEquals(975000000, znt.nanos());
    
    // over the interval
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1000, ChronoUnit.MILLIS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(5000, ChronoUnit.MILLIS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1234, ChronoUnit.MILLIS);
    assertEquals((DST_TS / 1000) - 1, znt.epoch());
    assertEquals(680000000, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(60000, ChronoUnit.MILLIS);
    assertEquals((DST_TS / 1000) - 25, znt.epoch());
    
    // bizarre case
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(6843581857L, ChronoUnit.MILLIS);
    assertEquals(1447444727, znt.epoch());
    assertEquals(428000000, znt.nanos());
    
    // test some affected by timezones
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(3600 * 1000L, ChronoUnit.MILLIS);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, AF);
    znt.snapToPreviousInterval(3600 * 1000L, ChronoUnit.MILLIS);
    assertEquals(1450150200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(3600 * 1000L, ChronoUnit.MILLIS);
    assertEquals(1450149300, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // another bizarre one.
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(3600 * 184L, ChronoUnit.MILLIS);
    assertEquals(1450151690, znt.epoch());
    assertEquals(400000000, znt.nanos());
  }
  
  @Test
  public void snapToPreviousIntervalSeconds() throws Exception {
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.SECONDS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(3, ChronoUnit.SECONDS);
    assertEquals((DST_TS / 1000) - 1, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(5, ChronoUnit.SECONDS);
    assertEquals(DST_TS / 1000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(10, ChronoUnit.SECONDS);
    assertEquals((DST_TS / 1000) - 5, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(15, ChronoUnit.SECONDS);
    assertEquals((DST_TS / 1000) - 10, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(30, ChronoUnit.SECONDS);
    assertEquals(1450152120, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // funky intervals based on minutes
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(45, ChronoUnit.SECONDS);
    assertEquals(1450152135, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(7, ChronoUnit.SECONDS);
    assertEquals(1450152140, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(25, ChronoUnit.SECONDS);
    assertEquals(1450152125, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(59, ChronoUnit.SECONDS);
    assertEquals(1450152118, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // over the interval
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(60, ChronoUnit.SECONDS);
    assertEquals(1450152120, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(61, ChronoUnit.SECONDS);
    assertEquals(1450152122, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // just a bizarre case
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1234, ChronoUnit.SECONDS);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(120, ChronoUnit.SECONDS);
    assertEquals(1450152120, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(3600, ChronoUnit.SECONDS);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(86400, ChronoUnit.SECONDS);
    assertEquals(1450137600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // test some affected by timezones
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, AF);
    znt.snapToPreviousInterval(3600, ChronoUnit.SECONDS);
    assertEquals(1450150200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(3600, ChronoUnit.SECONDS);
    assertEquals(1450149300, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // another bizarre one.
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(3600 * 1843L, ChronoUnit.SECONDS);
    assertEquals(1446560100, znt.epoch());
    assertEquals(0, znt.nanos());
  }
  
  @Test
  public void snapToPreviousIntervalMinutes() throws Exception {
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.MINUTES);
    assertEquals(1450152120, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(2, ChronoUnit.MINUTES);
    assertEquals(1450152120, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(3, ChronoUnit.MINUTES);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(5, ChronoUnit.MINUTES);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(10, ChronoUnit.MINUTES);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(15, ChronoUnit.MINUTES);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(30, ChronoUnit.MINUTES);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // funky intervals based on hours
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(45, ChronoUnit.MINUTES);
    assertEquals(1450151100, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(7, ChronoUnit.MINUTES);
    assertEquals(1450151880, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(25, ChronoUnit.MINUTES);
    assertEquals(1450151100, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(59, ChronoUnit.MINUTES);
    assertEquals(1450151760, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // over the interval
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(60, ChronoUnit.MINUTES);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(61, ChronoUnit.MINUTES);
    assertEquals(1450148580, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // Just a bizarre case
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1234, ChronoUnit.MINUTES);
    assertEquals(1450112640, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(120, ChronoUnit.MINUTES);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1440, ChronoUnit.MINUTES);
    assertEquals(1450137600, znt.epoch());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(2880, ChronoUnit.MINUTES);
    assertEquals(1450137600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // test some affected by timezones
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, AF);
    znt.snapToPreviousInterval(60, ChronoUnit.MINUTES);
    assertEquals(1450150200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(60, ChronoUnit.MINUTES);
    assertEquals(1450149300, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // another bizarre one.
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(60 * 1843L, ChronoUnit.MINUTES);
    assertEquals(1446560100, znt.epoch());
    assertEquals(0, znt.nanos());
  }
  
  @Test
  public void snapToPreviousIntervalHours() throws Exception {
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.HOURS);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(2, ChronoUnit.HOURS);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(3, ChronoUnit.HOURS);
    assertEquals(1450148400, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(4, ChronoUnit.HOURS);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(6, ChronoUnit.HOURS);
    assertEquals(1450137600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(12, ChronoUnit.HOURS);
    assertEquals(1450137600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // funky intervals based on days
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(18, ChronoUnit.HOURS);
    assertEquals(1450094400, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(5, ChronoUnit.HOURS);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(23, ChronoUnit.HOURS);
    assertEquals(1450087200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // over the interval
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(24, ChronoUnit.HOURS);
    assertEquals(1450137600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // based on the start of the year
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(48, ChronoUnit.HOURS);
    assertEquals(1450137600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // just a bizarre case
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1234, ChronoUnit.HOURS);
    assertEquals(1446724800, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // weekly, so we pass into that case.
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(168, ChronoUnit.HOURS);
    assertEquals(1449964800, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // weekly, so we pass into that case.
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(336, ChronoUnit.HOURS);
    assertEquals(1449360000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // time zones
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, AF);
    znt.snapToPreviousInterval(1, ChronoUnit.HOURS);
    assertEquals(1450150200, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, AF);
    znt.snapToPreviousInterval(1, ChronoUnit.HOURS);
    assertEquals(1431696600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(1, ChronoUnit.HOURS);
    assertEquals(1450149300, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(1, ChronoUnit.HOURS);
    assertEquals(1431699300, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, TV);
    znt.snapToPreviousInterval(1, ChronoUnit.HOURS);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, TV);
    znt.snapToPreviousInterval(1, ChronoUnit.HOURS);
    assertEquals(1431698400, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, FJ);
    znt.snapToPreviousInterval(1, ChronoUnit.HOURS);
    assertEquals(1450152000, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, FJ);
    znt.snapToPreviousInterval(1, ChronoUnit.HOURS);
    assertEquals(1431698400, znt.epoch());
    assertEquals(0, znt.nanos());
  }
  
  @Test
  public void snapToPreviousIntervalDays() throws Exception {
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.DAYS);
    assertEquals(1450137600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // goes to the top of the year.
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(2, ChronoUnit.DAYS);
    assertEquals(1450137600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // same here.
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(5, ChronoUnit.DAYS);
    assertEquals(1449878400, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(7, ChronoUnit.DAYS);
    assertEquals(1449964800, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(30, ChronoUnit.DAYS);
    assertEquals(1448582400, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(29, ChronoUnit.DAYS);
    assertEquals(1450137600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // over the interval
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(365, ChronoUnit.DAYS);
    assertEquals(1420070400, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(730, ChronoUnit.DAYS);
    assertEquals(1420070400, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // timezones
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, AF);
    znt.snapToPreviousInterval(1, ChronoUnit.DAYS);
    assertEquals(1450121400, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, AF);
    znt.snapToPreviousInterval(1, ChronoUnit.DAYS);
    assertEquals(1431631800, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(1, ChronoUnit.DAYS);
    assertEquals(1450088100, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(1, ChronoUnit.DAYS);
    assertEquals(1431688500, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, TV);
    znt.snapToPreviousInterval(1, ChronoUnit.DAYS);
    assertEquals(1450094400, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, TV);
    znt.snapToPreviousInterval(1, ChronoUnit.DAYS);
    assertEquals(1431691200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, FJ);
    znt.snapToPreviousInterval(1, ChronoUnit.DAYS);
    assertEquals(1450090800, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, FJ);
    znt.snapToPreviousInterval(1, ChronoUnit.DAYS);
    assertEquals(1431691200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // leap year
    znt = new ZonedNanoTimeStamp(1330516800, 0, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.MONTHS);
    assertEquals(1328054400, znt.epoch());
    assertEquals(0, znt.nanos());
  }
  
  @Test
  public void snapToPreviousIntervalWeeks() throws Exception {
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS);
    assertEquals(1449964800, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(2, ChronoUnit.WEEKS);
    assertEquals(1449360000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(4, ChronoUnit.WEEKS);
    assertEquals(1449360000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(7, ChronoUnit.WEEKS);
    assertEquals(1449964800, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // bizarro and over
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(53, ChronoUnit.WEEKS);
    assertEquals(1442707200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // timezones
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, AF);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS);
    assertEquals(1449948600, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, AF);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS);
    assertEquals(1431199800, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS);
    assertEquals(1449915300, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS);
    assertEquals(1431170100, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, TV);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS);
    assertEquals(1449921600, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, TV);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS);
    assertEquals(1431172800, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, FJ);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS);
    assertEquals(1449918000, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, FJ);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS);
    assertEquals(1431172800, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // diff day of weeks
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS, DayOfWeek.MONDAY);
    assertEquals(1450051200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS, DayOfWeek.TUESDAY);
    assertEquals(1450137600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS, DayOfWeek.WEDNESDAY);
    assertEquals(1449619200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS, DayOfWeek.THURSDAY);
    assertEquals(1449705600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS, DayOfWeek.FRIDAY);
    assertEquals(1449792000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.WEEKS, DayOfWeek.SATURDAY);
    assertEquals(1449878400, znt.epoch());
    assertEquals(0, znt.nanos());
  }
  
  @Test
  public void snapToPreviousIntervalMonths() throws Exception {
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.MONTHS);
    assertEquals(1448928000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(2, ChronoUnit.MONTHS);
    assertEquals(1446336000, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(3, ChronoUnit.MONTHS);
    assertEquals(1443657600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(6, ChronoUnit.MONTHS);
    assertEquals(1435708800, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // odd one
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(8, ChronoUnit.MONTHS);
    assertEquals(1430438400, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(12, ChronoUnit.MONTHS);
    assertEquals(1420070400, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // bizarro
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(57, ChronoUnit.MONTHS);
    assertEquals(1349049600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // timezones
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, AF);
    znt.snapToPreviousInterval(1, ChronoUnit.MONTHS);
    assertEquals(1448911800, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, AF);
    znt.snapToPreviousInterval(1, ChronoUnit.MONTHS);
    assertEquals(1430422200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(1, ChronoUnit.MONTHS);
    assertEquals(1448878500, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(1, ChronoUnit.MONTHS);
    assertEquals(1430392500, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, TV);
    znt.snapToPreviousInterval(1, ChronoUnit.MONTHS);
    assertEquals(1448884800, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, TV);
    znt.snapToPreviousInterval(1, ChronoUnit.MONTHS);
    assertEquals(1430395200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, FJ);
    znt.snapToPreviousInterval(1, ChronoUnit.MONTHS);
    assertEquals(1448881200, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, FJ);
    znt.snapToPreviousInterval(1, ChronoUnit.MONTHS);
    assertEquals(1430395200, znt.epoch());
    assertEquals(0, znt.nanos());
  }
  
  @Test
  public void snapToPreviousIntervalYears() throws Exception {
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(1, ChronoUnit.YEARS);
    assertEquals(1420070400, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    znt.snapToPreviousInterval(2, ChronoUnit.YEARS);
    assertEquals(1388534400, znt.epoch());
    assertEquals(0, znt.nanos());
    
    // timezones
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, AF);
    znt.snapToPreviousInterval(1, ChronoUnit.YEARS);
    assertEquals(1420054200, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, AF);
    znt.snapToPreviousInterval(1, ChronoUnit.YEARS);
    assertEquals(1420054200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(1, ChronoUnit.YEARS);
    assertEquals(1420020900, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, NZ);
    znt.snapToPreviousInterval(1, ChronoUnit.YEARS);
    assertEquals(1420020900, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, TV);
    znt.snapToPreviousInterval(1, ChronoUnit.YEARS);
    assertEquals(1420027200, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, TV);
    znt.snapToPreviousInterval(1, ChronoUnit.YEARS);
    assertEquals(1420027200, znt.epoch());
    assertEquals(0, znt.nanos());
    
    znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, FJ);
    znt.snapToPreviousInterval(1, ChronoUnit.YEARS);
    assertEquals(1420023600, znt.epoch());
    assertEquals(0, znt.nanos());
    znt = new ZonedNanoTimeStamp(NON_DST_TS / 1000, 523456236, FJ);
    znt.snapToPreviousInterval(1, ChronoUnit.YEARS);
    assertEquals(1420023600, znt.epoch());
    assertEquals(0, znt.nanos());
    
    try {
      znt = new ZonedNanoTimeStamp(-8324234, 523456236, UTC);
      znt.snapToPreviousInterval(2, ChronoUnit.YEARS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void snapToPreviousIntervalErrors() throws Exception {
    ZonedNanoTimeStamp znt = new ZonedNanoTimeStamp(DST_TS / 1000, 523456236, UTC);
    try {
      znt.snapToPreviousInterval(0, ChronoUnit.WEEKS, DayOfWeek.SATURDAY);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      znt.snapToPreviousInterval(-42, ChronoUnit.WEEKS, DayOfWeek.SATURDAY);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      znt.snapToPreviousInterval(1, null, DayOfWeek.SATURDAY);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      znt.snapToPreviousInterval(1, ChronoUnit.WEEKS, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      znt.snapToPreviousInterval(1, ChronoUnit.HALF_DAYS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
