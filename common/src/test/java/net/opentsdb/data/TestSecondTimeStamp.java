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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Period;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import org.junit.Test;

import net.opentsdb.data.TimeStamp.Op;

public class TestSecondTimeStamp {
  // Fri, 15 May 2015 14:21:13 UTC
  final static long TS = 1431699673L;
 
  @Test
  public void ctors() throws Exception {
    TimeStamp ts = new SecondTimeStamp(1531847231);
    assertEquals(1531847231000L, ts.msEpoch());
    assertEquals(1531847231, ts.epoch());
    assertEquals(0, ts.nanos());
    assertEquals(ChronoUnit.SECONDS, ts.units());
    assertEquals(ZoneId.of("UTC"), ts.timezone());
    
    ts = new SecondTimeStamp(-1000);
    assertEquals(-1000000L, ts.msEpoch());
    assertEquals(-1000, ts.epoch());
    assertEquals(0, ts.nanos());
    assertEquals(ChronoUnit.SECONDS, ts.units());
    
    ts = new SecondTimeStamp(60);
    assertEquals(60000L, ts.msEpoch());
    assertEquals(60, ts.epoch());
    assertEquals(0, ts.nanos());
    assertEquals(ChronoUnit.SECONDS, ts.units());
  }
  
  @Test
  public void getCopy() throws Exception {
    TimeStamp ts = new SecondTimeStamp(1531847231);
    assertEquals(1531847231000L, ts.msEpoch());
    assertEquals(1531847231, ts.epoch());
    
    TimeStamp copy = ts.getCopy();
    assertNotSame(ts, copy);
    assertEquals(1531847231000L, ts.msEpoch());
    assertEquals(1531847231, ts.epoch());
  }
  
  @Test
  public void update() throws Exception {
    TimeStamp ts = new SecondTimeStamp(1000);
    ts.updateMsEpoch(2000000);
    assertEquals(2000000, ts.msEpoch());
    assertEquals(2000, ts.epoch());
    assertEquals(0, ts.nanos());
    
    ts.updateEpoch(3);
    assertEquals(3000, ts.msEpoch());
    assertEquals(3, ts.epoch());
    assertEquals(0, ts.nanos());
    
    ts.update(5, 500);
    assertEquals(5000, ts.msEpoch());
    assertEquals(5, ts.epoch());
    assertEquals(0, ts.nanos());
    
    ts.update(6, 600400500);
    assertEquals(6000, ts.msEpoch());
    assertEquals(6, ts.epoch());
    assertEquals(0, ts.nanos());
    
    TimeStamp copy_into = new SecondTimeStamp(1000);
    assertEquals(1000000, copy_into.msEpoch());
    assertEquals(1000, copy_into.epoch());
    assertEquals(0, copy_into.nanos());
    
    copy_into.update(ts);
    assertEquals(6000, copy_into.msEpoch());
    assertEquals(6, copy_into.epoch());
    assertEquals(0, copy_into.nanos());
  }
  
  @Test
  public void compare() throws Exception {
    final TimeStamp ts1 = new SecondTimeStamp(1000);
    TimeStamp ts2 = new SecondTimeStamp(2000);
    
    assertTrue(ts1.compare(Op.LT, ts2));
    assertTrue(ts1.compare(Op.LTE, ts2));
    assertFalse(ts1.compare(Op.GT, ts2));
    assertFalse(ts1.compare(Op.GTE, ts2));
    assertFalse(ts1.compare(Op.EQ, ts2));
    assertTrue(ts1.compare(Op.NE, ts2));
    
    ts2.updateEpoch(1000);
    assertTrue(ts1.compare(Op.EQ, ts2));
    
    // compare to something with a higher resolution
    ts2 = new ZonedNanoTimeStamp(1000, 500, ZoneId.of("UTC"));
    
    assertTrue(ts1.compare(Op.LT, ts2));
    assertTrue(ts1.compare(Op.LTE, ts2));
    assertFalse(ts1.compare(Op.GT, ts2));
    assertFalse(ts1.compare(Op.GTE, ts2));
    assertFalse(ts1.compare(Op.EQ, ts2));
    assertTrue(ts1.compare(Op.NE, ts2));
    
    assertFalse(ts2.compare(Op.LT, ts1));
    assertFalse(ts2.compare(Op.LTE, ts1));
    assertTrue(ts2.compare(Op.GT, ts1));
    assertTrue(ts2.compare(Op.GTE, ts1));
    assertFalse(ts2.compare(Op.EQ, ts1));
    assertTrue(ts2.compare(Op.NE, ts1));
    
    try {
      ts1.compare(Op.LT, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ts1.compare(null, ts2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void setMax() throws Exception {
    TimeStamp ts = new SecondTimeStamp(1000);
    assertEquals(1000000, ts.msEpoch());
    assertEquals(1000, ts.epoch());
    
    ts.setMax();
    assertEquals(-1000, ts.msEpoch());
    assertEquals(Long.MAX_VALUE, ts.epoch());
  }

  @Test
  public void add() throws Exception {
    TimeStamp ts = new SecondTimeStamp(1);
    ts.add(Period.ofYears(1));
    assertEquals(31536001000L, ts.msEpoch());
    assertEquals(31536001, ts.epoch());
    assertEquals(0, ts.nanos());
    
    ts = new SecondTimeStamp(1);
    ts.add(Period.ofMonths(1));
    assertEquals(2678401000L, ts.msEpoch());
    assertEquals(2678401, ts.epoch());
    assertEquals(0, ts.nanos());
    
    ts = new SecondTimeStamp(1);
    ts.add(Period.ofWeeks(1));
    assertEquals(604801000, ts.msEpoch());
    assertEquals(604801, ts.epoch());
    assertEquals(0, ts.nanos());
    
    ts = new SecondTimeStamp(1);
    ts.add(Duration.of(1, ChronoUnit.DAYS));
    assertEquals(86401000, ts.msEpoch());
    assertEquals(86401, ts.epoch());
    assertEquals(0, ts.nanos());
    
    ts = new SecondTimeStamp(1);
    ts.add(Duration.of(2, ChronoUnit.HOURS));
    assertEquals(7201000, ts.msEpoch());
    assertEquals(7201, ts.epoch());
    assertEquals(0, ts.nanos());
    
    ts = new SecondTimeStamp(1);
    ts.add(Duration.of(14, ChronoUnit.SECONDS));
    assertEquals(15000, ts.msEpoch());
    assertEquals(15, ts.epoch());
    assertEquals(0, ts.nanos());
    
    // no-ops
    ts = new SecondTimeStamp(1);
    ts.add(Duration.of(25, ChronoUnit.MILLIS));
    assertEquals(1000, ts.msEpoch());
    assertEquals(1, ts.epoch());
    assertEquals(0, ts.nanos());
    
    ts = new SecondTimeStamp(1);
    ts.add(Duration.of(100, ChronoUnit.MICROS));
    assertEquals(1000, ts.msEpoch());
    assertEquals(1, ts.epoch());
    assertEquals(0, ts.nanos());
    
    ts = new SecondTimeStamp(1);
    ts.add(Duration.of(100, ChronoUnit.NANOS));
    assertEquals(1000, ts.msEpoch());
    assertEquals(1, ts.epoch());
    assertEquals(0, ts.nanos());
  }

  @Test
  public void snapToPreviousIntervalNanos() throws Exception {
    SecondTimeStamp ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1, ChronoUnit.NANOS);
    assertEquals(TS, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(239, ChronoUnit.NANOS);
    assertEquals(TS, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(999, ChronoUnit.NANOS);
    assertEquals(TS, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1000000, ChronoUnit.NANOS);
    assertEquals(TS, ts.epoch());
    
    // still the same second
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1000000000, ChronoUnit.NANOS);
    assertEquals(TS, ts.epoch());
    
    // snap to hour
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(3600 * 1000000000L, ChronoUnit.NANOS);
    assertEquals(1431698400L, ts.epoch());
  }
  
  @Test
  public void snapToPreviousIntervalMicros() throws Exception {
    SecondTimeStamp ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1, ChronoUnit.MICROS);
    assertEquals(TS, ts.epoch());
   
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(500, ChronoUnit.MICROS);
    assertEquals(TS, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1000, ChronoUnit.MICROS);
    assertEquals(TS, ts.epoch());
    
    // finally making a difference
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(289, ChronoUnit.MICROS);
    assertEquals(1431699673L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(999, ChronoUnit.MICROS);
    assertEquals(1431699673L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1000000, ChronoUnit.MICROS);
    assertEquals(1431699673L, ts.epoch());
    
    // just a bizarre case
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(6843581857L, ChronoUnit.MICROS);
    assertEquals(1431695905L, ts.epoch());
    
    // snap to hour
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(3600 * 1000000L, ChronoUnit.MICROS);
    assertEquals(1431698400L, ts.epoch());
  }
  
  @Test
  public void snapToPreviousIntervalMillis() throws Exception {
    SecondTimeStamp ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1, ChronoUnit.MILLIS);
    assertEquals(TS, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(5, ChronoUnit.MILLIS);
    assertEquals(1431699673L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(10, ChronoUnit.MILLIS);
    assertEquals(1431699673L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(20, ChronoUnit.MILLIS);
    assertEquals(1431699673L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(200, ChronoUnit.MILLIS);
    assertEquals(1431699673L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(250, ChronoUnit.MILLIS);
    assertEquals(1431699673L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(500, ChronoUnit.MILLIS);
    assertEquals(1431699673L, ts.epoch());
    
    // funky intervals based on seconds
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(750, ChronoUnit.MILLIS);
    assertEquals(1431699672L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(289, ChronoUnit.MILLIS);
    assertEquals(1431699672L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(3, ChronoUnit.MILLIS);
    assertEquals(1431699672L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(999, ChronoUnit.MILLIS);
    assertEquals(1431699672L, ts.epoch());
    
    // over the interval
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1000, ChronoUnit.MILLIS);
    assertEquals(1431699673L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(5000, ChronoUnit.MILLIS);
    assertEquals(1431699670L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1234, ChronoUnit.MILLIS);
    assertEquals(1431699672L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(60000, ChronoUnit.MILLIS);
    assertEquals(1431699660L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(6843581857L, ChronoUnit.MILLIS);
    assertEquals(1426913981L, ts.epoch());
  }
  
  @Test
  public void snapToPreviousIntervalSeconds() throws Exception {
    SecondTimeStamp ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1, ChronoUnit.SECONDS);
    assertEquals(1431699673L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(3, ChronoUnit.SECONDS);
    assertEquals(1431699672L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(5, ChronoUnit.SECONDS);
    assertEquals(1431699670L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(10, ChronoUnit.SECONDS);
    assertEquals(1431699670L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(15, ChronoUnit.SECONDS);
    assertEquals(1431699660L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(30, ChronoUnit.SECONDS);
    assertEquals(1431699660L, ts.epoch());
    
    // funky intervals based on minutes
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(45, ChronoUnit.SECONDS);
    assertEquals(1431699660L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(7, ChronoUnit.SECONDS);
    assertEquals(1431699667L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(25, ChronoUnit.SECONDS);
    assertEquals(1431699650L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(59, ChronoUnit.SECONDS);
    assertEquals(1431699639L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(60, ChronoUnit.SECONDS);
    assertEquals(1431699660L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(61, ChronoUnit.SECONDS);
    assertEquals(1431699620L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1234, ChronoUnit.SECONDS);
    assertEquals(1431699634L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(120, ChronoUnit.SECONDS);
    assertEquals(1431699600L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(3600, ChronoUnit.SECONDS);
    assertEquals(1431698400L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(86400, ChronoUnit.SECONDS);
    assertEquals(1431648000L, ts.epoch());
  }
  
  @Test
  public void snapToPreviousIntervalMinutes() throws Exception {
    SecondTimeStamp ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1, ChronoUnit.MINUTES);
    assertEquals(1431699660L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(2, ChronoUnit.MINUTES);
    assertEquals(1431699600L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(3, ChronoUnit.MINUTES);
    assertEquals(1431699660L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(5, ChronoUnit.MINUTES);
    assertEquals(1431699600L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(10, ChronoUnit.MINUTES);
    assertEquals(1431699600L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(15, ChronoUnit.MINUTES);
    assertEquals(1431699300L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(30, ChronoUnit.MINUTES);
    assertEquals(1431698400L, ts.epoch());
    
    // funky intervals based on hours
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(45, ChronoUnit.MINUTES);
    assertEquals(1431699300L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(7, ChronoUnit.MINUTES);
    assertEquals(1431699660L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(25, ChronoUnit.MINUTES);
    assertEquals(1431699000L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(59, ChronoUnit.MINUTES);
    assertEquals(1431697560L, ts.epoch());
    
    // over the interval
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(60, ChronoUnit.MINUTES);
    assertEquals(1431698400L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(61, ChronoUnit.MINUTES);
    assertEquals(1431699240L, ts.epoch());
    
    // another bizarre case
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1234, ChronoUnit.MINUTES);
    assertEquals(1431697080L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(120, ChronoUnit.MINUTES);
    assertEquals(1431698400L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1440, ChronoUnit.MINUTES);
    assertEquals(1431648000L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(2880, ChronoUnit.MINUTES);
    assertEquals(1431648000L, ts.epoch());
  }
  
  @Test
  public void snapToPreviousIntervalHours() throws Exception {
    SecondTimeStamp ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1, ChronoUnit.HOURS);
    assertEquals(1431698400L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(2, ChronoUnit.HOURS);
    assertEquals(1431698400L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(3, ChronoUnit.HOURS);
    assertEquals(1431691200L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(4, ChronoUnit.HOURS);
    assertEquals(1431691200L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(6, ChronoUnit.HOURS);
    assertEquals(1431691200L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(12, ChronoUnit.HOURS);
    assertEquals(1431691200L, ts.epoch());
    
    // funky intervals based on days
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(18, ChronoUnit.HOURS);
    assertEquals(1431669600L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(5, ChronoUnit.HOURS);
    assertEquals(1431698400L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(23, ChronoUnit.HOURS);
    assertEquals(1431680400L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(24, ChronoUnit.HOURS);
    assertEquals(1431648000L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(48, ChronoUnit.HOURS);
    assertEquals(1431648000L, ts.epoch());
    
    // just a bizarre case
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(1234, ChronoUnit.HOURS);
    assertEquals(1428955200L, ts.epoch());
    
    // weekly, so we pass into that case.
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(168, ChronoUnit.HOURS);
    assertEquals(1431216000L, ts.epoch());
    
    ts = new SecondTimeStamp(TS);
    ts.snapToPreviousInterval(336, ChronoUnit.HOURS);
    assertEquals(1431216000L, ts.epoch());
  }
  
  // For other snap tos, see the ZonedNanoTimestamp class.
  
  @Test
  public void snapToPreviousIntervalErrors() throws Exception {
    SecondTimeStamp ts = new SecondTimeStamp(TS);
    try {
      ts.snapToPreviousInterval(0, ChronoUnit.WEEKS, DayOfWeek.SATURDAY);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ts.snapToPreviousInterval(-42, ChronoUnit.WEEKS, DayOfWeek.SATURDAY);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ts.snapToPreviousInterval(1, null, DayOfWeek.SATURDAY);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ts.snapToPreviousInterval(1, ChronoUnit.WEEKS, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ts.snapToPreviousInterval(1, ChronoUnit.HALF_DAYS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void equals() throws Exception {
    TimeStamp ts = new SecondTimeStamp(1);
    TimeStamp ts2 = null;
    
    assertFalse(ts.equals(ts2));
    assertFalse(ts.equals("not a ts"));
    assertTrue(ts.equals(ts));
    
    ts2 = new SecondTimeStamp(1);
    assertTrue(ts.equals(ts2));
    
    ts2.add(Duration.of(1, ChronoUnit.DAYS));
    assertFalse(ts.equals(ts2));
    
    ts2 = new ZonedNanoTimeStamp(1000L, ZoneId.of("UTC"));
    assertTrue(ts.equals(ts2));
  }
  
}
