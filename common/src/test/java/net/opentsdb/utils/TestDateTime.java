// This file is part of OpenTSDB.
// Copyright (C) 2010-2017  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Period;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
             "ch.qos.*", "org.slf4j.*",
             "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class, System.class })
public final class TestDateTime {

  //30 minute offset
  final static TimeZone AF = DateTime.timezones.get("Asia/Kabul");
  final static ZoneId AFZ = ZoneId.of("Asia/Kabul");
  // 45 minute offset w DST
  final static TimeZone NZ = DateTime.timezones.get("Pacific/Chatham");
  final static ZoneId NZZ = ZoneId.of("Pacific/Chatham");
  // 12h offset w/o DST
  final static TimeZone TV = DateTime.timezones.get("Pacific/Funafuti");
  final static ZoneId TVZ = ZoneId.of("Pacific/Funafuti");
  // 12h offset w DST
  final static TimeZone FJ = DateTime.timezones.get("Pacific/Fiji");
  final static ZoneId FJZ = ZoneId.of("Pacific/Fiji");
  
  final static ZoneId UTC = ZoneId.of("UTC");
  
  // Fri, 15 May 2015 14:21:13.432 UTC
  final static long NON_DST_TS = 1431699673432L;
  // Tue, 15 Dec 2015 04:02:25.123 UTC
  final static long DST_TS = 1450152145123L;
 
  @Before
  public void before() {
    PowerMockito.mockStatic(System.class);
    when(System.currentTimeMillis()).thenReturn(1357300800000L);
  }
  
  @Test
  public void getTimezone() {
    assertNotNull(DateTime.timezones.get("America/Los_Angeles"));
  }
  
  @Test
  public void getTimezoneNull() {
    assertNull(DateTime.timezones.get("Nothere"));
  }
  
  @Test
  public void parseDateTimeStringNow() {
    long t = DateTime.parseDateTimeString("now", null);
    assertEquals(t, 1357300800000L);
  }

  @Test
  public void parseDateTimeStringRelativeS() {
    long t = DateTime.parseDateTimeString("60s-ago", null);
    assertEquals(60000, (System.currentTimeMillis() - t));
  }
  
  @Test
  public void parseDateTimeStringRelativeM() {
    long t = DateTime.parseDateTimeString("1m-ago", null);
    assertEquals(60000, (System.currentTimeMillis() - t));
  }
  
  @Test
  public void parseDateTimeStringRelativeH() {
    long t = DateTime.parseDateTimeString("2h-ago", null);
    assertEquals(7200000L, (System.currentTimeMillis() - t));
  }
  
  @Test
  public void parseDateTimeStringRelativeD() {
    long t = DateTime.parseDateTimeString("2d-ago", null);
    long x = 2 * 3600 * 24 * 1000;
    assertEquals(x, (System.currentTimeMillis() - t));
  }
  
  @Test
  public void parseDateTimeStringRelativeD30() {
    long t = DateTime.parseDateTimeString("30d-ago", null);
    long x = 30 * 3600;
    x *= 24;
    x *= 1000;
    assertEquals(x, (System.currentTimeMillis() - t));
  }
  
  @Test
  public void parseDateTimeStringRelativeW() {
    long t = DateTime.parseDateTimeString("3w-ago", null);
    long x = 3 * 7 * 3600 * 24 * 1000;
    assertEquals(x, (System.currentTimeMillis() - t));
  }
  
  @Test
  public void parseDateTimeStringRelativeN() {
    long t = DateTime.parseDateTimeString("2n-ago", null);
    long x = 2 * 30 * 3600 * 24;
    x *= 1000;
    assertEquals(x, (System.currentTimeMillis() - t));
  }
  
  @Test
  public void parseDateTimeStringRelativeY() {
    long t = DateTime.parseDateTimeString("2y-ago", null);
    long diff = 2 * 365 * 3600 * 24;
    diff *= 1000;
    assertEquals(diff, (System.currentTimeMillis() - t));
  }
  
  @Test
  public void parseDateTimeStringUnixSeconds() {
    long t = DateTime.parseDateTimeString("1355961600", null);
    assertEquals(1355961600000L, t);
  }
  
  @Test
  public void parseDateTimeStringUnixSecondsZero() {
    long t = DateTime.parseDateTimeString("0", null);
    assertEquals(0, t);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void parseDateTimeStringUnixSecondsNegative() {
    DateTime.parseDateTimeString("-135596160", null);
  }

  /*
  1234567890.418 - match
  1234567890.1235 - no match
  1234567890.12 - matches
  1234.56789.003 - no match
  1234567890.3 - match
  */

  @Test(expected = IllegalArgumentException.class)
  public void parseDateTimeStringMultipleDots() {
    DateTime.parseDateTimeString("1234567890.2.4", null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseDateTimeStringMultipleDotsEarlyDot() {
    DateTime.parseDateTimeString("1234.56789.123", null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseDateTimeStringEarlyandExtraDots() {
    DateTime.parseDateTimeString("1234.56789.0.3", null);
  }
  
  @Test
  public void parseDateTimeStringUnixSecondsInvalidLong() {
    // this can happen if someone leaves off a zero.
    long t = DateTime.parseDateTimeString("13559616000", null);
    assertEquals(13559616000L, t);
  }
  
  @Test
  public void parseDateTimeStringUnixMS() {
    long t = DateTime.parseDateTimeString("1355961603418", null);
    assertEquals(1355961603418L, t);
  }

  @Test
  public void parseDateTimeStringShortExplicitMS() {
    long t = DateTime.parseDateTimeString("123123ms", null);
    assertEquals(123123L, t);
  }

  @Test
  public void parseDateTimeStringExplicitMS() {
    long t = DateTime.parseDateTimeString("1234567890123ms", null);
    assertEquals(1234567890123L, t);
  }
  
  @Test
  public void parseDateTimeStringUnixMSDot() {
    long t = DateTime.parseDateTimeString("1355961603.418", null);
    assertEquals(1355961603418L, t);
  }

  @Test
  public void parseDateTimeStringUnixMSDotShorter() {
    long t = DateTime.parseDateTimeString("1355961603.41", null);
    assertEquals(135596160341L, t);
  }

  @Test
  public void parseDateTimeStringUnixMSDotShortest() {
    long t = DateTime.parseDateTimeString("1355961603.4", null);
    assertEquals(13559616034L, t);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDateTimeStringUnixMSDotInvalid() {
    long t = DateTime.parseDateTimeString("135596160.418", null);
    assertEquals(1355961603418L, t);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDateTimeStringUnixMSDotInvalid2() {
    long t = DateTime.parseDateTimeString("1355961603.4180", null);
    assertEquals(1355961603418L, t);
  }
  
  @Test
  public void parseDateTimeStringDate() {
    long t = DateTime.parseDateTimeString("2012/12/20", "GMT");
    assertEquals(1355961600000L, t);
  }
  
  @Test
  public void parseDateTimeStringDateTimeShort() {
    long t = DateTime.parseDateTimeString("2012/12/20 12:42", "GMT");
    assertEquals(1356007320000L, t);
  }
  
  @Test
  public void parseDateTimeStringDateTimeDashShort() {
    long t = DateTime.parseDateTimeString("2012/12/20-12:42", "GMT");
    assertEquals(1356007320000L, t);
  }
  
  @Test
  public void parseDateTimeStringDateTime() {
    long t = DateTime.parseDateTimeString("2012/12/20 12:42:42", "GMT");
    assertEquals(1356007362000L, t);
    
    t = DateTime.parseDateTimeString("1970/01/01 00:00:00", "GMT");
    assertEquals(0L, t);
    
    t = DateTime.parseDateTimeString("1970/01/01 00:00:01", "GMT");
    assertEquals(1000L, t);
    
    t = DateTime.parseDateTimeString("1970/01/01 00:00:01", null);
    assertEquals(1000L, t);
    // TODO - more
  }
  
  @Test
  public void parseDateTimeStringDateTimeDash() {
    long t = DateTime.parseDateTimeString("2012/12/20-12:42:42", "GMT");
    assertEquals(1356007362000L, t);
    
    t = DateTime.parseDateTimeString("1970/01/01-00:00:00", "GMT");
    assertEquals(0L, t);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDateTimeStringTooBig() {
    DateTime.parseDateTimeString("1355961603587168438418", null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDateTimeStringBadFormat() {
    DateTime.parseDateTimeString("2012/12/", "GMT");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDateTimeStringBadRelative() {
    DateTime.parseDateTimeString("1s", "GMT");
  }
  
  @Test
  public void parseDateTimeStringNull() {
    long t = DateTime.parseDateTimeString(null, "GMT");
    assertEquals(-1, t);
  }
  
  @Test
  public void parseDateTimeStringEmpty() {
    long t = DateTime.parseDateTimeString("", "GMT");
    assertEquals(-1, t);
  }
  
  @Test
  public void parseDurationMS() {
    long t = DateTime.parseDuration("60ms");
    assertEquals(60, t);
  }
  
  @Test
  public void parseDurationS() {
    long t = DateTime.parseDuration("60s");
    assertEquals(60 * 1000, t);
  }
  
  @Test
  public void parseDurationCase() {
    long t = DateTime.parseDuration("60S");
    assertEquals(60 * 1000, t);
  }
  
  @Test
  public void parseDurationM() {
    long t = DateTime.parseDuration("60m");
    assertEquals(60 * 60 * 1000, t);
  }
  
  @Test
  public void parseDurationH() {
    long t = DateTime.parseDuration("24h");
    assertEquals(24 * 60 * 60 * 1000, t);
  }
  
  @Test
  public void parseDurationD() {
    long t = DateTime.parseDuration("1d");
    assertEquals(24 * 60 * 60 * 1000, t);
  }
  
  @Test
  public void parseDurationW() {
    long t = DateTime.parseDuration("1w");
    assertEquals(7 * 24 * 60 * 60 * 1000, t);
  }
  
  @Test
  public void parseDurationN() {
    long t = DateTime.parseDuration("1n");
    assertEquals(((long)30 * 24 * 60 * 60 * 1000), t);
  }
  
  @Test
  public void parseDurationY() {
    long t = DateTime.parseDuration("2y");
    assertEquals((2 * 365L * 24 * 60 * 60 * 1000), t);
  }
  
  @Test
  public void parseDurationAll() {
    assertEquals(0, DateTime.parseDuration("2all"));
    assertEquals(0, DateTime.parseDuration("0ALL"));
    try {
      DateTime.parseDuration("2al");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void parseDurationLongMS() {
    long t = DateTime.parseDuration("4294967296ms");
    assertEquals(1L << 32, t);
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseDurationTooLong() {
    DateTime.parseDuration("4611686018427387904y");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDurationNegative() {
    DateTime.parseDuration("-60s");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDurationBad() {
    DateTime.parseDuration("foo60s");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDurationInvalidSuffix() {
    DateTime.parseDuration("60p");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDurationTooBig() {
    DateTime.parseDuration("6393590450230209347573980s");
  }
  
  @Test
  public void getDurationUnits() {
    assertEquals("ns", DateTime.getDurationUnits("365ns"));
    assertEquals("mu", DateTime.getDurationUnits("4mu"));
    assertEquals("ms", DateTime.getDurationUnits("5ms"));
    assertEquals("s", DateTime.getDurationUnits("30s"));
    assertEquals("m", DateTime.getDurationUnits("60m"));
    assertEquals("h", DateTime.getDurationUnits("42h"));
    assertEquals("d", DateTime.getDurationUnits("9d"));
    assertEquals("w", DateTime.getDurationUnits("4w"));
    assertEquals("n", DateTime.getDurationUnits("12n"));
    assertEquals("y", DateTime.getDurationUnits("4y"));
    
    // zeros ok
    assertEquals("d", DateTime.getDurationUnits("0d"));
    
    // biggies
    assertEquals("s", DateTime.getDurationUnits("86400s"));
    assertEquals("s", DateTime.getDurationUnits("8641816584387483775236188168735700s"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getDurationUnitsNegative() {
    DateTime.getDurationUnits("-1d");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getDurationUnitsFloat() {
    DateTime.getDurationUnits("0.42d");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getDurationUnitsNoUnits() {
    DateTime.getDurationUnits("42");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getDurationUnitsNull() {
    DateTime.getDurationUnits(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getDurationUnitsEmpty() {
    DateTime.getDurationUnits("");
  }
  
  @Test
  public void getDurationInterval() {
    assertEquals(1, DateTime.getDurationInterval("1s"));
    assertEquals(1, DateTime.getDurationInterval("1ms"));
    assertEquals(42, DateTime.getDurationInterval("42d"));
    assertEquals(86400, DateTime.getDurationInterval("86400s"));
    assertEquals(Integer.MAX_VALUE, DateTime.getDurationInterval(
        Integer.MAX_VALUE + "s"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getDurationIntervalTooBig() {
    long value = Integer.MAX_VALUE;
    value++;
    DateTime.getDurationInterval(Long.toString(value) + "s");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getDurationIntervalNegative() {
    DateTime.getDurationInterval("-1s");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getDurationIntervalFloat() {
    DateTime.getDurationInterval("1.42s");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getDurationIntervalNoInt() {
    DateTime.getDurationInterval("s");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getDurationIntervalNull() {
    DateTime.getDurationInterval(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getDurationIntervalEmpty() {
    DateTime.getDurationInterval("");
  }
  
  @Test
  public void setTimeZone() {
    SimpleDateFormat fmt = new SimpleDateFormat("yyyy/MM/dd");
    DateTime.setTimeZone(fmt, "America/Los_Angeles");
    assertEquals("America/Los_Angeles", fmt.getTimeZone().getID());
  }
  
  @SuppressWarnings("null")
  @Test (expected = NullPointerException.class)
  public void setTimeZoneNullFmt() {
    SimpleDateFormat fmt = null;
    DateTime.setTimeZone(fmt, "America/Los_Angeles");
    assertEquals("America/Los_Angeles", fmt.getTimeZone().getID());
  }
  
  @Test
  public void setTimeZoneNullTZ() {
    SimpleDateFormat fmt = new SimpleDateFormat("yyyy/MM/dd");
    DateTime.setTimeZone(fmt, null);
    // This should return the default timezone for this box
    assertEquals(TimeZone.getDefault().getID(), fmt.getTimeZone().getID());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setTimeZoneBadTZ() {
    SimpleDateFormat fmt = new SimpleDateFormat("yyyy/MM/dd");
    DateTime.setTimeZone(fmt, "NotHere");
  }
  
  @Test
  public void isRelativeDate() {
    assertTrue(DateTime.isRelativeDate("1h-ago"));
  }
  
  @Test
  public void isRelativeDateCase() {
    assertTrue(DateTime.isRelativeDate("1H-AGO"));
  }
  
  @Test
  public void isRelativeDateNot() {
    assertFalse(DateTime.isRelativeDate("1355961600"));
  }
  
  @Test (expected = NullPointerException.class)
  public void isRelativeNull() {
    DateTime.isRelativeDate(null);
  }
  
  @Test
  public void setDefaultTimezone() {
    // because setting the default is thread local when a security manager is
    // present, we'll fail this test to warn users. We should be alright unless
    // someone tries embedding OpenTSDB in another app or app server
    assertNull(System.getSecurityManager());
    
    String current_tz = TimeZone.getDefault().getID();
    // flip between two choices so we can verify that the change holds
    String new_tz = current_tz.equals("UTC") ? 
        "America/New_York" : "UTC";
    DateTime.setDefaultTimezone(new_tz);
    assertEquals(new_tz, TimeZone.getDefault().getID());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setDefaultTimezoneNull() {
    DateTime.setDefaultTimezone(null);
  }

  @Test
  public void currentTimeMillis() {
    PowerMockito.mockStatic(System.class);
    when(System.currentTimeMillis()).thenReturn(1388534400000L);
    assertEquals(1388534400000L, DateTime.currentTimeMillis());
  }
  
  @Test
  public void nanoTime() {
    PowerMockito.mockStatic(System.class);
    when(System.nanoTime()).thenReturn(1388534400000000000L);
    assertEquals(1388534400000000000L, DateTime.nanoTime());
  }
  
  @Test
  public void msFromNano() {
    assertEquals(0, DateTime.msFromNano(0), 0.0001);
    assertEquals(1, DateTime.msFromNano(1000000), 0.0001);
    assertEquals(-1, DateTime.msFromNano(-1000000), 0.0001);
    assertEquals(1.5, DateTime.msFromNano(1500000), 0.0001);
    assertEquals(1.123, DateTime.msFromNano(1123000), 0.0001);
  }
  
  @Test
  public void msFromNanoDiff() {
    assertEquals(0, DateTime.msFromNanoDiff(1000000, 1000000), 0.0001);
    assertEquals(0.5, DateTime.msFromNanoDiff(1500000, 1000000), 0.0001);
    assertEquals(1.5, DateTime.msFromNanoDiff(1500000, 0), 0.0001);
    assertEquals(0.5, DateTime.msFromNanoDiff(-1000000, -1500000), 0.0001);
    try {
      assertEquals(0.5, DateTime.msFromNanoDiff(1000000, 1500000), 0.0001);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException e) {}
  }

  @Test
  public void parseDuration2() {
    TemporalAmount amount = DateTime.parseDuration2("1s");
    assertEquals(Duration.ofSeconds(1), amount);
    
    amount = DateTime.parseDuration2("2s");
    assertEquals(Duration.ofSeconds(2), amount);
    
    amount = DateTime.parseDuration2("1ns");
    assertEquals(Duration.ofNanos(1), amount);
    
    amount = DateTime.parseDuration2("100ns");
    assertEquals(Duration.ofNanos(100), amount);
    
    amount = DateTime.parseDuration2("1mu");
    assertEquals(Duration.ofNanos(1000), amount);
    
    amount = DateTime.parseDuration2("1ms");
    assertEquals(Duration.ofMillis(1), amount);
    
    amount = DateTime.parseDuration2("1m");
    assertEquals(Duration.ofMinutes(1), amount);
    
    amount = DateTime.parseDuration2("1h");
    assertEquals(Duration.ofHours(1), amount);
    
    amount = DateTime.parseDuration2("1d");
    assertEquals(Period.ofDays(1), amount);
    
    amount = DateTime.parseDuration2("1w");
    assertEquals(Period.ofWeeks(1), amount);
    
    amount = DateTime.parseDuration2("1n");
    assertEquals(Period.ofMonths(1), amount);
    
    amount = DateTime.parseDuration2("1y");
    assertEquals(Period.ofYears(1), amount);
    
    // floats are truncated
    amount = DateTime.parseDuration2("1.5s");
    assertEquals(Duration.ofSeconds(1), amount);
    
    amount = DateTime.parseDuration2("1.138483s");
    assertEquals(Duration.ofSeconds(1), amount);
    
    amount = DateTime.parseDuration2("0all");
    assertEquals(Duration.ofSeconds(0), amount);
    
    try {
      DateTime.parseDuration2(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DateTime.parseDuration2("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DateTime.parseDuration2("1");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DateTime.parseDuration2("s");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DateTime.parseDuration2("42p");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void unitsToChronoUnit() {
    assertEquals(ChronoUnit.NANOS, DateTime.unitsToChronoUnit("ns"));
    assertEquals(ChronoUnit.MICROS, DateTime.unitsToChronoUnit("mu"));
    assertEquals(ChronoUnit.MILLIS, DateTime.unitsToChronoUnit("ms"));
    assertEquals(ChronoUnit.SECONDS, DateTime.unitsToChronoUnit("s"));
    assertEquals(ChronoUnit.MINUTES, DateTime.unitsToChronoUnit("m"));
    assertEquals(ChronoUnit.HOURS, DateTime.unitsToChronoUnit("h"));
    assertEquals(ChronoUnit.DAYS, DateTime.unitsToChronoUnit("d"));
    assertEquals(ChronoUnit.WEEKS, DateTime.unitsToChronoUnit("w"));
    assertEquals(ChronoUnit.MONTHS, DateTime.unitsToChronoUnit("n"));
    assertEquals(ChronoUnit.YEARS, DateTime.unitsToChronoUnit("y"));
    
    try {
      DateTime.unitsToChronoUnit(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DateTime.unitsToChronoUnit("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DateTime.unitsToChronoUnit("p");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
