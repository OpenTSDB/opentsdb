// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class, System.class })
public final class TestDateTime {

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
  }
  
  @Test
  public void parseDateTimeStringDateTimeDash() {
    long t = DateTime.parseDateTimeString("2012/12/20-12:42:42", "GMT");
    assertEquals(1356007362000L, t);
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

}
