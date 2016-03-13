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
import java.util.Calendar;
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
  // 45 minute offset w DST
  final static TimeZone NZ = DateTime.timezones.get("Pacific/Chatham");
  // 12h offset w/o DST
  final static TimeZone TV = DateTime.timezones.get("Pacific/Funafuti");
  // 12h offset w DST
  final static TimeZone FJ = DateTime.timezones.get("Pacific/Fiji");
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
  public void previousIntervalMilliseconds() {
    // interval 1
    assertEquals(DST_TS, DateTime.previousInterval(DST_TS, 
        1, Calendar.MILLISECOND).getTimeInMillis());
    assertEquals(NON_DST_TS, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.MILLISECOND).getTimeInMillis());
    
    // interval 100
    assertEquals(1450152145100L, DateTime.previousInterval(DST_TS, 
        100, Calendar.MILLISECOND).getTimeInMillis());
    assertEquals(1450152145000L, DateTime.previousInterval(1450152145000L, 
        100, Calendar.MILLISECOND).getTimeInMillis());
    
    // odd interval
    assertEquals(1450152144769L, DateTime.previousInterval(DST_TS, 
        799, Calendar.MILLISECOND).getTimeInMillis());
    
    // TZs - all the same for ms
    assertEquals(1450152145100L, DateTime.previousInterval(DST_TS, 
        100, Calendar.MILLISECOND, AF).getTimeInMillis());
    assertEquals(1431699673400L, DateTime.previousInterval(NON_DST_TS, 
        100, Calendar.MILLISECOND, AF).getTimeInMillis());
    assertEquals(1450152145100L, DateTime.previousInterval(DST_TS, 
        100, Calendar.MILLISECOND, NZ).getTimeInMillis());
    assertEquals(1431699673400L, DateTime.previousInterval(NON_DST_TS, 
        100, Calendar.MILLISECOND, NZ).getTimeInMillis());
    assertEquals(1450152145100L, DateTime.previousInterval(DST_TS, 
        100, Calendar.MILLISECOND, TV).getTimeInMillis());
    assertEquals(1431699673400L, DateTime.previousInterval(NON_DST_TS, 
        100, Calendar.MILLISECOND, TV).getTimeInMillis());
    assertEquals(1450152145100L, DateTime.previousInterval(DST_TS, 
        100, Calendar.MILLISECOND, FJ).getTimeInMillis());
    assertEquals(1431699673400L, DateTime.previousInterval(NON_DST_TS, 
        100, Calendar.MILLISECOND, FJ).getTimeInMillis());
    
    // multiples
    assertEquals(1450152120000L, DateTime.previousInterval(DST_TS, 
        60000, Calendar.MILLISECOND).getTimeInMillis());
    assertEquals(1431699660000L, DateTime.previousInterval(NON_DST_TS, 
        60000, Calendar.MILLISECOND).getTimeInMillis());
  }
  
  @Test
  public void previousIntervalSeconds() {
    // interval 1
    assertEquals(1450152145000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.SECOND).getTimeInMillis());
    assertEquals(1431699673000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.SECOND).getTimeInMillis());
    
    // interval 30
    assertEquals(1450152120000L, DateTime.previousInterval(DST_TS, 
        30, Calendar.SECOND).getTimeInMillis());
    assertEquals(1431699660000L, DateTime.previousInterval(NON_DST_TS, 
        30, Calendar.SECOND).getTimeInMillis());
    assertEquals(1450152120000L, DateTime.previousInterval(1450152120000L, 
        30, Calendar.SECOND).getTimeInMillis());
        
    // odd interval
    assertEquals(1431699647000L, DateTime.previousInterval(NON_DST_TS, 
        29, Calendar.SECOND).getTimeInMillis());
    assertEquals(1450152145000L, DateTime.previousInterval(DST_TS, 
        29, Calendar.SECOND).getTimeInMillis());
    
    // TZs - all the same for seconds
    assertEquals(1450152120000L, DateTime.previousInterval(DST_TS, 
        30, Calendar.SECOND, AF).getTimeInMillis());
    assertEquals(1431699660000L, DateTime.previousInterval(NON_DST_TS, 
        30, Calendar.SECOND, AF).getTimeInMillis());
    assertEquals(1450152120000L, DateTime.previousInterval(DST_TS, 
        30, Calendar.SECOND, NZ).getTimeInMillis());
    assertEquals(1431699660000L, DateTime.previousInterval(NON_DST_TS, 
        30, Calendar.SECOND, NZ).getTimeInMillis());
    assertEquals(1450152120000L, DateTime.previousInterval(DST_TS, 
        30, Calendar.SECOND, TV).getTimeInMillis());
    assertEquals(1431699660000L, DateTime.previousInterval(NON_DST_TS, 
        30, Calendar.SECOND, TV).getTimeInMillis());
    assertEquals(1450152120000L, DateTime.previousInterval(DST_TS, 
        30, Calendar.SECOND, FJ).getTimeInMillis());
    assertEquals(1431699660000L, DateTime.previousInterval(NON_DST_TS, 
        30, Calendar.SECOND, FJ).getTimeInMillis());
    
    // multiples
    assertEquals(1450152000000L, DateTime.previousInterval(DST_TS, 
        60000, Calendar.SECOND).getTimeInMillis());
    assertEquals(1431698400000L, DateTime.previousInterval(NON_DST_TS, 
        60000, Calendar.SECOND).getTimeInMillis());
  }
  
  @Test
  public void previousIntervalMinutes() {
    // interval 1
    assertEquals(1450152120000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.MINUTE).getTimeInMillis());
    assertEquals(1431699660000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.MINUTE).getTimeInMillis());
    
    // interval 30
    assertEquals(1450152000000L, DateTime.previousInterval(DST_TS, 
        30, Calendar.MINUTE).getTimeInMillis());
    assertEquals(1431698400000L, DateTime.previousInterval(NON_DST_TS, 
        30, Calendar.MINUTE).getTimeInMillis());
    assertEquals(1431698400000L, DateTime.previousInterval(1431698400000L, 
        30, Calendar.MINUTE).getTimeInMillis());
        
    // odd interval
    assertEquals(1431698460000L, DateTime.previousInterval(NON_DST_TS, 
        29, Calendar.MINUTE).getTimeInMillis());
    assertEquals(1450151520000L, DateTime.previousInterval(DST_TS, 
        29, Calendar.MINUTE).getTimeInMillis());
    
    // TZs
    assertEquals(1450152000000L, DateTime.previousInterval(DST_TS, 
        30, Calendar.MINUTE, AF).getTimeInMillis());
    assertEquals(1431698400000L, DateTime.previousInterval(NON_DST_TS, 
        30, Calendar.MINUTE, AF).getTimeInMillis());
    // 15 min diff
    assertEquals(1450152000000L, DateTime.previousInterval(DST_TS, 
        15, Calendar.MINUTE, AF).getTimeInMillis());
    assertEquals(1431699300000L, DateTime.previousInterval(NON_DST_TS, 
        15, Calendar.MINUTE, AF).getTimeInMillis());
    // outliers @ 45 minutes
    assertEquals(1450151100000L, DateTime.previousInterval(DST_TS, 
        30, Calendar.MINUTE, NZ).getTimeInMillis());
    assertEquals(1431699300000L, DateTime.previousInterval(NON_DST_TS, 
        30, Calendar.MINUTE, NZ).getTimeInMillis());
    // back to normal
    assertEquals(1450152000000L, DateTime.previousInterval(DST_TS, 
        30, Calendar.MINUTE, TV).getTimeInMillis());
    assertEquals(1431698400000L, DateTime.previousInterval(NON_DST_TS, 
        30, Calendar.MINUTE, TV).getTimeInMillis());
    assertEquals(1450152000000L, DateTime.previousInterval(DST_TS, 
        30, Calendar.MINUTE, FJ).getTimeInMillis());
    assertEquals(1431698400000L, DateTime.previousInterval(NON_DST_TS, 
        30, Calendar.MINUTE, FJ).getTimeInMillis());
    
    // multiples
    assertEquals(1450152000000L, DateTime.previousInterval(DST_TS, 
        120, Calendar.MINUTE).getTimeInMillis());
    assertEquals(1431698400000L, DateTime.previousInterval(NON_DST_TS, 
        120, Calendar.MINUTE).getTimeInMillis());
  }
  
  @Test
  public void previousIntervalHours() {
    // interval 1
    assertEquals(1450152000000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.HOUR_OF_DAY).getTimeInMillis());
    assertEquals(1431698400000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.HOUR_OF_DAY).getTimeInMillis());
    
    // interval 12
    assertEquals(1450137600000L, DateTime.previousInterval(DST_TS, 
        12, Calendar.HOUR_OF_DAY).getTimeInMillis());
    assertEquals(1431691200000L, DateTime.previousInterval(NON_DST_TS, 
        12, Calendar.HOUR_OF_DAY).getTimeInMillis());
    assertEquals(1450137600000L, DateTime.previousInterval(1450137600000L, 
        12, Calendar.HOUR_OF_DAY).getTimeInMillis());
        
    // odd interval
    assertEquals(1431680400000L, DateTime.previousInterval(NON_DST_TS, 
        15, Calendar.HOUR_OF_DAY).getTimeInMillis());
    assertEquals(1450116000000L, DateTime.previousInterval(DST_TS, 
        15, Calendar.HOUR_OF_DAY).getTimeInMillis());
    
    // TZs - 30m offset here
    assertEquals(1450121400000L, DateTime.previousInterval(DST_TS, 
        12, Calendar.HOUR_OF_DAY, AF).getTimeInMillis());
    assertEquals(1431675000000L, DateTime.previousInterval(NON_DST_TS, 
        12, Calendar.HOUR_OF_DAY, AF).getTimeInMillis());
    // outliers @ 45 minutes
    assertEquals(1450131300000L, DateTime.previousInterval(DST_TS, 
        12, Calendar.HOUR_OF_DAY, NZ).getTimeInMillis());
    assertEquals(1431688500000L, DateTime.previousInterval(NON_DST_TS, 
        12, Calendar.HOUR_OF_DAY, NZ).getTimeInMillis());
    // back to normal
    assertEquals(1450137600000L, DateTime.previousInterval(DST_TS, 
        12, Calendar.HOUR_OF_DAY, TV).getTimeInMillis());
    assertEquals(1431691200000L, DateTime.previousInterval(NON_DST_TS, 
        12, Calendar.HOUR_OF_DAY, TV).getTimeInMillis());
    assertEquals(1450134000000L, DateTime.previousInterval(DST_TS, 
        12, Calendar.HOUR_OF_DAY, FJ).getTimeInMillis());
    assertEquals(1431691200000L, DateTime.previousInterval(NON_DST_TS, 
        12, Calendar.HOUR_OF_DAY, FJ).getTimeInMillis());
    
    // multiples
    assertEquals(1450094400000L, DateTime.previousInterval(DST_TS, 
        36, Calendar.HOUR_OF_DAY).getTimeInMillis());
    assertEquals(1431604800000L, DateTime.previousInterval(NON_DST_TS, 
        36, Calendar.HOUR_OF_DAY).getTimeInMillis());
  }
  
  @Test
  public void previousIntervalDays() {
    // interval 1
    assertEquals(1450137600000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.DAY_OF_MONTH).getTimeInMillis());
    assertEquals(1431648000000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.DAY_OF_MONTH).getTimeInMillis());
    
    // interval 7 - since days aren't consistent, the only thing we can
    // do is pick a starting day, i.e. start of the year
    assertEquals(1449705600000L, DateTime.previousInterval(DST_TS, 
        7, Calendar.DAY_OF_MONTH).getTimeInMillis());
    assertEquals(1431561600000L, DateTime.previousInterval(NON_DST_TS, 
        7, Calendar.DAY_OF_MONTH).getTimeInMillis());
    assertEquals(1449705600000L, DateTime.previousInterval(1449705600000L, 
        7, Calendar.DAY_OF_MONTH).getTimeInMillis());
    
    // leap year
    assertEquals(1330473600000L, DateTime.previousInterval(1330516800000L,  
        1, Calendar.DAY_OF_MONTH).getTimeInMillis());
    
    // TZs - 30m offset here
    assertEquals(1450121400000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.DAY_OF_MONTH, AF).getTimeInMillis());
    assertEquals(1431631800000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.DAY_OF_MONTH, AF).getTimeInMillis());
    // outliers @ 45 minutes
    assertEquals(1450088100000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.DAY_OF_MONTH, NZ).getTimeInMillis());
    assertEquals(1431688500000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.DAY_OF_MONTH, NZ).getTimeInMillis());
    // back to normal
    assertEquals(1450094400000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.DAY_OF_MONTH, TV).getTimeInMillis());
    assertEquals(1431691200000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.DAY_OF_MONTH, TV).getTimeInMillis());
    assertEquals(1450090800000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.DAY_OF_MONTH, FJ).getTimeInMillis());
    assertEquals(1431691200000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.DAY_OF_MONTH, FJ).getTimeInMillis());
    
    // multiples
    assertEquals(1445990400000L, DateTime.previousInterval(DST_TS, 
        60, Calendar.DAY_OF_MONTH).getTimeInMillis());
    assertEquals(1430438400000L, DateTime.previousInterval(NON_DST_TS, 
        60, Calendar.DAY_OF_MONTH).getTimeInMillis());
  }
  
  @Test
  public void previousIntervalWeeks() {
    // interval 1 DST_TS starts on 13th of Dec, NON starts on the 10th of May
    assertEquals(1449964800000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.DAY_OF_WEEK).getTimeInMillis());
    assertEquals(1431216000000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.DAY_OF_WEEK).getTimeInMillis());
    
    // interval 2
    assertEquals(1449964800000L, DateTime.previousInterval(DST_TS, 
        2, Calendar.DAY_OF_WEEK).getTimeInMillis());
    assertEquals(1431216000000L, DateTime.previousInterval(NON_DST_TS, 
        2, Calendar.DAY_OF_WEEK).getTimeInMillis());
    assertEquals(1435449600000L, DateTime.previousInterval(1435795200000L, 
        2, Calendar.DAY_OF_WEEK).getTimeInMillis());
    
    // TZs - 30m offset here
    assertEquals(1449948600000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.DAY_OF_WEEK, AF).getTimeInMillis());
    assertEquals(1431199800000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.DAY_OF_WEEK, AF).getTimeInMillis());
    // outliers @ 45 minutes
    assertEquals(1449915300000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.DAY_OF_WEEK, NZ).getTimeInMillis());
    assertEquals(1431170100000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.DAY_OF_WEEK, NZ).getTimeInMillis());
    // back to normal
    assertEquals(1449921600000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.DAY_OF_WEEK, TV).getTimeInMillis());
    assertEquals(1431172800000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.DAY_OF_WEEK, TV).getTimeInMillis());
    assertEquals(1449918000000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.DAY_OF_WEEK, FJ).getTimeInMillis());
    assertEquals(1431172800000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.DAY_OF_WEEK, FJ).getTimeInMillis());
    
    // multiples - still from start of the week
    assertEquals(1449964800000L, DateTime.previousInterval(DST_TS, 
        104, Calendar.DAY_OF_WEEK).getTimeInMillis());
    assertEquals(1431216000000L, DateTime.previousInterval(NON_DST_TS, 
        104, Calendar.DAY_OF_WEEK).getTimeInMillis());
  }
  
  @Test
  public void previousIntervalWeekOfYear() {
    // interval 1 DST_TS starts on 10th of Dec, NON starts on the 14th of May
    assertEquals(1449705600000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.WEEK_OF_YEAR).getTimeInMillis());
    assertEquals(1431561600000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.WEEK_OF_YEAR).getTimeInMillis());
    
    // interval 26
    assertEquals(1435795200000L, DateTime.previousInterval(DST_TS, 
        26, Calendar.WEEK_OF_YEAR).getTimeInMillis());
    assertEquals(1420070400000L, DateTime.previousInterval(NON_DST_TS, 
        26, Calendar.WEEK_OF_YEAR).getTimeInMillis());
    assertEquals(1435795200000L, DateTime.previousInterval(1435795200000L, 
        26, Calendar.WEEK_OF_YEAR).getTimeInMillis());
    
    // TZs - 30m offset here
    assertEquals(1449689400000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.WEEK_OF_YEAR, AF).getTimeInMillis());
    assertEquals(1431545400000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.WEEK_OF_YEAR, AF).getTimeInMillis());
    // outliers @ 45 minutes
    assertEquals(1449656100000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.WEEK_OF_YEAR, NZ).getTimeInMillis());
    assertEquals(1431515700000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.WEEK_OF_YEAR, NZ).getTimeInMillis());
    // back to normal
    assertEquals(1449662400000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.WEEK_OF_YEAR, TV).getTimeInMillis());
    assertEquals(1431518400000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.WEEK_OF_YEAR, TV).getTimeInMillis());
    assertEquals(1449658800000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.WEEK_OF_YEAR, FJ).getTimeInMillis());
    assertEquals(1431518400000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.WEEK_OF_YEAR, FJ).getTimeInMillis());
    
    // multiples
    assertEquals(1420070400000L, DateTime.previousInterval(DST_TS, 
        104, Calendar.WEEK_OF_YEAR).getTimeInMillis());
    assertEquals(1420070400000L, DateTime.previousInterval(NON_DST_TS, 
        104, Calendar.WEEK_OF_YEAR).getTimeInMillis());
  }
  
  @Test
  public void previousIntervalMonths() {
    // interval 1
    assertEquals(1448928000000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.MONTH).getTimeInMillis());
    assertEquals(1430438400000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.MONTH).getTimeInMillis());
    
    // interval 3 (quarters)
    assertEquals(1443657600000L, DateTime.previousInterval(DST_TS, 
        3, Calendar.MONTH).getTimeInMillis());
    assertEquals(1427846400000L, DateTime.previousInterval(NON_DST_TS, 
        3, Calendar.MONTH).getTimeInMillis());
    assertEquals(1443657600000L, DateTime.previousInterval(1443657600000L, 
        3, Calendar.MONTH).getTimeInMillis());
    
    // odd intervals
    assertEquals(1446336000000L, DateTime.previousInterval(DST_TS, 
        5, Calendar.MONTH).getTimeInMillis());
    assertEquals(1420070400000L, DateTime.previousInterval(NON_DST_TS, 
        5, Calendar.MONTH).getTimeInMillis());
    
    // TZs - 30m offset here
    assertEquals(1448911800000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.MONTH, AF).getTimeInMillis());
    assertEquals(1430422200000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.MONTH, AF).getTimeInMillis());
    // outliers @ 45 minutes
    assertEquals(1448878500000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.MONTH, NZ).getTimeInMillis());
    assertEquals(1430392500000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.MONTH, NZ).getTimeInMillis());
    // back to normal
    assertEquals(1448884800000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.MONTH, TV).getTimeInMillis());
    assertEquals(1430395200000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.MONTH, TV).getTimeInMillis());
    assertEquals(1448881200000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.MONTH, FJ).getTimeInMillis());
    assertEquals(1430395200000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.MONTH, FJ).getTimeInMillis());
    
    // multiples
    assertEquals(1420070400000L, DateTime.previousInterval(DST_TS, 
        24, Calendar.MONTH).getTimeInMillis());
    assertEquals(1420070400000L, DateTime.previousInterval(NON_DST_TS, 
        24, Calendar.MONTH).getTimeInMillis());
  }
  
  @Test
  public void previousIntervalYears() {
    // interval 1
    assertEquals(1420070400000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.YEAR).getTimeInMillis());
    assertEquals(1420070400000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.YEAR).getTimeInMillis());
    
    // interval 5
    assertEquals(1420070400000L, DateTime.previousInterval(DST_TS, 
        5, Calendar.YEAR).getTimeInMillis());
    assertEquals(1420070400000L, DateTime.previousInterval(NON_DST_TS, 
        5, Calendar.YEAR).getTimeInMillis());
    assertEquals(1420070400000L, DateTime.previousInterval(1420070400000L, 
        5, Calendar.YEAR).getTimeInMillis());
    
    // TZs - 30m offset here
    assertEquals(1420054200000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.YEAR, AF).getTimeInMillis());
    assertEquals(1420054200000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.YEAR, AF).getTimeInMillis());
    // outliers @ 45 minutes
    assertEquals(1420020900000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.YEAR, NZ).getTimeInMillis());
    assertEquals(1420020900000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.YEAR, NZ).getTimeInMillis());
    // back to normal
    assertEquals(1420027200000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.YEAR, TV).getTimeInMillis());
    assertEquals(1420027200000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.YEAR, TV).getTimeInMillis());
    assertEquals(1420023600000L, DateTime.previousInterval(DST_TS, 
        1, Calendar.YEAR, FJ).getTimeInMillis());
    assertEquals(1420023600000L, DateTime.previousInterval(NON_DST_TS, 
        1, Calendar.YEAR, FJ).getTimeInMillis());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void previousIntervalNegativeTs() {
    DateTime.previousInterval(-42, 1, Calendar.MINUTE);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void previousIntervalNegativeInterval() {
    DateTime.previousInterval(1355961600000L, -1, Calendar.MINUTE);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void previousIntervalZeroInterval() {
    DateTime.previousInterval(1355961600000L, 0, Calendar.MINUTE);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void previousIntervalNegativeUnit() {
    DateTime.previousInterval(1355961600000L, 1, -1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void previousIntervalUnsupportedUnit() {
    DateTime.previousInterval(1355961600000L, 1, Calendar.HOUR);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void previousIntervalMassiveUnit() {
    DateTime.previousInterval(1355961600000L, 1, 6048);
  }
  
  @Test
  public void unitsToCalendarType() {
    assertEquals(Calendar.MILLISECOND, DateTime.unitsToCalendarType("ms"));
    assertEquals(Calendar.SECOND, DateTime.unitsToCalendarType("s"));
    assertEquals(Calendar.MINUTE, DateTime.unitsToCalendarType("m"));
    assertEquals(Calendar.HOUR_OF_DAY, DateTime.unitsToCalendarType("h"));
    assertEquals(Calendar.DAY_OF_MONTH, DateTime.unitsToCalendarType("d"));
    assertEquals(Calendar.DAY_OF_WEEK, DateTime.unitsToCalendarType("w"));
    assertEquals(Calendar.MONTH, DateTime.unitsToCalendarType("n"));
    assertEquals(Calendar.YEAR, DateTime.unitsToCalendarType("y"));
    try {
      DateTime.unitsToCalendarType("j");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      DateTime.unitsToCalendarType(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      DateTime.unitsToCalendarType("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
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
