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

import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.junit.Test;

public final class TestDateTime {

  @Test
  public void getTimezone() {
    assertNotNull(DateTime.timezones.get("America/Los_Angeles"));
  }
  
  @Test
  public void getTimezoneNull() {
    assertNull(DateTime.timezones.get("Nothere"));
  }
  
  // NOTE: These relative tests *should* complete fast enough to pass
  // but there's a possibility that when run on a heavily used system
  // that the current time will change between calls. Thus the epsilon
  // is 5 ms
  @Test
  public void parseDateTimeStringRelativeS() {
    long t = DateTime.parseDateTimeString("60s-ago", null);
    long s = System.currentTimeMillis();
    assertEquals((s - t), 60000, 5);
  }
  
  @Test
  public void parseDateTimeStringRelativeM() {
    long t = DateTime.parseDateTimeString("1m-ago", null);
    long s = System.currentTimeMillis();
    assertEquals((s - t),  60000, 5);
  }
  
  @Test
  public void parseDateTimeStringRelativeH() {
    long t = DateTime.parseDateTimeString("2h-ago", null);
    long s = System.currentTimeMillis();
    assertEquals((s - t), 7200000, 5);
  }
  
  @Test
  public void parseDateTimeStringRelativeD() {
    long t = DateTime.parseDateTimeString("2d-ago", null);
    long s = System.currentTimeMillis();
    assertEquals((s - t), (2 * 3600 * 24 * 1000), 5);
  }
  
  @Test
  public void parseDateTimeStringRelativeW() {
    long t = DateTime.parseDateTimeString("3w-ago", null);
    long s = System.currentTimeMillis();
    assertEquals((s - t), (3 * 7 * 3600 * 24 * 1000), 5);
  }
  
  @Test
  public void parseDateTimeStringRelativeN() {
    long t = DateTime.parseDateTimeString("2n-ago", null);
    long s = System.currentTimeMillis();
    long diff = 2 * 30 * 3600 * 24;
    diff *= 1000;
    assertEquals((s - t), diff, 5);
  }
  
  @Test
  public void parseDateTimeStringRelativeY() {
    long t = DateTime.parseDateTimeString("2y-ago", null);
    long s = System.currentTimeMillis();
    long diff = 2 * 365 * 3600 * 24;
    diff *= 1000;
    assertEquals((s - t), diff, 5);
  }

  @Test
  public void parseDateTimeStringUnixSeconds() {
    long t = DateTime.parseDateTimeString("1355961600", null);
    assertEquals(t, 1355961600000L);
  }
  
  @Test
  public void parseDateTimeStringUnixMS() {
    long t = DateTime.parseDateTimeString("1355961603418", null);
    assertEquals(t, 1355961603418L);
  }
  
  @Test
  public void parseDateTimeStringDate() {
    long t = DateTime.parseDateTimeString("2012/12/20", "GMT");
    assertEquals(t, 1355961600000L);
  }
  
  @Test
  public void parseDateTimeStringDateTimeShort() {
    long t = DateTime.parseDateTimeString("2012/12/20 12:42", "GMT");
    assertEquals(t, 1356007320000L);
  }
  
  @Test
  public void parseDateTimeStringDateTimeDashShort() {
    long t = DateTime.parseDateTimeString("2012/12/20-12:42", "GMT");
    assertEquals(t, 1356007320000L);
  }
  
  @Test
  public void parseDateTimeStringDateTime() {
    long t = DateTime.parseDateTimeString("2012/12/20 12:42:42", "GMT");
    assertEquals(t, 1356007362000L);
  }
  
  @Test
  public void parseDateTimeStringDateTimeDash() {
    long t = DateTime.parseDateTimeString("2012/12/20-12:42:42", "GMT");
    assertEquals(t, 1356007362000L);
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
    assertEquals(t, -1);
  }
  
  @Test
  public void parseDateTimeStringEmpty() {
    long t = DateTime.parseDateTimeString("", "GMT");
    assertEquals(t, -1);
  }
  
  @Test
  public void parseDurationS() {
    long t = DateTime.parseDuration("60s");
    assertEquals(t, 60);
  }
  
  @Test
  public void parseDurationCase() {
    long t = DateTime.parseDuration("60S");
    assertEquals(t, 60);
  }
  
  @Test
  public void parseDurationM() {
    long t = DateTime.parseDuration("60m");
    assertEquals(t, 60 * 60);
  }
  
  @Test
  public void parseDurationH() {
    long t = DateTime.parseDuration("24h");
    assertEquals(t, 24 * 60 * 60);
  }
  
  @Test
  public void parseDurationD() {
    long t = DateTime.parseDuration("1d");
    assertEquals(t, 24 * 60 * 60);
  }
  
  @Test
  public void parseDurationW() {
    long t = DateTime.parseDuration("1w");
    assertEquals(t, 7 * 24 * 60 * 60);
  }
  
  @Test
  public void parseDurationN() {
    long t = DateTime.parseDuration("1n");
    assertEquals(t, 30 * 24 * 60 * 60);
  }
  
  @Test
  public void parseDurationY() {
    long t = DateTime.parseDuration("2y");
    assertEquals(t, 2 * 365 * 24 * 60 * 60);
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
    assertEquals(fmt.getTimeZone().getID(), "America/Los_Angeles");
  }
  
  @SuppressWarnings("null")
  @Test (expected = NullPointerException.class)
  public void setTimeZoneNullFmt() {
    SimpleDateFormat fmt = null;
    DateTime.setTimeZone(fmt, "America/Los_Angeles");
    assertEquals(fmt.getTimeZone().getID(), "America/Los_Angeles");
  }
  
  @Test
  public void setTimeZoneNullTZ() {
    SimpleDateFormat fmt = new SimpleDateFormat("yyyy/MM/dd");
    DateTime.setTimeZone(fmt, null);
    // This should return the default timezone for this box
    assertEquals(fmt.getTimeZone().getID(), TimeZone.getDefault().getID());
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
    assertEquals(TimeZone.getDefault().getID(), new_tz);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setDefaultTimezoneNull() {
    DateTime.setDefaultTimezone(null);
  }
}
