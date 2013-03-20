package net.opentsdb.utils;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.junit.Test;

public class TestDateTime {

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
  // that the current time will change between calls.
  @Test
  public void parseDateTimeStringRelativeS() {
    long t = DateTime.parseDateTimeString("60s-ago", null);
    long s = System.currentTimeMillis();
    assertTrue((s - t) == 60000);
  }
  
  @Test
  public void parseDateTimeStringRelativeM() {
    long t = DateTime.parseDateTimeString("1m-ago", null);
    long s = System.currentTimeMillis();
    assertTrue((s - t) == 60000);
  }
  
  @Test
  public void parseDateTimeStringRelativeH() {
    long t = DateTime.parseDateTimeString("2h-ago", null);
    long s = System.currentTimeMillis();
    assertTrue((s - t) == 7200000);
  }
  
  @Test
  public void parseDateTimeStringRelativeD() {
    long t = DateTime.parseDateTimeString("2d-ago", null);
    long s = System.currentTimeMillis();
    assertTrue((s - t) == (2 * 3600 * 24 * 1000));
  }
  
  @Test
  public void parseDateTimeStringRelativeW() {
    long t = DateTime.parseDateTimeString("3w-ago", null);
    long s = System.currentTimeMillis();
    assertTrue((s - t) == (3 * 7 * 3600 * 24 * 1000));
  }
  
  @Test
  public void parseDateTimeStringRelativeN() {
    long t = DateTime.parseDateTimeString("2n-ago", null);
    long s = System.currentTimeMillis();
    long diff = 2 * 30 * 3600 * 24;
    diff *= 1000;
    assertTrue((s - t) == diff);
  }
  
  @Test
  public void parseDateTimeStringRelativeY() {
    long t = DateTime.parseDateTimeString("2y-ago", null);
    long s = System.currentTimeMillis();
    long diff = 2 * 365 * 3600 * 24;
    diff *= 1000;
    assertTrue((s - t) == diff);
  }

  @Test
  public void parseDateTimeStringUnixSeconds() {
    long t = DateTime.parseDateTimeString("1355961600", null);
    assertTrue(t == 1355961600000L);
  }
  
  @Test
  public void parseDateTimeStringUnixMS() {
    long t = DateTime.parseDateTimeString("1355961603418", null);
    assertTrue(t == 1355961603418L);
  }
  
  @Test
  public void parseDateTimeStringDate() {
    long t = DateTime.parseDateTimeString("2012/12/20", "GMT");
    assertTrue(t == 1355961600000L);
  }
  
  @Test
  public void parseDateTimeStringDateTimeShort() {
    long t = DateTime.parseDateTimeString("2012/12/20 12:42", "GMT");
    assertTrue(t == 1356007320000L);
  }
  
  @Test
  public void parseDateTimeStringDateTimeDashShort() {
    long t = DateTime.parseDateTimeString("2012/12/20-12:42", "GMT");
    assertTrue(t == 1356007320000L);
  }
  
  @Test
  public void parseDateTimeStringDateTime() {
    long t = DateTime.parseDateTimeString("2012/12/20 12:42:42", "GMT");
    assertTrue(t == 1356007362000L);
  }
  
  @Test
  public void parseDateTimeStringDateTimeDash() {
    long t = DateTime.parseDateTimeString("2012/12/20-12:42:42", "GMT");
    assertTrue(t == 1356007362000L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDateTimeStringTooBig() {
    long t = DateTime.parseDateTimeString("1355961603587168438418", null);
    assertTrue(t == 1355961603418L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDateTimeStringBadFormat() {
    long t = DateTime.parseDateTimeString("2012/12/", "GMT");
    assertTrue(t == 1356007362000L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDateTimeStringBadRelative() {
    long t = DateTime.parseDateTimeString("1s", "GMT");
    assertTrue(t == 1356007362000L);
  }
  
  @Test
  public void parseDateTimeStringNull() {
    long t = DateTime.parseDateTimeString(null, "GMT");
    assertTrue(t == -1);
  }
  
  @Test
  public void parseDateTimeStringEmpty() {
    long t = DateTime.parseDateTimeString("", "GMT");
    assertTrue(t == -1);
  }
  
  @Test
  public void parseDurationS() {
    long t = DateTime.parseDuration("60s");
    assertTrue(t == 60);
  }
  
  @Test
  public void parseDurationCase() {
    long t = DateTime.parseDuration("60S");
    assertTrue(t == 60);
  }
  
  @Test
  public void parseDurationM() {
    long t = DateTime.parseDuration("60m");
    assertTrue(t == 60 * 60);
  }
  
  @Test
  public void parseDurationH() {
    long t = DateTime.parseDuration("24h");
    assertTrue(t == 24 * 60 * 60);
  }
  
  @Test
  public void parseDurationD() {
    long t = DateTime.parseDuration("1d");
    assertTrue(t == 24 * 60 * 60);
  }
  
  @Test
  public void parseDurationW() {
    long t = DateTime.parseDuration("1w");
    assertTrue(t == 7 * 24 * 60 * 60);
  }
  
  @Test
  public void parseDurationN() {
    long t = DateTime.parseDuration("1n");
    assertTrue(t == 30 * 24 * 60 * 60);
  }
  
  @Test
  public void parseDurationY() {
    long t = DateTime.parseDuration("2y");
    assertTrue(t == 2 * 365 * 24 * 60 * 60);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDurationNegative() {
    long t = DateTime.parseDuration("-60s");
    assertTrue(t == 60);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDurationBad() {
    long t = DateTime.parseDuration("foo60s");
    assertTrue(t == 60);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDurationInvalidSuffix() {
    long t = DateTime.parseDuration("60p");
    assertTrue(t == 60);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseDurationTooBig() {
    long t = DateTime.parseDuration("6393590450230209347573980s");
    assertTrue(t == 60);
  }
  
  @Test
  public void setTimeZone() {
    SimpleDateFormat fmt = new SimpleDateFormat("yyyy/MM/dd");
    DateTime.setTimeZone(fmt, "America/Los_Angeles");
    assertTrue(fmt.getTimeZone().getID().equals("America/Los_Angeles"));
  }
  
  @SuppressWarnings("null")
  @Test (expected = NullPointerException.class)
  public void setTimeZoneNullFmt() {
    SimpleDateFormat fmt = null;
    DateTime.setTimeZone(fmt, "America/Los_Angeles");
    assertTrue(fmt.getTimeZone().getID().equals("America/Los_Angeles"));
  }
  
  @Test
  public void setTimeZoneNullTZ() {
    SimpleDateFormat fmt = new SimpleDateFormat("yyyy/MM/dd");
    DateTime.setTimeZone(fmt, null);
    // This should return the default timezone for this box
    assertTrue(fmt.getTimeZone().getID().equals(TimeZone.getDefault().getID()));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setTimeZoneBadTZ() {
    SimpleDateFormat fmt = new SimpleDateFormat("yyyy/MM/dd");
    DateTime.setTimeZone(fmt, "NotHere");
    assertTrue(fmt.getTimeZone().getID().equals("America/Los_Angeles"));
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
    assertFalse(DateTime.isRelativeDate(null));
  }
  
  @Test
  public void setDefaultTimezone() {
    // because setting the default is thread local when a security manager is
    // present, we'll fail this test to warn users. We should be alright unless
    // someone tries embedding OpenTSDB in another app or app server
    assertNull(System.getSecurityManager());
    
    String current_tz = TimeZone.getDefault().getID();
    // flip between two choices so we can verify that the change holds
    TimeZone new_tz = current_tz.equals("UTC") ? 
        TimeZone.getTimeZone("America/New_York")
        : TimeZone.getTimeZone("UTC");
    TimeZone.setDefault(new_tz);
    assertTrue(TimeZone.getDefault().getID().equals(new_tz.getID()));
  }
}
