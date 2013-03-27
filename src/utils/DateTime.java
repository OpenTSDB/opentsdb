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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.TimeZone;

import net.opentsdb.core.Tags;

/**
 * Utility class that provides helpers for dealing with dates and timestamps.
 * In particular, this class handles parsing relative or human readable 
 * date/time strings provided in queries.
 * @since 2.0
 */
public class DateTime {

  /**
   * Immutable cache mapping a timezone name to its object.
   * We do this because the JDK's TimeZone class was implemented by retards,
   * and it's synchronized, going through a huge pile of code, and allocating
   * new objects all the time.  And to make things even better, if you ask for
   * a TimeZone that doesn't exist, it returns GMT!  It is thus impractical to
   * tell if the timezone name was valid or not.  JDK_brain_damage++;
   * Note: caching everything wastes a few KB on RAM (34KB on my system with
   * 611 timezones -- each instance is 56 bytes with the Sun JDK).
   */
  public static final HashMap<String, TimeZone> timezones;
  static {
    final String[] tzs = TimeZone.getAvailableIDs();
    timezones = new HashMap<String, TimeZone>(tzs.length);
    for (final String tz : tzs) {
      timezones.put(tz, TimeZone.getTimeZone(tz));
    }
  }
  
  /**
   * Attempts to parse a timestamp from a given string
   * Formats accepted are:
   * <ul>
   * <li>Relative: {@code 5m-ago}, {@code 1h-ago}, etc. See 
   * {@link #parseDuration}</li>
   * <li>Absolute human readable dates:
   * <ul><li>"yyyy/MM/dd-HH:mm:ss"</li>
   * <li>"yyyy/MM/dd HH:mm:ss"</li>
   * <li>"yyyy/MM/dd-HH:mm"</li>
   * <li>"yyyy/MM/dd HH:mm"</li>
   * <li>"yyyy/MM/dd"</li></ul></li>
   * <li>Unix Timestamp in seconds or milliseconds: 
   * <ul><li>1355961600</li>
   * <li>1355961600000</li></ul></li>
   * </ul>
   * @param datetime The string to parse a value for
   * @return A Unix epoch timestamp in milliseconds
   * @throws NullPointerException if the timestamp is null
   * @throws IllegalArgumentException if the request was malformed 
   */
  public static final long parseDateTimeString(final String datetime, 
      final String tz) {
    if (datetime == null || datetime.isEmpty())
      return -1;
    if (datetime.toLowerCase().endsWith("-ago")) {
      long interval = DateTime.parseDuration(
        datetime.substring(0, datetime.length() - 4)) * 1000;
      return System.currentTimeMillis() - interval;
    }
    
    if (datetime.contains("/") || datetime.contains(":")) {
      try {
        SimpleDateFormat fmt = null;
        switch (datetime.length()) {
          // these were pulled from cliQuery but don't work as intended since 
          // they assume a date of 1970/01/01. Can be fixed but may not be worth
          // it
          // case 5:
          //   fmt = new SimpleDateFormat("HH:mm");
          //   break;
          // case 8:
          //   fmt = new SimpleDateFormat("HH:mm:ss");
          //   break;
          case 10:
            fmt = new SimpleDateFormat("yyyy/MM/dd");
            break;
          case 16:
            if (datetime.contains("-"))
              fmt = new SimpleDateFormat("yyyy/MM/dd-HH:mm");
            else
              fmt = new SimpleDateFormat("yyyy/MM/dd HH:mm");
            break;
          case 19:
            if (datetime.contains("-"))
              fmt = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
            else
              fmt = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            break;
          default:
            // todo - deal with internationalization, other time formats
            throw new IllegalArgumentException("Invalid absolute date: " 
                + datetime);
        }
        if (tz != null && !tz.isEmpty())
          setTimeZone(fmt, tz);
        return fmt.parse(datetime).getTime();
      } catch (ParseException e) {
        throw new IllegalArgumentException("Invalid date: " + datetime  
            + ". " + e.getMessage());
      }
    } else {
      try {
        // todo - maybe deal with sssss.mmm unix times?
        long time = Tags.parseLong(datetime);   
        // this is a nasty hack to determine if the incoming request is
        // in seconds or milliseconds. This will work until November 2286
        if (datetime.length() <= 10)
          time *= 1000;
        return time;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid time: " + datetime  
            + ". " + e.getMessage());
      }
    }
  }
  
  /**
   * Parses a human-readable duration (e.g, "10m", "3h", "14d") into seconds.
   * <p>
   * Formats supported:<ul>
   * <li>{@code s}: seconds</li>
   * <li>{@code m}: minutes</li>
   * <li>{@code h}: hours</li>
   * <li>{@code d}: days</li>
   * <li>{@code w}: weeks</li> 
   * <li>{@code n}: month (30 days)</li>
   * <li>{@code y}: years (365 days)</li></ul>
   * Milliseconds are not supported since a relative request can't be submitted
   * by a human that fast. If an application needs it, they could use an 
   * absolute time.
   * @param duration The human-readable duration to parse.
   * @return A strictly positive number of seconds.
   * @throws IllegalArgumentException if the interval was malformed.
   */
  public static final long parseDuration(final String duration) {
    int interval;
    final int lastchar = duration.length() - 1;
    try {
      interval = Integer.parseInt(duration.substring(0, lastchar));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid duration (number): " + duration);
    }
    if (interval <= 0) {
      throw new IllegalArgumentException("Zero or negative duration: " + duration);
    }
    switch (duration.toLowerCase().charAt(lastchar)) {
      case 's': return interval;                    // seconds
      case 'm': return interval * 60;               // minutes
      case 'h': return interval * 3600;             // hours
      case 'd': return interval * 3600 * 24;        // days
      case 'w': return interval * 3600 * 24 * 7;    // weeks
      case 'n': return interval * 3600 * 24 * 30;   // month (average)
      case 'y': return interval * 3600 * 24 * 365;  // years (screw leap years)
    }
    throw new IllegalArgumentException("Invalid duration (suffix): " + duration);
  }

  /**
   * Returns whether or not a date is specified in a relative fashion.
   * <p>
   * A date is specified in a relative fashion if it ends in "-ago",
   * e.g. {@code 1d-ago} is the same as {@code 24h-ago}.
   * @param value The value to parse
   * @return {@code true} if the parameter is passed and is a relative date.
   * Note the method doesn't attempt to validate the relative date.  So this
   * function can return true on something that looks like a relative date,
   * but is actually invalid once we really try to parse it.
   * @throws NullPointerException if the value is null
   */
  public static boolean isRelativeDate(final String value) {
    return value.toLowerCase().endsWith("-ago");
  }
  
  /**
   * Applies the given timezone to the given date format.
   * @param fmt Date format to apply the timezone to.
   * @param tzname Name of the timezone, or {@code null} in which case this
   * function is a no-op.
   * @throws IllegalArgumentException if tzname isn't a valid timezone name.
   * @throws NullPointerException if the format is null
   */
  public static void setTimeZone(final SimpleDateFormat fmt,
                                  final String tzname) {
    if (tzname == null) {
      return;  // Use the default timezone.
    }
    final TimeZone tz = DateTime.timezones.get(tzname);
    if (tz != null) {
      fmt.setTimeZone(tz);
    } else {
      throw new IllegalArgumentException("Invalid timezone name: " + tzname);
    }
  }
  
  /**
   * Sets the default timezone for this running OpenTSDB instance
   * <p>
   * <b>WARNING</b> If OpenTSDB is used with a Security Manager, setting the default
   * timezone only works for the running thread. Otherwise it will work for the
   * entire application. 
   * <p>
   * @param tzname Name of the timezone to use
   * @throws IllegalArgumentException if tzname isn't a valid timezone name
   */
  public static void setDefaultTimezone(final String tzname) {
    final TimeZone tz = DateTime.timezones.get(tzname);
    if (tz != null) {
      TimeZone.setDefault(tz);
    } else {
      throw new IllegalArgumentException("Invalid timezone name: " + tzname);
    }
  }
}
