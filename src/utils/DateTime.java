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
import java.util.Calendar;
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
  /** ID of the UTC timezone */
  public static final String UTC_ID = "UTC";
  
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
   * <li>1355961600000</li>
   * <li>1355961600.000</li></ul></li>
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

    if (datetime.matches("^[0-9]+ms$")) {
      return Tags.parseLong(datetime.replaceFirst("^([0-9]+)(ms)$", "$1"));
    }

    if (datetime.toLowerCase().equals("now")) {
      return System.currentTimeMillis();
    }

    if (datetime.toLowerCase().endsWith("-ago")) {
      long interval = DateTime.parseDuration(
        datetime.substring(0, datetime.length() - 4));
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
        long time;
        Boolean containsDot = datetime.contains(".");
        // [0-9]{10} ten digits
        // \\. a dot
        // [0-9]{1,3} one to three digits
        Boolean isValidDottedMillesecond = datetime.matches("^[0-9]{10}\\.[0-9]{1,3}$");
        // one to ten digits (0-9)
        Boolean isValidSeconds = datetime.matches("^[0-9]{1,10}$");
        if (containsDot) {
          if (!isValidDottedMillesecond) {
            throw new IllegalArgumentException("Invalid time: " + datetime  
                + ". Millisecond timestamps must be in the format "
                + "<seconds>.<ms> where the milliseconds are limited to 3 digits");
          }
          time = Tags.parseLong(datetime.replace(".", ""));   
        } else {
          time = Tags.parseLong(datetime);
        }
        if (time < 0) {
          throw new IllegalArgumentException("Invalid time: " + datetime  
              + ". Negative timestamps are not supported.");
        }
        // this is a nasty hack to determine if the incoming request is
        // in seconds or milliseconds. This will work until November 2286
        if (datetime.length() <= 10) {
          time *= 1000;
        }
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
   * <li>{@code ms}: milliseconds</li>
   * <li>{@code s}: seconds</li>
   * <li>{@code m}: minutes</li>
   * <li>{@code h}: hours</li>
   * <li>{@code d}: days</li>
   * <li>{@code w}: weeks</li> 
   * <li>{@code n}: month (30 days)</li>
   * <li>{@code y}: years (365 days)</li></ul>
   * @param duration The human-readable duration to parse.
   * @return A strictly positive number of milliseconds.
   * @throws IllegalArgumentException if the interval was malformed.
   */
  public static final long parseDuration(final String duration) {
    long interval;
    long multiplier;
    double temp;
    int unit = 0;
    while (Character.isDigit(duration.charAt(unit))) {
      unit++;
      if (unit >= duration.length()) {
        throw new IllegalArgumentException("Invalid duration, must have an "
            + "integer and unit: " + duration);
      }
    }
    try {
      interval = Long.parseLong(duration.substring(0, unit));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid duration (number): " + duration);
    }
    if (interval <= 0) {
      throw new IllegalArgumentException("Zero or negative duration: " + duration);
    }
    switch (duration.toLowerCase().charAt(duration.length() - 1)) {
      case 's': 
        if (duration.charAt(duration.length() - 2) == 'm') {
          return interval;
        }
        multiplier = 1; break;                        // seconds
      case 'm': multiplier = 60; break;               // minutes
      case 'h': multiplier = 3600; break;             // hours
      case 'd': multiplier = 3600 * 24; break;        // days
      case 'w': multiplier = 3600 * 24 * 7; break;    // weeks
      case 'n': multiplier = 3600 * 24 * 30; break;   // month (average)
      case 'y': multiplier = 3600 * 24 * 365; break;  // years (screw leap years)
      default: throw new IllegalArgumentException("Invalid duration (suffix): " + duration);
    }
    multiplier *= 1000;
    temp = (double)interval * multiplier;
    if (temp > Long.MAX_VALUE) {
      throw new IllegalArgumentException("Duration must be < Long.MAX_VALUE ms: " + duration);
    }
    return interval * multiplier;
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

  /**
   * Pass through to {@link System.currentTimeMillis} for use in classes to
   * make unit testing easier. Mocking System.class is a bad idea in general
   * so placing this here and mocking DateTime.class is MUCH cleaner.
   * @return The current epoch time in milliseconds
   * @since 2.1
   */
  public static long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  /**
   * Pass through to {@link System.nanoTime} for use in classes to
   * make unit testing easier. Mocking System.class is a bad idea in general
   * so placing this here and mocking DateTime.class is MUCH cleaner.
   * @return The current epoch time in milliseconds
   * @since 2.2
   */
  public static long nanoTime() {
    return System.nanoTime();
  }

  /**
   * Converts the long nanosecond value to a double in milliseconds
   * @param ts The timestamp or value in nanoseconds
   * @return The timestamp in milliseconds
   * @since 2.2
   */
  public static double msFromNano(final long ts) {
    return (double)ts / 1000000;
  }
  
  /**
   * Calculates the difference between two values and returns the time in
   * milliseconds as a double.
   * @param end The end timestamp
   * @param start The start timestamp
   * @return The value in milliseconds
   * @throws IllegalArgumentException if end is less than start
   * @since 2.2
   */
  public static double msFromNanoDiff(final long end, final long start) {
    if (end < start) {
      throw new IllegalArgumentException("End (" + end + ") cannot be less "
          + "than start (" + start + ")");
    }
    return ((double) end - (double) start) / 1000000;
  }
  
  /**
   * Returns a calendar set to the previous interval time based on the 
   * units and UTC the timezone. This allows for snapping to day, week, 
   * monthly, etc. boundaries. 
   * NOTE: It uses a calendar for snapping so isn't as efficient as a simple
   * modulo calculation.
   * NOTE: For intervals that don't nicely divide into their given unit (e.g.
   * a 23s interval where 60 seconds is not divisible by 23) the base time may
   * start at the top of the day (for ms and s) or from Unix epoch 0. In the
   * latter case, setting up the base timestamp may be slow if the caller does
   * something silly like "23m" where we iterate 23 minutes at a time from 0
   * till we find the proper timestamp.
   * TODO - There is likely a better way to do all of this 
   * @param ts The timestamp to find an interval for, in milliseconds as
   * a Unix epoch.
   * @param interval The interval as a measure of units.
   * @param unit The unit. This must cast to a Calendar time unit.
   * @return A calendar set to the timestamp aligned to the proper interval
   * before the given ts
   * @throws IllegalArgumentException if the timestamp is negative, if the 
   * interval is less than 1 or the unit is unrecognized.
   * @since 2.3
   */
  public static Calendar previousInterval(final long ts, final int interval, 
      final int unit) {
    return previousInterval(ts, interval, unit, null);
  }
  
  /**
   * Returns a calendar set to the previous interval time based on the 
   * units and timezone. This allows for snapping to day, week, monthly, etc.
   * boundaries. 
   * NOTE: It uses a calendar for snapping so isn't as efficient as a simple
   * modulo calculation.
   * NOTE: For intervals that don't nicely divide into their given unit (e.g.
   * a 23s interval where 60 seconds is not divisible by 23) the base time may
   * start at the top of the day (for ms and s) or from Unix epoch 0. In the
   * latter case, setting up the base timestamp may be slow if the caller does
   * something silly like "23m" where we iterate 23 minutes at a time from 0
   * till we find the proper timestamp.
   * TODO - There is likely a better way to do all of this 
   * @param ts The timestamp to find an interval for, in milliseconds as
   * a Unix epoch.
   * @param interval The interval as a measure of units.
   * @param unit The unit. This must cast to a Calendar time unit.
   * @param tz An optional timezone.
   * @return A calendar set to the timestamp aligned to the proper interval
   * before the given ts
   * @throws IllegalArgumentException if the timestamp is negative, if the 
   * interval is less than 1 or the unit is unrecognized.
   * @since 2.3
   */
  public static Calendar previousInterval(final long ts, final int interval, 
      final int unit, final TimeZone tz) {
    if (ts < 0) {
      throw new IllegalArgumentException("Timestamp cannot be less than zero");
    }
    if (interval < 1) {
      throw new IllegalArgumentException("Interval must be greater than zero");
    }
    
    int unit_override = unit;
    int interval_override = interval;
    final Calendar calendar;
    if (tz == null) {
      calendar = Calendar.getInstance(timezones.get(UTC_ID));
    } else {
      calendar = Calendar.getInstance(tz);
    }
    
    switch (unit_override) {
    case Calendar.MILLISECOND:
      if (1000 % interval_override == 0) {
        calendar.setTimeInMillis(ts);
        calendar.set(Calendar.MILLISECOND, 0);
        if (interval_override > 1000) {
          calendar.add(Calendar.MILLISECOND, -interval_override);
        }
      } else {
        // from top of minute
        calendar.setTimeInMillis(ts);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
      }
      break;
    case Calendar.SECOND:
      if (60 % interval_override == 0) {
        calendar.setTimeInMillis(ts);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        if (interval_override > 60) {
          calendar.add(Calendar.SECOND, -interval_override);
        }
      } else {
        // from top of hour
        calendar.setTimeInMillis(ts);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
      }
      break;
    case Calendar.MINUTE:
      if (60 % interval_override == 0) {
        calendar.setTimeInMillis(ts);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        if (interval_override > 60) {
          calendar.add(Calendar.MINUTE, -interval_override);
        }
      } else {
        // from top of day
        calendar.setTimeInMillis(ts);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
      }
      break;
    case Calendar.HOUR_OF_DAY:
      if (24 % interval_override == 0) {
        calendar.setTimeInMillis(ts);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        if (interval_override > 24) {
          calendar.add(Calendar.HOUR_OF_DAY, -interval_override);
        }
      } else {
        // from top of month
        calendar.setTimeInMillis(ts);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
      }
      break;
    case Calendar.DAY_OF_MONTH:
      if (interval_override == 1) {
        calendar.setTimeInMillis(ts);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
      } else {
        // from top of year
        calendar.setTimeInMillis(ts);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.MONTH, 0);
      }
      break;
    case Calendar.DAY_OF_WEEK:
      if (2 % interval_override == 0) {
        calendar.setTimeInMillis(ts);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.DAY_OF_WEEK, calendar.getFirstDayOfWeek());
      } else {
        // from top of year
        calendar.setTimeInMillis(ts);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_WEEK, calendar.getFirstDayOfWeek());
      }
      unit_override = Calendar.DAY_OF_MONTH;
      interval_override = 7;
      break;
    case Calendar.WEEK_OF_YEAR:
      // from top of year
      calendar.setTimeInMillis(ts);
      calendar.set(Calendar.MILLISECOND, 0);
      calendar.set(Calendar.SECOND, 0);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.HOUR_OF_DAY, 0);
      calendar.set(Calendar.DAY_OF_MONTH, 1);
      calendar.set(Calendar.MONTH, 0);
      break;
    case Calendar.MONTH:
    case Calendar.YEAR:
      calendar.setTimeInMillis(ts);
      calendar.set(Calendar.MILLISECOND, 0);
      calendar.set(Calendar.SECOND, 0);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.HOUR_OF_DAY, 0);
      calendar.set(Calendar.DAY_OF_MONTH, 1);
      calendar.set(Calendar.MONTH, 0);
      break;
    default:
      throw new IllegalArgumentException("Unexpected unit_overrides of type: "
          + unit_override);
    }
    
    if (calendar.getTimeInMillis() == ts) {
      return calendar;
    }
    // TODO optimize a bit. We probably don't need to go past then back.
    while (calendar.getTimeInMillis() <= ts) {
      calendar.add(unit_override, interval_override);
    }
    calendar.add(unit_override, -interval_override);
    return calendar;
  }

  /**
   * Return the proper Calendar time unit as an integer given the string
   * @param units The unit to parse
   * @return An integer matching a Calendar.<UNIT> enum
   * @throws IllegalArgumentException if the unit is null, empty or doesn't 
   * match one of the configured units.
   * @since 2.3
   */
  public static int unitsToCalendarType(final String units) {
    if (units == null || units.isEmpty()) {
      throw new IllegalArgumentException("Units cannot be null or empty");
    }
    
    final String lc = units.toLowerCase();
    if (lc.equals("ms")) {
      return Calendar.MILLISECOND;
    } else if (lc.equals("s")) {
      return Calendar.SECOND;
    } else if (lc.equals("m")) {
      return Calendar.MINUTE;
    } else if (lc.equals("h")) {
      return Calendar.HOUR_OF_DAY;
    } else if (lc.equals("d")) {
      return Calendar.DAY_OF_MONTH;
    } else if (lc.equals("w")) {
      return Calendar.DAY_OF_WEEK;
    } else if (lc.equals("n")) {
      return Calendar.MONTH;
    } else if (lc.equals("y")) {
      return Calendar.YEAR;
    }
    throw new IllegalArgumentException("Unrecognized unit type: " + units);
  }

}
