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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.TimeZone;

import com.google.common.base.Strings;

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
   * @param tz The timezone to use for parsing.
   * @return A Unix epoch timestamp in milliseconds
   * @throws NullPointerException if the timestamp is null
   * @throws IllegalArgumentException if the request was malformed 
   */
  public static final long parseDateTimeString(final String datetime, 
                                               final String tz) {
    if (datetime == null || datetime.isEmpty())
      return -1;

    if (datetime.matches("^[0-9]+ms$")) {
      return Long.parseLong(datetime.replaceFirst("^([0-9]+)(ms)$", "$1"));
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
        
        if (tz != null && !tz.isEmpty()) {
          setTimeZone(fmt, tz);
        } else {
          setTimeZone(fmt, "GMT");
        }
        return fmt.parse(datetime).getTime();
      } catch (ParseException e) {
        throw new IllegalArgumentException("Invalid date: " + datetime  
            + ". " + e.getMessage());
      }
    } else {
      try {
        long time;
        final boolean contains_dot = datetime.contains(".");
        // [0-9]{10} ten digits
        // \\. a dot
        // [0-9]{1,3} one to three digits
        final boolean valid_dotted_ms = 
            datetime.matches("^[0-9]{10}\\.[0-9]{1,3}$");
        if (contains_dot) {
          if (!valid_dotted_ms) {
            throw new IllegalArgumentException("Invalid time: " + datetime  
                + ". Millisecond timestamps must be in the format "
                + "<seconds>.<ms> where the milliseconds are limited to 3 digits");
          }
          time = Long.parseLong(datetime.replace(".", ""));   
        } else {
          time = Long.parseLong(datetime);
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
   * <li>{@code y}: years (365 days)</li>
   * <li>{@code all}: all time, returns 0 ms </li></ul>
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
    if (interval <= 0 && !duration.toLowerCase().endsWith("all")) {
      throw new IllegalArgumentException("Zero or negative duration: " + duration);
    }
    switch (duration.toLowerCase().charAt(duration.length() - 1)) {
      case 'l':
        if (duration.toLowerCase().endsWith("all")) {
          multiplier = 0;
        } else {
          throw new IllegalArgumentException("Invalid duration (suffix): " + duration);
        }
        break;
      case 's': 
        if (duration.toLowerCase().charAt(duration.length() - 2) == 'm') {
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
   * Parses a human-readable duration (e.g, "10m", "3h", "14d") into seconds.
   * <p>
   * Formats supported:<ul>
   * <li>{@code ns}: nanoseconds</li
   * <li>{@code mu}: microseconds</li
   * <li>{@code ms}: milliseconds</li>
   * <li>{@code s}: seconds</li>
   * <li>{@code m}: minutes</li>
   * <li>{@code h}: hours</li>
   * <li>{@code d}: days</li>
   * <li>{@code w}: weeks</li> 
   * <li>{@code n}: month</li>
   * <li>{@code y}: years</li>
   * <li>{@code all}: all time, returns 0 ms </li></ul>
   * @param duration The human-readable duration to parse.
   * @return A strictly positive number of milliseconds.
   * @throws IllegalArgumentException if the interval was malformed.
   * TODO - rename once we take care of parseDuration().
   */
  public static final TemporalAmount parseDuration2(final String duration) {
    if (Strings.isNullOrEmpty(duration)) {
      throw new IllegalArgumentException("Duration cannot be null or empty.");
    }
    final long interval;
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
    if (interval <= 0 && !duration.toLowerCase().endsWith("all")) {
      throw new IllegalArgumentException("Zero or negative duration: " + duration);
    }
    switch (duration.toLowerCase().charAt(duration.length() - 1)) {
      case 'l':
        if (duration.toLowerCase().endsWith("all")) {
          return Duration.ZERO;
        } else {
          throw new IllegalArgumentException("Invalid duration (suffix): " + duration);
        }
      case 'u':
        if (duration.toLowerCase().charAt(duration.length() - 2) != 'm') {
          throw new IllegalArgumentException("Invalid duration (suffix): " + duration);
        }
        return Duration.of(interval, ChronoUnit.MICROS);
      case 's': 
        if (duration.toLowerCase().charAt(duration.length() - 2) == 'm') {
          return Duration.of(interval, ChronoUnit.MILLIS);
        }
        if (duration.toLowerCase().charAt(duration.length() - 2) == 'n') {
          return Duration.of(interval, ChronoUnit.NANOS);
        }
        return Duration.of(interval, ChronoUnit.SECONDS);
      case 'm': return Duration.of(interval, ChronoUnit.MINUTES);    // minutes
      case 'h': return Duration.of(interval, ChronoUnit.HOURS);      // hours
      case 'd': return Period.ofDays((int) interval);       // days
      case 'w': return Period.ofWeeks((int) interval);      // weeks
      case 'n': return Period.ofMonths((int) interval);     // month (average)
      case 'y': return Period.ofYears((int) interval);      // years (screw leap years)
      default: throw new IllegalArgumentException("Invalid duration (suffix): " + duration);
    }
  }
  
  /**
   * Returns the suffix or "units" of the duration as a string. The result will
   * be ms, s, m, h, d, w, n or y.
   * @param duration The duration in the format #units, e.g. 1d or 6h
   * @return Just the suffix, e.g. 'd' or 'h'
   * @throws IllegalArgumentException if the duration is null, empty or if 
   * the units are invalid.
   * @since 2.4
   */
  public static final String getDurationUnits(final String duration) {
    if (duration == null || duration.isEmpty()) {
      throw new IllegalArgumentException("Duration cannot be null or empty");
    }
    int unit = 0;
    while (unit < duration.length() && 
        Character.isDigit(duration.charAt(unit))) {
      unit++;
    }
    final String units = duration.substring(unit).toLowerCase();
    if (units.equals("ns") || units.equals("mu") || units.equals("ms") || 
        units.equals("s") || units.equals("m") || units.equals("h") || 
        units.equals("d") || units.equals("w") || units.equals("n") || 
        units.equals("y")) {
      return units;
    }
    throw new IllegalArgumentException("Invalid units in the duration: " + units);
  }
  
  /**
   * Parses the prefix of the duration, the interval and returns it as a number.
   * E.g. if you supply "1d" it will return "1". If you supply "60m" it will
   * return "60".
   * @param duration The duration to parse in the format #units, e.g. "1d" or "60m"
   * @return The interval as an integer, regardless of units.
   * @throws IllegalArgumentException if the duration is null, empty or parsing
   * of the integer failed.
   * @since 2.4
   */
  public static final int getDurationInterval(final String duration) {
    if (duration == null || duration.isEmpty()) {
      throw new IllegalArgumentException("Duration cannot be null or empty");
    }
    if (duration.contains(".")) {
      throw new IllegalArgumentException("Floating point intervals are not supported");
    }
    int unit = 0;
    while (Character.isDigit(duration.charAt(unit))) {
      unit++;
    }
    int interval;
    try {
      interval = Integer.parseInt(duration.substring(0, unit));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid duration (number): " + duration);
    }
    if (interval <= 0) {
      throw new IllegalArgumentException("Zero or negative duration: " + duration);
    }
    return interval;
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
   * Pass through to {@link System#currentTimeMillis()} for use in classes to
   * make unit testing easier. Mocking System.class is a bad idea in general
   * so placing this here and mocking DateTime.class is MUCH cleaner.
   * @return The current epoch time in milliseconds
   * @since 2.1
   */
  public static long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  /**
   * Pass through to {@link System#nanoTime()} for use in classes to
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
   * Converts the given units to a {@link ChronoUnit}. Note that it does not
   * support all units, just nanos through years excepting half days.
   * @param units The units as a string.
   * @return A {@link ChronoUnit} if the units were recognized, an exception
   * if not.
   * @throws IllegalArgumentException if the units were not recognized.
   * @since 3.0
   */
  public static ChronoUnit unitsToChronoUnit(final String units) {
    if (Strings.isNullOrEmpty(units)) {
      throw new IllegalArgumentException("Units cannot be null or empty");
    }
    
    final String lc = units.toLowerCase();
    if (lc.equals("ns")) {
      return ChronoUnit.NANOS;
    } else if (lc.equals("mu")) {
      return ChronoUnit.MICROS;
    } else if (lc.equals("ms")) {
      return ChronoUnit.MILLIS;
    } else if (lc.equals("s")) {
      return ChronoUnit.SECONDS;
    } else if (lc.equals("m")) {
      return ChronoUnit.MINUTES;
    } else if (lc.equals("h")) {
      return ChronoUnit.HOURS;
    } else if (lc.equals("d")) {
      return ChronoUnit.DAYS;
    } else if (lc.equals("w")) {
      return ChronoUnit.WEEKS;
    } else if (lc.equals("n")) {
      return ChronoUnit.MONTHS;
    } else if (lc.equals("y")) {
      return ChronoUnit.YEARS;
    }
    throw new IllegalArgumentException("Unrecognized unit type: " + units);
  }
}
