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

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalAmount;

/**
 * A timestamp implemented with a {@link ZonedDateTime} under the hood. The ZDT
 * by itself takes up 24 bytes with just internal references so this should only
 * be used when time zones are in play such as a calendar based downsample or
 * period over period alignment. Supports nano-second precision. 
 * <p>
 * This implementation should only be used when calendar alignment is required.
 * <p>
 * <b>Notes on {@link #snapToPreviousInterval(long, ChronoUnit, DayOfWeek)}:</b>
 * <p>
 * This class uses the calendar to align on the proper interval using the given
 * timezone. For example, if the timestamp should snap to the top of the hour at
 * 8 AM, for UTC it would snap to 8:00:00 with zero nanos. However for 
 * Afghanistan, wich is offset by 30 minutes, the UTC time would be 8:30:00 or
 * 7:30:00 depending on the current value.
 * <p>
 * For each {@link ChronoUnit}, the {@code interval} value should evenly divide
 * into the number of intervals per unit and be less than or equal to the median
 * of units. E.g. if {@link ChronoUnit#SECONDS} is given, the interval should be 
 * one of 1, 2, 5, 10, 15, 30. This allows us to snap cleanly and quickly to
 * the proper interval by first snapping to the top of the minute then 
 * iterating to the interval just before the current time.
 * <p>
 * However if the interval does <i>not</i> cleanly divide into the units then we
 * will move to the next, lower, resolution unit and iterate from that until we
 * reach the previous interval. For example, if we have an interval of 7 and 
 * units of {@link ChronoUnit#SECONDS}, something not divisible into 60, then we
 * snap to the top of the <b>hour</b> instead of <b>minutes</b> and add 7 
 * seconds until we reach the proper time. This can be confusing and leads to
 * changes in timestamps when querying over different periods so try to use a
 * properly interval count that aligns with the given units.
 * <p>
 * For intervals greater than the maximum number of slices in a unit, they may be
 * treated at the next lower resolution if they align. For instance, if the
 * interval is 3600 and units are set to {@link ChronoUnit#SECONDS}, then it
 * becomes an interval of 1 with units of {@link ChronoUnit#HOURS}. Thus on 
 * incrementing the time via {@link #add(TemporalAmount)}, leap years, seconds
 * and DST are taken into account. For odd intervals greater than the units, the
 * previous rule applies.
 * <p>
 * When snapping to lower resolution intervals like weeks, months or years, for
 * any interval greater than a year, the snap will start from the 1st of January
 * 1970 or Unix Epoch equivalent 0. The interval will iterate from then to the
 * proper time just before the current timestamp.
 * <p>
 * The default day of week is {@link DayOfWeek#SUNDAY} when snapping to weeks.
 * 
 * @since 3.0
 */
public class ZonedNanoTimeStamp implements TimeStamp {
  /** The timestamp. */
  private ZonedDateTime timestamp;
  
  public ZonedNanoTimeStamp(final ZonedDateTime timestamp) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    this.timestamp = ZonedDateTime.from(timestamp);
  }
  
  public ZonedNanoTimeStamp(long epoch_millis, ZoneId zone) {
    if (zone == null) {
      throw new IllegalArgumentException("Zone cannot be null.");
    }
    timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(epoch_millis), zone);
  }
  
  public ZonedNanoTimeStamp(final long epoch, final long nano, final ZoneId zone) {
    if (zone == null) {
      throw new IllegalArgumentException("Zone cannot be null.");
    }
    timestamp = ZonedDateTime.ofInstant(Instant.ofEpochSecond(epoch, nano), zone);
  }
  
  @Override
  public long nanos() {
    return Instant.from(timestamp).getNano();
  }

  @Override
  public long msEpoch() {
    return Instant.from(timestamp).toEpochMilli();
  }

  @Override
  public long epoch() {
    return Instant.from(timestamp).getEpochSecond();
  }

  @Override
  public void updateMsEpoch(final long timestamp) {
    this.timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), 
        this.timestamp.getZone());
  }

  @Override
  public void updateEpoch(final long timestamp) {
    this.timestamp = ZonedDateTime.ofInstant(Instant.ofEpochSecond(timestamp), 
        this.timestamp.getZone());
  }

  @Override
  public void update(final TimeStamp timestamp) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    // doesn't appear to be a real shortcut.
    this.timestamp = ZonedDateTime.ofInstant(
        Instant.ofEpochSecond(timestamp.epoch(), timestamp.nanos()), 
        timestamp.timezone());
  }

  @Override
  public void update(final long epoch, final long nano) {
    this.timestamp = ZonedDateTime.ofInstant(
        Instant.ofEpochSecond(epoch, nano), 
        this.timestamp.getZone());
  }

  @Override
  public TimeStamp getCopy() {
    return new ZonedNanoTimeStamp(timestamp);
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof TimeStamp)) {
      return false;
    }
    return compare(Op.EQ, (TimeStamp) o);
  }
  
  @Override
  public boolean compare(final Op comparator, 
                         final TimeStamp compareTo) {
    if (compareTo == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    if (comparator == null) {
      throw new IllegalArgumentException("Comparator cannot be null.");
    }
    
    switch (comparator) {
    case LT:
      if (epoch() == compareTo.epoch()) {
        if (nanos() < compareTo.nanos()) {
          return true;
        }
      } else if (epoch() < compareTo.epoch()) {
        return true;
      }
      return false;
    case LTE:
      if (epoch() == compareTo.epoch()) {
        if (nanos() <= compareTo.nanos()) {
          return true;
        }
      } else if (epoch() <= compareTo.epoch()) {
        return true;
      }
      return false;
    case GT:
      if (epoch() == compareTo.epoch()) {
        if (nanos() > compareTo.nanos()) {
          return true;
        }
      } else if (epoch() > compareTo.epoch()) {
        return true;
      }
      return false;
    case GTE:
      if (epoch() == compareTo.epoch()) {
        if (nanos() >= compareTo.nanos()) {
          return true;
        }
      } else if (epoch() >= compareTo.epoch()) {
        return true;
      }
      return false;
    case EQ:
      return epoch() == compareTo.epoch() && 
             nanos() == compareTo.nanos();
    case NE:
      return epoch() != compareTo.epoch() || 
             nanos() != compareTo.nanos();
    default:
      throw new UnsupportedOperationException("Unknown comparator: " + comparator);
    }
  }

  @Override
  public void setMax() {
    // can't use LocalDateTime.MAX as it throws an ArithmeticException when we
    // try to pull out the millis. Instead we have to set to max Int for epoch
    timestamp = ZonedDateTime.ofInstant(
        Instant.ofEpochSecond(Integer.MAX_VALUE, 999999999), timestamp.getZone());
  }

  @Override
  public ChronoUnit units() {
    return ChronoUnit.NANOS;
  }
  
  @Override
  public ZoneId timezone() {
    return timestamp.getZone();
  }

  @Override
  public void add(final TemporalAmount amount) {
    if (amount == null) {
      throw new IllegalArgumentException("Amount cannot be null.");
    }
    timestamp = timestamp.plus(amount);
  }
  
  @Override
  public void snapToPreviousInterval(final long interval, final ChronoUnit units) {
    snapToPreviousInterval(interval, units, DayOfWeek.SUNDAY);
  }
  
  @Override
  public void snapToPreviousInterval(final long interval, 
                                     final ChronoUnit units, 
                                     final DayOfWeek day_of_week) {
    if (units == null) {
      throw new IllegalArgumentException("Unit cannot be null.");
    }
    if (day_of_week == null) {
      throw new IllegalArgumentException("Day of week cannot be null.");
    }
    if (interval < 1) {
      throw new IllegalArgumentException("Interval must be 1 or greater.");
    }
    final ZonedNanoTimeStamp original = new ZonedNanoTimeStamp(timestamp);
    TemporalAmount increment = null;
    switch (units) {
    case YEARS:
      increment = Period.ofYears((int) interval);
      break;
    case MONTHS:
      increment = Period.ofMonths((int) interval);
      break;
    case WEEKS:
      increment = Period.ofWeeks((int) interval);
      break;
    default:
      increment = Duration.of(interval, units);
    }
    
    long computed_interval = interval;
    boolean skip = false;
    switch (units) {
    case NANOS:
      if (computed_interval == 1) {
        // short circuit!
        return;
      }
      if (1000 % computed_interval == 0) {
        timestamp = timestamp.truncatedTo(ChronoUnit.MICROS);
        break;
      }
      if (computed_interval < 1000) {
        computed_interval = 1;
      } else {
        computed_interval /= 1000;
      }
      // fall through
    case MICROS:
      if (computed_interval == 0) {
        computed_interval = 1;
      }
      if (1000 % computed_interval == 0) {
        timestamp = timestamp.truncatedTo(ChronoUnit.MILLIS);
        break;
      }
      if (computed_interval < 1000) {
        computed_interval = 1;
      } else {
        computed_interval /= 1000;
      }
      // fall through
    case MILLIS:
      if (1000 % computed_interval == 0) {
        timestamp = timestamp.truncatedTo(ChronoUnit.SECONDS);
        break;
      }
      if (computed_interval < 1000) {
        computed_interval = 1;
      } else {
        computed_interval /= 1000;
      }
      // fall through
    case SECONDS:
      if (60 % computed_interval == 0) {
        timestamp = timestamp.truncatedTo(ChronoUnit.MINUTES);
        break;
      }
      if (computed_interval < 60) {
        computed_interval = 1;
      } else {
        computed_interval /= 60;
      }
      // fall through
    case MINUTES:
      if (60 % computed_interval == 0) {
        timestamp = timestamp.truncatedTo(ChronoUnit.HOURS);
        break;
      }
      if (computed_interval < 60) {
        computed_interval = 1;
      } else {
        computed_interval /= 60;
      }
      // fall through
    case HOURS:
      // TODO - test me on leap days
      if (24 % computed_interval == 0) {
        timestamp = timestamp.truncatedTo(ChronoUnit.DAYS);
        break;
      }
      if (computed_interval < 24) {
        computed_interval = 1;
      } else {
        if (computed_interval % 24 == 0) {
          computed_interval /= 24;          
        } else {
          computed_interval = (computed_interval / 24) + 1;
        }
      }
      // fall through
    case DAYS:
      if (computed_interval == 1) {
        timestamp = timestamp.truncatedTo(ChronoUnit.DAYS);
        timestamp = timestamp.with(TemporalAdjusters.firstDayOfMonth());
        break;
      }
      
      if (computed_interval % 7 == 0 && computed_interval < 52) {
        // user actually wanted weekly.
        computed_interval /= 7;
      } else if (computed_interval <= 365) {
        timestamp = timestamp.truncatedTo(ChronoUnit.DAYS);
        timestamp = timestamp.with(TemporalAdjusters.firstDayOfYear());
        break;
      } else {
        computed_interval = 1;
        skip = true;
      }
      // fall through
    case WEEKS:
      if (!skip) {
        if (computed_interval == 1) {
          timestamp = timestamp.truncatedTo(ChronoUnit.DAYS);
          timestamp = timestamp.with(TemporalAdjusters.firstDayOfMonth());
          timestamp = timestamp.with(day_of_week);
          break;
        } else if (computed_interval <= 52) {
          timestamp = timestamp.truncatedTo(ChronoUnit.DAYS);
          timestamp = timestamp.with(TemporalAdjusters.firstDayOfYear());
          timestamp = timestamp.with(day_of_week);
          break;
        } else {
          timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(0), timestamp.getZone());
          timestamp = timestamp.with(TemporalAdjusters.firstDayOfYear());
          timestamp = timestamp.with(day_of_week);
          break;
        }
      }
      // fall through
    case MONTHS:
      if (computed_interval <= 6 || computed_interval == 12) {
        timestamp = timestamp.truncatedTo(ChronoUnit.DAYS);
        timestamp = timestamp.with(TemporalAdjusters.firstDayOfYear());
        break;
      }
      computed_interval = -1;
      // fall through
    case YEARS:
      if (computed_interval == 1) {
        timestamp = timestamp.truncatedTo(ChronoUnit.DAYS);
        timestamp = timestamp.with(TemporalAdjusters.firstDayOfYear());
        break;
      }
      // ug
      // TODO - year check ,can't go > now - 1970 in years.
      if (timestamp.getYear() < 1970) {
        throw new IllegalArgumentException("Timestamp year cannot be less "
            + "than 1970: " + timestamp);
      }
      
      timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(0), 
          timestamp.getZone());
      timestamp = timestamp.with(TemporalAdjusters.firstDayOfYear());
      break;
    default:
      throw new IllegalArgumentException("Unsupported time units: " + units);
    }
    
    TimeStamp next = this.getCopy();
    int incremented = 0;
    while (next.compare(Op.LTE, original)) {
      if (incremented > 0) {
        // save an op
        update(next);
      }
      next.add(increment);
      ++incremented;
    }
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("timestamp=")
        .append(timestamp)
        .append(", utc=")
        .append(Instant.from(timestamp))
        .append(", epoch=")
        .append(epoch())
        .append(", nanos=")
        .append(nanos())
        .append(", msEpoch=")
        .append(msEpoch())
        .toString();
  }
}
