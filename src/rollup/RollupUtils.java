// This file is part of OpenTSDB.
// Copyright (C) 2010-2015  The OpenTSDB Authors.
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
package net.opentsdb.rollup;

import java.util.Calendar;

import org.hbase.async.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Const;

/**
 * Static util class for dealing with parsing and storing rolled up data points
 * @since 2.4
 */
public final class RollupUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RollupUtils.class);
  
  /** The rollup qualifier delimiter character */
  public static final String ROLLUP_QUAL_DELIM = ":";
  
  private RollupUtils() {
    // Do not instantiate me brah!
  }
  
  /**
   * Calculates the base time for a rollup interval, the time that can be
   * stored in the row key.
   * @param timestamp The data point timestamp to calculate from in seconds
   * or milliseconds
   * @param interval The configured interval object to use for calcaulting
   * the base time with a valid span of 'h', 'd', 'm' or 'y'
   * @return A base time as a unix epoch timestamp in seconds
   * @throws IllegalArgumentException if the timestamp is negative or the interval
   * has an unsupported span
   */
  public static int getRollupBasetime(final long timestamp, 
      final RollupInterval interval) {
    if (timestamp < 0) {
      throw new IllegalArgumentException("Not supporting negative "
          + "timestamps at this time: " + timestamp);
    }
    
    // avoid instantiating a calendar at all costs! If we are based on an hourly
    // span then use the old method of snapping to the hour
    if (interval.getUnits() == 'h') {
      int modulo = Const.MAX_TIMESPAN;
      if (interval.getUnitMultiplier() > 1) {
        modulo = interval.getUnitMultiplier() * 60 * 60;
      }
      if ((timestamp & Const.SECOND_MASK) != 0) {
        // drop the ms timestamp to seconds to calculate the base timestamp
        return (int) ((timestamp / 1000) - 
            ((timestamp / 1000) % modulo));
      } else {
        return (int) (timestamp - (timestamp % modulo));
      }
    } else {
      final long time_milliseconds = (timestamp & Const.SECOND_MASK) != 0 ?
          timestamp : timestamp * 1000;
      
      // gotta go the long way with the calendar to snap to an appropriate
      // daily, monthly or weekly boundary
      final Calendar calendar = Calendar.getInstance(Const.UTC_TZ);
      calendar.setTimeInMillis(time_milliseconds);
      
      // zero out the hour, minutes, seconds
      calendar.set(Calendar.HOUR_OF_DAY, 0);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);
      
      switch (interval.getUnits()) {
      case 'd':
        // all set via the zeros above
        break;
      case 'n':
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        break;
      case 'y':
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.MONTH, 0); // 0 for January
        break;
      default:
        throw new IllegalArgumentException("Unrecogznied span: " + interval);
      }
      
      return (int) (calendar.getTimeInMillis() / 1000);
    }
  }

  /**
   * Builds a rollup column qualifier, prepending the appender as a string
   * then the offeset on 2 bytes for the interval after a colon with the last
   * four bits reserved for the length and type flags. I.e.
   * {@code <agg>:<offset(flag)> 
   *   n  :  2 bytes }
   * @param timestamp The data point timestamp
   * @param flags The length and type (float || int) flags for the value
   * @param aggregator The aggregator used to generate the data
   * @param interval The RollupInterval object with data about the interval
   * @return An n byte array to use as the qualifier
   * @throws IllegalArgumentException if the aggregator is null or empty or the
   * timestamp is too far from the base time to fit within the interval.
   */
  public static byte[] buildRollupQualifier(final long timestamp,
      final short flags,
      final String aggregator,
      final RollupInterval interval) {
    return buildRollupQualifier(timestamp, 
        getRollupBasetime(timestamp, interval), flags, aggregator, interval);
  }
  
  /**
   * Builds a rollup column qualifier, prepending the appender as a string
   * then the offeset on 2 bytes for the interval after a colon with the last
   * four bits reserved for the length and type flags. I.e.
   * {@code <agg>:<offset(flag)>
   *   n  :  2 bytes }
   * @param timestamp The data point timestamp
   * @param basetime The base timestamp to calculate the offset from 
   * @param flags The length and type (float || int) flags for the value
   * @param aggregator The aggregator used to generate the data
   * @param interval The RollupInterval object with data about the interval
   * @return An n byte array to use as the qualifier
   * @throws IllegalArgumentException if the aggregator is null or empty or the
   * timestamp is too far from the base time to fit within the interval.
   */
  public static byte[] buildRollupQualifier(final long timestamp,
                                            final int basetime,
                                            final short flags,
                                            final String aggregator,
                                            final RollupInterval interval) {
    if (aggregator == null || aggregator.isEmpty()) {
      throw new IllegalArgumentException("Aggregator cannot be null or empty");
    }
    
    final byte[] agg = getRollupQualifierPrefix(aggregator);
    final byte[] qualifier = new byte[agg.length + 2];

    final int time_seconds = (int) ((timestamp & Const.SECOND_MASK) != 0 ?
            timestamp / 1000 : timestamp);

    // we shouldn't have a divide by 0 here as the rollup config validator makes
    // sure the interval is positive
    int offset = (time_seconds - basetime) / interval.getIntervalSeconds();
    if (offset >= interval.getIntervals()) {
      throw new IllegalArgumentException("Offset of " + offset + " was greater "
          + "than the configured intervals " + interval.getIntervals());
    }
   
    // shift the offset over 4 bits then apply the flag
    offset = offset << Const.FLAG_BITS;
    offset = offset | flags;
    final byte[] offset_array = Bytes.fromShort((short) offset);
    System.arraycopy(agg, 0, qualifier, 0, agg.length);
    System.arraycopy(offset_array, 0, qualifier, agg.length, 
        offset_array.length);

    return qualifier;
  }
  
  /**
   * Returns the absolute timestamp of a data point qualifier in milliseconds
   * @param qualifier The qualifier to parse
   * @param base_time The base time, in seconds, from the row key
   * @param interval The RollupInterval object with data about the interval
   * @param offset An offset within the byte array
   * @return The absolute timestamp in milliseconds
   */
  public static long getTimestampFromRollupQualifier(final byte[] qualifier, 
                  final long base_time, 
                  final RollupInterval interval,
                  final int offset) {
    return (base_time * 1000) + 
            getOffsetFromRollupQualifier(qualifier, offset, interval);
  }
  
  /**
   * Returns the absolute timestamp of a data point qualifier in milliseconds
   * @param qualifier The qualifier to parse
   * @param base_time The base time, in seconds, from the row key
   * @param interval  The RollupInterval object with data about the interval
   * @return The absolute timestamp in milliseconds
   */
  public static long getTimestampFromRollupQualifier(final int qualifier, 
      final long base_time, 
      final RollupInterval interval) {
    return (base_time * 1000) + getOffsetFromRollupQualifier(qualifier, interval);
  }

  /**
   * Returns the offset in milliseconds from the row base timestamp from a data
   * point qualifier at the given offset (for compacted columns)
   * @param qualifier The qualifier to parse
   * @param byte_offset An offset within the byte array
   * @param interval The RollupInterval object with data about the interval
   * @return The offset in milliseconds from the base time
   */
  public static long getOffsetFromRollupQualifier(final byte[] qualifier, 
                  final int byte_offset, 
                  final RollupInterval interval) {
    
    long offset = 0;
    
    if ((qualifier[byte_offset] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
      offset = ((Bytes.getUnsignedInt(qualifier, byte_offset) & 0x0FFFFFC0) 
        >>> Const.MS_FLAG_BITS)/1000;
    } else {
      offset = (Bytes.getUnsignedShort(qualifier, byte_offset) & 0xFFFF) 
        >>> Const.FLAG_BITS;
    }
    
    return offset * interval.getIntervalSeconds() * 1000;
  }
  
  /**
   * Returns the offset in milliseconds from the row base timestamp from a data
   * point qualifier at the given offset (for compacted columns)
   * @param qualifier The qualifier to parse
   * @param interval The RollupInterval object with data about the interval
   * @return The offset in milliseconds from the base time
   */
  public static long getOffsetFromRollupQualifier(final int qualifier, 
      final RollupInterval interval) {

    long offset = 0;
    if ((qualifier & Const.MS_FLAG) == Const.MS_FLAG) {
      LOG.warn("Unexpected rollup qualifier in milliseconds: " + qualifier 
          + " for interval " + interval);
      offset = (qualifier & 0x0FFFFFC0) >>> (Const.MS_FLAG_BITS) / 1000;
    } else {
      offset = (qualifier & 0xFFFF) >>> Const.FLAG_BITS;
    }
    return offset * interval.getIntervalSeconds() * 1000;
  }

  /**
   * Builds a rollup column qualifier prefix,prepending the appender as a string
   * along with a colon as delimiter
   * @param aggregator The aggregator used to generate the data
   * @return An n byte array to use as the qualifier prefix
   */
  public static byte[] getRollupQualifierPrefix(final String aggregator) {
    return (aggregator.toLowerCase() + ROLLUP_QUAL_DELIM)
            .getBytes(Const.ASCII_CHARSET);
  }
}
