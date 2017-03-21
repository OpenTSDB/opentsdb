// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data;

/**
 * Simple implementation of a timestamp that is stored as a {@link Long} in
 * millisecond Unix Epoch.
 * 
 * @since 3.0
 */
public class MillisecondTimeStamp  implements TimeStamp {
  /** The timestamp. */
  private long timestamp;
  
  /**
   * Ctor, initializes the timestamp.
   * @param timestamp A timestamp in milliseconds.
   */
  public MillisecondTimeStamp(final long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public long msEpoch() {
    return timestamp;
  }

  @Override
  public long epoch() {
    return timestamp / 1000;
  }

  @Override
  public TimeStamp getCopy() {
    return new MillisecondTimeStamp(timestamp);
  }

  @Override
  public void updateMsEpoch(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public void updateEpoch(long timestamp) {
    this.timestamp = timestamp * 1000;
  }
  
  @Override
  public void update(final TimeStamp timestamp) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    if (timestamp instanceof MillisecondTimeStamp) {
      this.timestamp = ((MillisecondTimeStamp) timestamp).timestamp;
    } else {
      this.timestamp = timestamp.msEpoch();
    }
  }

  @Override
  public boolean compare(final TimeStampComparator comparator, 
      final TimeStamp compareTo) {
    if (compareTo == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    if (comparator == null) {
      throw new IllegalArgumentException("Comparator cannot be null.");
    }
    switch (comparator) {
    case LT:
      return timestamp < compareTo.msEpoch();
    case LTE:
      return timestamp <= compareTo.msEpoch();
    case GT:
      return timestamp > compareTo.msEpoch();
    case GTE:
      return timestamp >= compareTo.msEpoch();
    case EQ:
      return timestamp == compareTo.msEpoch();
    case NE:
      return timestamp != compareTo.msEpoch();
    default:
      throw new UnsupportedOperationException("Unknown comparator: " + comparator);
    }
  }

  @Override
  public void setMax() {
    timestamp = Long.MAX_VALUE;
  }

  @Override
  public String toString() {
    return Long.toString(timestamp);
  }
}
