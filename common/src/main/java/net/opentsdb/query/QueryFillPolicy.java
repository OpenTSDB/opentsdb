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
package net.opentsdb.query;

import net.opentsdb.data.TimeSeriesDataType;

/**
 * An interface used to fill missing or unaligned values when aggregating
 * time series.
 *
 * @param <T> The data type filled with this policy.
 * 
 * @since 3.0
 */
public interface QueryFillPolicy<T extends TimeSeriesDataType> {
  
  /**
   * Describes how fill policies interact with real values when available. For
   * instance a fill that returns {@code null} values by default can choose to
   * return the previous or next values when available.
   */
  public enum FillWithRealPolicy {
    
    /** Only returns the value specified by the fill policy, ignoring any real
     * values. */
    NONE,
    
    /** Returns the previous real value when available, otherwise returns the 
     * fill's default value. */
    PREVIOUS_ONLY,
    
    /** Returns the next real value when available, otherwise returns the fill's
     * default value. */
    NEXT_ONLY,
    
    /** Returns the previous value when available or the next if no previous is
     * present. Otherwise falls back to the fill's default. */
    PREFER_PREVIOUS,
    
    /** Returns the next value when available or the previous if no next is
     * present. Otherwise falls back to the fill's default. */
    PREFER_NEXT
  }
  
  /** @return The fill value of the policy. May be null. */
  public T fill();
  
  /** @return The real-value fill policy used by the implementation. */
  public FillWithRealPolicy realPolicy();
}
