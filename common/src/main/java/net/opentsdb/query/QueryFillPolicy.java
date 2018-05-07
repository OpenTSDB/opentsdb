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
  
  /** @return The configuration class that this fill policy is linked to. */
  public QueryInterpolatorConfig config();
}
