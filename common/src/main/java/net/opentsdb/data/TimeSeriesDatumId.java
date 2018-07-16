// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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

import com.google.common.reflect.TypeToken;

/**
 * An ID representing a single dataum or value. This is opposed to the
 * {@link TimeSeriesId} which is used at query time and may include 
 * multiple time series wrapped up into a new ID. Hence this ID does not
 * have any aggregated or disjoint tag keys.
 * 
 * @since 3.0
 */
public interface TimeSeriesDatumId extends Comparable<TimeSeriesDatumId> {

  /**
   * The type of series dealt with. Either a {@link TimeSeriesByteId} or
   * {@link TimeSeriesStringId}
   * @return A non-null type token.
   */
  public TypeToken<? extends TimeSeriesId> type();
  
  @Override
  public boolean equals(final Object other);
  
  @Override
  public int hashCode();
  
  /**
   * The hash code as a Long value for reducing hash collisions.
   * @return A hash code as a 64 bit signed integer.
   */
  public long buildHashCode();

}
