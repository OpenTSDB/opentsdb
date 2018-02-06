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
package net.opentsdb.query.plan;

import java.util.List;

import net.opentsdb.query.pojo.TimeSeriesQuery;

/**
 * A planner that slices the given {@link TimeSeriesQuery} into smaller chunks
 * by time so that the chunks can be cached and/or streamed.
 *
 * @param <T> The type of data returned by this query.
 * 
 * @since 3.0
 */
public abstract class TimeSlicedQueryPlanner<T> extends QueryPlanner<T> {

  /**
   * Default ctor.
   * @param query A non-null query to use for plan generation.
   */
  public TimeSlicedQueryPlanner(final TimeSeriesQuery query) {
    super(query);
  }

  /**
   * Merges a list of time ordered results into a single result set, likely
   * using SlicedTimeSeriesIterators. While the results list may not
   * be null, the individual entries may be null and they will be skipped
   * during merge. If the list is empty, the result will be an empty object (but
   * never null).
   * @param results A non-null list of possibly nulled or empty results. 
   * @return An instantiated object that may be empty.
   * @throws IllegalArgumentException if the results were null.
   */
  public abstract T mergeSlicedResults(final List<T> results);
  
  /**
   * Slices the given result up by time according to the query plan. If the
   * {@code start_index} and {@code  end_index} are the same, the resulting list 
   * will only have one entry. Otherwise the list will have the proper number
   * of sliced results based on {@link #getTimeRanges()}.
   *   
   * @param result_copy A non-null result. <b>NOTE:</b> This should be a copy 
   * of the original as the data will be iterated over within the implementation.
   * @param start_index The start index within the {@link #getTimeRanges()} for
   * this result.
   * @param end_index The end index within the {@link #getTimeRanges()} for this
   * result. May be the same as the {@code start_index}.
   * @return A non-null list of results. The results may be empty though.
   * @throws IllegalArgumentException if the result or query were null or if the
   * indices were out of range.
   */
  public abstract List<T> sliceResult(final T result_copy,
                                      final int start_index, 
                                      final int end_index);

}
