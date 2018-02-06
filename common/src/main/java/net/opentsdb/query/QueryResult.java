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

import java.util.Collection;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSpecification;

/**
 * Represents a query result, returning zero or more time series from a data
 * source. All time series represent the same time range. For downsampled
 * results, the time specification will be filled in.
 * 
 * @since 3.0
 */
public interface QueryResult {

  /**
   * An optional time specification for calculating timestamps for downsampled
   * results.
   * @return Null if the results were not downsampled or a non-null specification
   * if timestamps should be calculated.
   */
  public TimeSpecification timeSpecification();
  
  /**
   * The collection of time series results. May be empty but will not be null.
   * @return A non-null collection of zero or more time series.
   */
  public Collection<TimeSeries> timeSeries();
  
  /**
   * @return The zero based sequence ID of the result when operating in a 
   * streaming mode.
   */
  public long sequenceId();
  
  /**
   * The node that generated this query result.
   * @return A non-null query node.
   */
  public QueryNode source();

  /**
   * Closes and releases resources used by this result set. Should be called
   * by the API consumer or {@link QueryPipelineContext} when the listeners are
   * finished to avoid balooning memory.
   */
  public void close();
  
}
