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

import java.time.temporal.ChronoUnit;
import java.util.Collection;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.rollup.RollupConfig;

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
  
  // TODO - I may need to reorg the time series by data source. That way
  // we can link to the schema.
  
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
   * The type of time series ID used to describe the time series in this
   * result set. 
   * <b>Invariant:</b> All series in the set must share the same type.
   * @return A non-null type token.
   */
  public TypeToken<? extends TimeSeriesId> idType();
  
  /** @return The non-null resolution of the underlying time series 
   * timestamps. */
  public ChronoUnit resolution();
  
  /** @return A rollup config associated with this result if applicable. 
   * May be null. */
  public RollupConfig rollupConfig();
  
  /**
   * Closes and releases resources used by this result set. Should be called
   * by the API consumer or {@link QueryPipelineContext} when the listeners are
   * finished to avoid balooning memory.
   */
  public void close();
  
}
