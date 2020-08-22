// This file is part of OpenTSDB.
// Copyright (C) 2017-2020  The OpenTSDB Authors.
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
import java.util.List;

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
  
  /**
   * The collection of time series results, potentially in order (e.g. if TopN
   * is used). May be empty but will not be null.
   * @return A non-null list of zero or more time series.
   */
  public List<TimeSeries> timeSeries();
  
  /**
   * An optional error from downstream. If this is set, then 
   * {@link #timeSeries()} must be empty. If this is not null and not 
   * empty then the result had an error and upstream can take the 
   * appropriate action.
   * @return An optional error message. May be null.
   */
  public String error();
  
  /**
   * An optional exception from downstream. {@link #error()} takes 
   * precedence and should be set to either the message of the throwable
   * or a different string.
   * @return An optional exception. May be null.
   */
  public Throwable exception();
  
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
  
  /** @return The identifier of the data source that these results came from.
   * E.g. can be a metric source (QuerySourceConfig) or it could be a 
   * node that generates a new result set such as an expression. If 
   * data is just passing through (e.g. a downsample node) this should
   * be the source metric or node. */
  public QueryResultId dataSource();
  
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

  /**
   * @return if the source node supports parallel processing of the time series
   */
  public boolean processInParallel();
}
