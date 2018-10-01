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

import java.util.List;

import com.google.common.hash.HashCode;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.filter.NamedFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.serdes.SerdesOptions;

/**
 * The base interface for OpenTSDB queries.
 * TODO - this'll need a lot of work.
 * 
 * @since 3.0
 */
public interface TimeSeriesQuery extends Comparable<TimeSeriesQuery> {

  /** @return The overall start timestamp for the query. This is used to
   * filter the output. */
  public String getStart();
  
  /** @return The overall end timestamp for the query. Used to filter the
   * output. When null, the current system time is assumed. */
  public String getEnd();
  
  /** @return An optional timezone associated with the start and end
   * times. */
  public String getTimezone();
  
  /** @return The query execution mode. */
  public QueryMode getMode();
  
  /** @return An optional list of named filters. May be empty but may
   * not be null. */
  public List<NamedFilter> getFilters();

  /**
   * Returns the filter associated with the given ID. 
   * @param filter_id A non-null and non-empty filter ID.
   * @return The given filter if found, null if not.
   */
  public QueryFilter getFilter(final String filter_id);
  
  /** @return The non-null execution graph associated with this query.
   * Note that this is the user provided graph, not the graph actually
   * constructed. */
  public ExecutionGraph getExecutionGraph();
  
  /** @return The parsed start time of the query. */
  public TimeStamp startTime();
  
  /** @return The parsed end time of the query. */
  public TimeStamp endTime();
  
  /** @return A non-null list of serdes configs. */
  public List<SerdesOptions> getSerdesConfigs();
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode();
  
}
