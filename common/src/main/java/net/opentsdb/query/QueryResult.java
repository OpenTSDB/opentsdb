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