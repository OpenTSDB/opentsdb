//This file is part of OpenTSDB.
//Copyright (C) 2019  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 2.1 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.
package net.opentsdb.data;

import net.opentsdb.query.QueryNode;

/**
 * A container and status object that represents a partial set of time series
 * returned from a source or node. Every {@link PartialTimeSeries} is a child
 * of this set and as they are streamed up through the pipeline nodes can
 * reference this set by calling {@link PartialTimeSeries#set()}.
 * <p>
 * At the start of streaming, {@link #totalSets()} is set to a value of 1 or
 * more and must be the same for all sets for the query.   
 * <p>
 * Note that while data is moving through the pipeline, {@link #timeSeriesCount()}
 * may change. However at the end when the set is set to the complete
 * method, the count must be the final count of series emitted for this set.
 * 
 * @since 3.0
 */
public interface PartialTimeSeriesSet extends AutoCloseable {
  
  /** @return The total number of sets for this query and data source. It cannot
   * change between sets for the same query. */
  public int totalSets();
 
  /** @return Whether or not the set has found all of the time series and
   * {@link #timeSeriesCount()} is at the final value. */
  public boolean complete();
  
  /** @return The node this set came from. */
  public QueryNode node();
  
  /** @return The name of the data source this set originated from. */
  public String dataSource();
 
  /** @return The non-null start time of this set. */
  public TimeStamp start();
 
  /** @return The non-null end time of this set. */
  public TimeStamp end();
  
  /** @return The total number of series emitted for this set. */
  public int timeSeriesCount();
 
  /** @return An optional time spec. */
  public TimeSpecification timeSpecification();
  
}
