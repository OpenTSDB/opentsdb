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
package net.opentsdb.query.processor.groupby;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.groupby.GroupByConfig;

/**
 * Performs the time series grouping aggregation by sorting time series according
 * to tag keys and merging the results into single time series using an 
 * aggregation function.
 * <p>
 * For each result returned to {@link #onNext(QueryResult)}, a new set of time 
 * series is generated containing a collection of source time series from the 
 * incoming result set. The actual arithmetic is performed when upstream sources
 * fetch an iterator and being the iteration.
 * 
 * @since 3.0
 */
public class GroupBy extends AbstractQueryNode {
  private static final Logger LOG = LoggerFactory.getLogger(GroupBy.class);
  
  /** The config for this group by node. */
  private final GroupByConfig config;
    
  /**
   * Default ctor.
   * @param factory The non-null factory for generating iterators.
   * @param context The non-null pipeline context we belong to.
   * @param config A non-null group by config to configure the iterators with.
   */
  public GroupBy(final QueryNodeFactory factory, 
                 final QueryPipelineContext context, 
                 final GroupByConfig config) {
    super(factory, context);
    if (config == null) {
      throw new IllegalArgumentException("Group By config cannot be null.");
    }
    this.config = config;
  }
    
  @Override
  public void close() {
    // No-op
  }

  @Override
  public void onComplete(final QueryNode downstream, 
                         final long final_sequence,
                         final long total_sequences) {
    for (final QueryNode us : upstream) {
      try {
        us.onComplete(this, final_sequence, total_sequences);
      } catch (Exception e) {
        LOG.error("Failed to call upstream onComplete on Node: " + us, e);
      }
    }
  }

  @Override
  public void onNext(final QueryResult next) {
    for (final QueryNode us : upstream) {
      try {
        us.onNext(new GroupByResult(this, next));
      } catch (Exception e) {
        LOG.error("Failed to call upstream onNext on Node: " + us, e);
      }
    }
  }

  @Override
  public void onError(final Throwable t) {
    for (final QueryNode us : upstream) {
      try {
        us.onError(t);
      } catch (Exception e) {
        LOG.error("Failed to call upstream onError on Node: " + us, e);
      }
    }
  }

  @Override
  public String id() {
    return config.getId();
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }
  
}