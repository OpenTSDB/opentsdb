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
    final GroupByResult result = new GroupByResult(this, next);
    for (final QueryNode us : upstream) {
      try {
        us.onNext(result);
      } catch (Exception e) {
        LOG.error("Failed to call upstream onNext on Node: " + us, e);
        result.close();
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
  
  /** @return The number of upstream consumers. */
  protected int upstreams() {
    return upstream.size();
  }
}
