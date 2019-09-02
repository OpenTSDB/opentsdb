// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.summarizer;

import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.summarizer.SummarizerPassThroughResult.SummarizerSummarizedResult;
import net.opentsdb.stats.Span;

import java.util.Map;

/**
 * A node that computes summaries across a time series, such as computing
 * the max, min, avg, etc.
 * 
 * @since 3.0
 */
public class Summarizer extends AbstractQueryNode {
  
  /** The config. */
  private final SummarizerConfig config;
  
  /** The map of aggregator string IDs to aggregators for this summary. */
  private final Map<String, NumericAggregator> aggregators;
  
  /**
   * Default ctor.
   * @param factory The non-null factory we spawned from.
   * @param context The non-null context.
   * @param config The non-null config.
   */
  public Summarizer(final QueryNodeFactory factory, 
                    final QueryPipelineContext context,
                    final QueryNodeConfig config) {
    super(factory, context);
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    this.config = (SummarizerConfig) config;
    aggregators = Maps.newHashMapWithExpectedSize(this.config.getSummaries().size());
  }
  
  @Override
  public Deferred<Void> initialize(final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass() + ".initialize()").start();
    } else {
      child = null;
    }
    for (final String summary : this.config.getSummaries()) {
      final NumericAggregatorFactory agg_factory = context.tsdb()
          .getRegistry().getPlugin(NumericAggregatorFactory.class, summary);
      if (agg_factory == null) {
        throw new IllegalArgumentException("No aggregator found for type: " 
            + summary);
      }
      final NumericAggregator agg = agg_factory.newAggregator(
          this.config.getInfectiousNan());
      aggregators.put(summary, agg);
    }
    upstream = context.upstream(this);
    downstream = context.downstream(this);
    downstream_sources = context.downstreamSources(this);
    if (child != null) {
      child.setSuccessTags().finish();
    }
    return INITIALIZED;
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public void onNext(final QueryResult next) {
    if (next instanceof SummarizerSummarizedResult) {
      sendUpstream(next);
      return;
    }
    
    if (config.passThrough()) {
      final SummarizerPassThroughResult results = 
          new SummarizerPassThroughResult(this, next);
      sendUpstream(results);
    } else {
      final SummarizerNonPassThroughResult results = 
          new SummarizerNonPassThroughResult(this, next);
      sendUpstream(results);
    }
  }
  
  /** @return Package private method to return the instantiated aggregators for
   * the summaries requested by the user. */
  Map<String, NumericAggregator> aggregators() {
    return aggregators;
  }
}
