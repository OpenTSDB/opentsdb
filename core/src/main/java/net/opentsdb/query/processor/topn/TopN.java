// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.topn;

import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;

/**
 * A processor that evaluates the time series in a result set using an
 * aggregation function over their results, then sorts the results based
 * on the numeric aggregated value and returns the top or bottom N 
 * time series, sorted.
 * 
 * @since 3.0
 */
public class TopN extends AbstractQueryNode {

  /** The non-null config. */
  private final TopNConfig config;
  
  public TopN(final QueryNodeFactory factory, 
              final QueryPipelineContext context,
              final String id,
              final TopNConfig config) {
    super(factory, context, id);
    this.config = config;
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
  public void onComplete(QueryNode downstream, long final_sequence,
      long total_sequences) {
    completeUpstream(final_sequence, total_sequences);
  }

  @Override
  public void onNext(final QueryResult next) {
    if (next instanceof TopNResult) {
      sendUpstream(next);
      return;
    }
    
    // TODO - separate thread if necessary.
    final TopNResult result = new TopNResult(this, next);
    result.run();
  }

  @Override
  public void onError(final Throwable t) {
    sendUpstream(t);
  }

}
