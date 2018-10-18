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
package net.opentsdb.query.processor.summarizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;

/**
 * A node that computes summaries across a time series, such as computing
 * the max, min, avg, etc.
 * 
 * @since 3.0
 */
public class Summarizer extends AbstractQueryNode {
  private static final Logger LOG = LoggerFactory.getLogger(Summarizer.class);
  
  /** The config. */
  private final SummarizerConfig config;
  
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
    final SummarizerResult results = new SummarizerResult(this, next);
    for (final QueryNode us : upstream) {
      try {
        us.onNext(results);
      } catch (Exception e) {
        LOG.error("Failed to call upstream onNext on Node: " + us, e);
        results.close();
      }
    }
  }
  
}
