// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.merge;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.stats.Span;

public class Merger extends AbstractQueryNode {

  /** The source config. */
  private final MergerConfig config;
  
  /** The result to populate and return. */
  private Map<String, Boolean> results;
  
  /** The result we'll send upstream. */
  private MergerResult result;
  
  /**
   * Default ctor.
   * @param factory The factory we came from.
   * @param context The non-null context.
   * @param id The ID of this node.
   * @param config The non-null config.
   */
  public Merger(final QueryNodeFactory factory, 
                final QueryPipelineContext context,
                final String id, 
                final MergerConfig config) {
    super(factory, context, id);
    if (config == null) {
      throw new IllegalArgumentException("Merger config cannot be null.");
    }
    this.config = (MergerConfig) config;
  }

  @Override
  public Deferred<Void> initialize(final Span span) {
    super.initialize(span);
    final Collection<String> expected = 
        context.downstreamSourcesIds(this);
    results = Maps.newHashMapWithExpectedSize(expected.size());
    for (final String id : expected) {
      results.put(id, false);
    }
    return INITIALIZED;
  }
  
  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {
    // No-op
  }
  
  @Override
  public void onNext(final QueryResult next) {
    synchronized (results) {
      if (next.dataSource().equals(next.source().config().getId())) {
        Boolean extant = results.get(next.dataSource());
        if (extant == null) {
          return;
        }
        if (extant) {
          throw new IllegalStateException("Already got a result for: " + next.dataSource());
        }
        results.put(next.dataSource(), true);
      } else {
        final String id = next.source().config().getId() + ":" + next.dataSource();
        Boolean extant = results.get(id);
        if (extant) {
          throw new IllegalStateException("Already got a result for: " + id);
        }
        results.put(id, true);
      }
      
      if (result == null) {
        result = new MergerResult(this, next);
      } else {
        result.add(next);
      }
      
      for (final Boolean gotit : results.values()) {
        if (!gotit) {
          return;
        }
      }
      
      // got em all!
      result.join();
      sendUpstream(result);
    }
    
  }
  
  /** @return The number of upstream consumers. */
  protected int upstreams() {
    return upstream.size();
  }
  
}
