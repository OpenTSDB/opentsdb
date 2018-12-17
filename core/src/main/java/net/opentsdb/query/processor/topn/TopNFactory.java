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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;

/**
 * The factory for instantiating TopN processor nodes.
 * 
 * @since 3.0
 */
public class TopNFactory extends BaseQueryNodeFactory {

  public static final String TYPE = "TopN";
  
  public static TopNConfig DEFAULT;
  static {
    DEFAULT = (TopNConfig) TopNConfig.newBuilder()
        .setCount(10)
        .setTop(true)
        .setId("TopN")
        .build();
  }
  
  /**
   * Plugin ctor.
   */
  public TopNFactory() {
    super();
  }
  
  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    return Deferred.fromResult(null);
  }
  
  @Override
  public QueryNode newNode(final QueryPipelineContext context) {
    return new TopN(this, context, DEFAULT);
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final QueryNodeConfig config) {
    return new TopN(this, context, (TopNConfig) config);
  }
  
  @Override
  public QueryNodeConfig parseConfig(final ObjectMapper mapper, 
                                     final TSDB tsdb,
                                     final JsonNode node) {
    try {
      return mapper.treeToValue(node, TopNConfig.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to parse config", e);
    }
  }
  
  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final QueryNodeConfig config, 
                         final QueryPlanner plan) {
    // TODO Auto-generated method stub
  }
  
}
