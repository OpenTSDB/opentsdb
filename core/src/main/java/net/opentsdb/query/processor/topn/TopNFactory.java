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

import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.processor.BaseQueryNodeFactory;

/**
 * The factory for instantiating TopN processor nodes.
 * 
 * @since 3.0
 */
public class TopNFactory extends BaseQueryNodeFactory {

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
    super("topn");
  }
  
  /**
   * Named factory ctor.
   * @param id A non-null name.
   */
  public TopNFactory(final String id) {
    super(id);
  }
  
  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id) {
    return new TopN(this, context, id, DEFAULT);
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id,
                           final QueryNodeConfig config) {
    return new TopN(this, context, id, (TopNConfig) config);
  }

  @Override
  public Class<? extends QueryNodeConfig> nodeConfigClass() {
    return TopNConfig.class;
  }
  
}
