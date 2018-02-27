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
package net.opentsdb.storage;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.storage.schemas.tsdb1x.Schema;

/**
 * A query node implementation for the V1 schema from OpenTSDB.
 * 
 * @since 3.0
 */
public class Tsdb1xQueryNode extends AbstractQueryNode implements SourceNode {

  /** The query source config. */
  private final QuerySourceConfig config;
  
  /**
   * Default ctor.
   * @param factory The Tsdb1xHBaseDataStore that instantiated this node.
   * @param context A non-null query pipeline context.
   * @param config A non-null config.
   */
  public Tsdb1xQueryNode(final QueryNodeFactory factory,
                         final QueryPipelineContext context,
                         final QuerySourceConfig config) {
    super(factory, context);
    if (config == null) {
      throw new IllegalArgumentException("Configuration cannot be null.");
    }
    this.config = config;
    // TODO Auto-generated constructor stub
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public String id() {
    return "Tsdb1xAsyncHBaseQueryNode";
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onComplete(QueryNode downstream, long final_sequence,
      long total_sequences) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onNext(QueryResult next) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onError(Throwable t) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public TimeStamp sequenceEnd() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Schema schema() {
    return ((Tsdb1xHBaseDataStore) factory).schema();
  }

}
