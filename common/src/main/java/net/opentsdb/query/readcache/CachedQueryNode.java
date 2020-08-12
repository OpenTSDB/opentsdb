// This file is part of OpenTSDB.
// Copyright (C) 2019-2020  The OpenTSDB Authors.
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
package net.opentsdb.query.readcache;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.hash.HashCode;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.stats.Span;

/**
 * A super simple node that implements the ID so that it can be passed in a 
 * QueryResult and it's ID fetched by upstream operators.
 * 
 * @since 3.0
 */
public class CachedQueryNode implements QueryNode, QueryNodeConfig {

  /** The ID of this node. */
  protected final String id;
  
  protected final QueryNode original_node;
  
  public CachedQueryNode(final String id, 
                         final QueryNode node) {
    this.id = id;
    this.original_node = node;
  }
  
  @Override
  public int compareTo(Object o) {
    return 0;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getType() {
    return null;
  }

  @Override
  public List getSources() {
    return Collections.emptyList();
  }

  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean pushDown() {
    return false;
  }

  @Override
  public boolean joins() {
    return false;
  }

  @Override
  public boolean readCacheable() {
    return false;
  }
  
  @Override
  public Map getOverrides() {
    return null;
  }

  @Override
  public String getString(Configuration config, String key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getInt(Configuration config, String key) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getLong(Configuration config, String key) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean getBoolean(Configuration config, String key) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public double getDouble(Configuration config, String key) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean hasKey(String key) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Builder toBuilder() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<QueryResultId> resultIds() {
    // TODO - do we need to populate this?
    return Collections.emptyList();
  }
  
  @Override
  public boolean markedCacheable() {
    return false;
  }
  
  @Override
  public void markCacheable(final boolean cacheable) {
    // no-op
  }
  
  @Override
  public QueryPipelineContext pipelineContext() {
    return original_node.pipelineContext();
  }

  @Override
  public Deferred initialize(Span span) {
    return Deferred.fromResult(null);
  }

  @Override
  public QueryNodeConfig config() {
    return this;
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public void onComplete(QueryNode downstream, long final_sequence,
      long total_sequences) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void onNext(QueryResult next) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void onNext(PartialTimeSeries next) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void onError(Throwable t) {
    throw new UnsupportedOperationException();
  }

  public QueryNode originalNode() {
    return original_node;
  }
  
}
