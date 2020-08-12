// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query;

import com.google.common.base.Strings;

/**
 * A default implementation of the query result ID.
 * 
 * @since 3.0
 */
public class DefaultQueryResultId implements QueryResultId {
  /** The node ID, part 1 of the colon delimited ID string. */
  private final String node_id;
  
  /** The data source for the result, part 2 of the colon delimited ID String. */
  private final String data_source;
  
  /**
   * Default ctor.
   * @param node_id The non-null and non-empty node ID.
   * @param data_source The non-null and non-empty data source.
   */
  public DefaultQueryResultId(final String node_id, final String data_source) {
    if (Strings.isNullOrEmpty(node_id)) {
      throw new IllegalStateException("Node ID cannot be null or empty.");
    }
    if (Strings.isNullOrEmpty(data_source)) {
      throw new IllegalStateException("Data source cannot be null or empty.");
    }
    this.node_id = node_id;
    this.data_source = data_source;
  }
  
  @Override
  public String nodeID() {
    return node_id;
  }

  @Override
  public String dataSource() {
    return data_source;
  }

  @Override
  public String toString() {
    return node_id + ":" + data_source;
  }
  
  @Override
  public boolean equals(final Object obj) {
    if (obj == null || !(obj instanceof QueryResultId)) {
      return false;
    }
    
    final QueryResultId other = (QueryResultId) obj;
    if (Strings.isNullOrEmpty(other.nodeID()) || 
        Strings.isNullOrEmpty(other.dataSource())) {
      throw new IllegalStateException("ID " + obj 
          + " has a null or empty node or data source ID.");
    }
    
    return other.nodeID().equals(node_id) &&
           other.dataSource().equals(data_source);
  }
  
  @Override
  public int hashCode() {
    // TODO - verify this is ok for ints. It should be as we'd have low
    // cardinality within a given query. If we ever use this for multi-query
    // then we need to revisit it.
    return 2251 * node_id.hashCode() ^ 37 * data_source.hashCode();
  }
  
}
