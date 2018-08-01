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
package net.opentsdb.query.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;

public class UTFilterFactory implements QueryFilterFactory {

  static class UTQueryFilter implements QueryFilter {
    public String type;
    public String tag;
    public String filter;
    
    public UTQueryFilter() { }
    
    public UTQueryFilter(final String tag, final String filter) {
      this.tag = tag;
      this.filter = filter;
    }
  }

  @Override
  public String getType() {
    return "UTQueryFilter";
  }

  @Override
  public QueryFilter parse(TSDB tsdb, ObjectMapper mapper, JsonNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    try {
      return (QueryFilter) mapper.treeToValue(node, UTQueryFilter.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse UTQueryFilter", e);
    }
  }

  @Override
  public String id() {
    return "UTFilter";
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }
  
  
}
