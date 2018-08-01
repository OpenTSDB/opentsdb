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
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.filter.ChainFilter.Builder;
import net.opentsdb.query.filter.ChainFilter.FilterOp;

/**
 * A factory for the {@link ChainFilter} filter type.
 * 
 * @since 3.0
 */
public class ChainFilterFactory implements QueryFilterFactory {

  @Override
  public String getType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryFilter parse(final TSDB tsdb, 
                           final ObjectMapper mapper, 
                           final JsonNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Chain root canot be null.");
    }
    
    final Builder builder = ChainFilter.newBuilder();
    final JsonNode op = node.get("op");
    if (op != null) {
      try {
        builder.setOp(mapper.treeToValue(op, FilterOp.class));
      } catch (JsonProcessingException e) {
        throw new QueryExecutionException("Failed to parse chain filter: " 
            + op, 0, e);
      }
    }
    
    final JsonNode filters = node.get("filters");
    if (filters == null) {
      throw new IllegalArgumentException("Filters field cannot be null.");
    }
    for (final JsonNode filter : filters) {
      final JsonNode type_node = filter.get("type");
      if (type_node == null) {
        throw new IllegalArgumentException("Filter must include a type.");
      }
      final String type = type_node.asText();
      if (Strings.isNullOrEmpty(type)) {
        throw new IllegalArgumentException("Filter type cannot be null "
            + "or empty.");
      }
      
      final QueryFilterFactory factory = tsdb.getRegistry()
          .getPlugin(QueryFilterFactory.class, type);
      if (factory == null) {
        throw new IllegalArgumentException("No filter factory found "
            + "for type: " + type);
      }
      builder.addFilter(factory.parse(tsdb, mapper, filter));
    }
    return builder.build();
  }

  @Override
  public String id() {
    return "Chain";
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
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
