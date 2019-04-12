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
package net.opentsdb.meta;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.QueryFilterFactory;
import net.opentsdb.meta.BatchMetaQuery.QueryType;

/**
 * Represents parameters to search for metadata.
 *
 * @since 3.0
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DefaultMetaQuery implements MetaQuery {
  
  /**
   * The namespace for the query
   */
  private String namespace;

  /**
   * Filters for the query. Can be a chained filter or a single filter
   */
  private QueryFilter filters;

  /**
   * Unique id of the query
   */
  private String id;

  protected DefaultMetaQuery(final Builder builder) {
    namespace = Strings.isNullOrEmpty(builder.namespace) ? null : builder.namespace;
    filters = builder.filter;
    id = builder.id;
    if (builder.namespace == null) {
      throw new IllegalArgumentException("Please set a namespace");
    }
  }

  public String namespace() {
    return namespace;
  }

  public QueryFilter filter() {
    return filters;
  }

  public String id() {
    return id;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder through which the query is parsed and parameters are set
   */
  public static class Builder extends MetaQuery.Builder {
    
    public MetaQuery build() {
      return new DefaultMetaQuery(this);
    }

  }

  /**
   * Parses the query and validates.
   *
   * @param tsdb   The TSDB instance
   * @param mapper Object mapper to use for parsing filters
   * @param node   The JSON node for the query
   * @return a Builder after parsing the query
   */
  public static Builder parse(final TSDB tsdb, 
                              final ObjectMapper mapper, 
                              final JsonNode node,
                              final QueryType query_type) {
    if (node == null) {
      throw new IllegalArgumentException("Cannot be empty");
    }
    final Builder builder = newBuilder();

  JsonNode n = node.get("namespace");
    if (n == null || n.isNull()) {
      throw new IllegalArgumentException(
          "The namespace field cannot be null or empty");
    }
    builder.setNamespace(n.asText());

    n = node.get("id");
    if (n == null || n.isNull()) {
      throw new IllegalArgumentException("ID cannot be null");
    }
    builder.setId(n.asText());

    if (query_type != QueryType.NAMESPACES) {
      n = node.get("filter");
      if (n == null || n.isNull()) {
        throw new IllegalArgumentException(
          "The fitler field cannot be null or empty");
      }
      JsonNode type = n.get("type");

      final QueryFilterFactory factory = tsdb.getRegistry()
        .getPlugin(QueryFilterFactory.class, type.asText());
      builder.setFilter((factory.parse(tsdb, mapper, n)));
    }
    return builder;

  }
}
