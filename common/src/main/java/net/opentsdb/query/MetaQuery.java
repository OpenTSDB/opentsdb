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
package net.opentsdb.query;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.QueryFilterFactory;

/**
 * Represents parameters to search for metadata.
 *
 * @since 3.0
 */
@SuppressWarnings("serial")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MetaQuery {

  /**
   * The starting pointer for results for pagination
   */
  private String from;

  /**
   * The end pointer for results for pagination
   */
  private String to;

  /**
   * The namespace for the query
   */
  @JsonProperty("namespace")
  private String namespace;

  /**
   * Filters for the query. Can be a chained filter or a single filter
   */
  private QueryFilter filters;

  /**
   * The field by which user wants to aggregate by to return unique values
   */
  private Field aggregateBy = Field.ALL;

  /**
   * The tag for which a second level aggregation is applied. Lists tag values for a tag key
   */
  private String aggregationField;

  /**
   * Size of number unique tag values to return.
   */
  private String size;

  protected MetaQuery(final Builder builder) {

    from = Strings.isNullOrEmpty(builder.from) ? null : builder.from;

    to = Strings.isNullOrEmpty(builder.to) ? null : builder.to;

    namespace = Strings.isNullOrEmpty(builder.namespace) ? null : builder.namespace;

    aggregationField = Strings.isNullOrEmpty(builder.aggregationField) ? null : builder.aggregationField;

    if (builder.filters == null) {
      throw new IllegalArgumentException("Please set atleast one filter");
    } else {
      filters = builder.filters;
    }

    aggregateBy = builder.aggregateBy == null ? Field.ALL : builder.aggregateBy;

    size = Strings.isNullOrEmpty(builder.size) ? null : builder.size;

    if (builder.from == null) {
      throw new IllegalArgumentException("Please set from field");
    }

    if (builder.to == null) {
      throw new IllegalArgumentException("Please set to field");
    }

    if (builder.namespace == null) {
      throw new IllegalArgumentException("Please set a namespace");
    }

  }

  public String getNamespace() {
    return namespace;
  }


  public String getFrom() {
    return from;
  }


  public String getTo() {
    return to;
  }

  public QueryFilter getFilters() {
    return filters;
  }


  public Field getAggregateBy() {
    return aggregateBy;
  }

  public String getAggregationField() {
    return aggregationField;
  }


  public static MetaQuery.Builder newBuilder() {
    return new Builder();
  }


  /**
   * Builder through which the query is parsed and parameters are set
   */
  public static class Builder {
    private String from;
    private String to;
    private String namespace;
    private QueryFilter filters;
    private Field aggregateBy;
    private String aggregationField;
    private String size;

    public Builder setFrom(final String from) {
      this.from = from;
      return this;
    }

    public Builder setTo(final String to) {
      this.to = to;
      return this;
    }

    public Builder setNamespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder setFilters(final QueryFilter filters) {
      this.filters = filters;
      return this;
    }


    public Builder setAggregateBy(final Field aggregateBy) {
      this.aggregateBy = aggregateBy;
      return this;
    }

    public Builder setAggregationField(final String aggregationField) {
      this.aggregationField = aggregationField;
      return this;
    }

    public Builder setSize(final String size) {
      this.size = size;
      return this;
    }

    public MetaQuery build() {
      return new MetaQuery(this);
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
  public static Builder parse(TSDB tsdb, ObjectMapper mapper, JsonNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Cannot be empty");
    }
    final Builder builder = newBuilder();

    JsonNode n = node.get("from");
    builder.setFrom(n.asText());

    n = node.get("to");
    builder.setTo(n.asText());

    n = node.get("namespace");
    System.out.println(n);
    builder.setNamespace(n.asText());

    n = node.get("filter");
    JsonNode type = n.get("type");

    final QueryFilterFactory factory = tsdb.getRegistry()
            .getPlugin(QueryFilterFactory.class, type.asText());

    builder.setFilters((factory.parse(tsdb, mapper, n)));

    n = node.get("aggregateBy");
    if (n.asText().equalsIgnoreCase("ALL")) {
      builder.setAggregateBy(Field.ALL);
    } else if (n.asText().equalsIgnoreCase("Metrics")) {
      builder.setAggregateBy(Field.AM_NESTED);
    } else if (n.asText().equalsIgnoreCase("Tags_keys")) {
      builder.setAggregateBy(Field.TAGS_KEYS);
    } else if (n.asText().equalsIgnoreCase("Tags_Values")) {
      builder.setAggregateBy(Field.TAGS_VALUES);
      n = node.get("aggregateField");
      if (n != null) {
        builder.setAggregationField(n.asText());
      }
      n = node.get("size");
      builder.setSize(n.asText());
    } else {
      throw new IllegalArgumentException("Invalid aggregation");
    }
    return builder;

  }

  public enum Field {

    AM_NESTED("AM_nested"),
    TAGS_KEYS("tags.key"),
    TAGS_VALUES("tags.value"),
    ALL("ALL");

    private String field;

    Field(String field) {
      this.field = field;
    }

    @Override
    public String toString() {
      return field;
    }

  }
}




