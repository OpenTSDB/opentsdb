// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Strings;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.utils.DateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents parameters to search for metadata.
 *
 * @since 3.0
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DefaultBatchMetaQuery implements BatchMetaQuery {

  /**
   * The starting pointer for results for pagination
   */
  private int from;

  /**
   * The end pointer for results for pagination
   */
  private int to;

  /**
   * The namespace for the query
   */
  private String namespace;

  /**
   * Filters for the query. Can be a chained filter or a single filter
   */
  private QueryFilter filters;

  /**
   * The tag for which a second level aggregation is applied. Lists tag values for a tag key
   */
  private String aggregation_field;

  /**
   * Size of number unique tag values to return.
   */
  private int agg_size;
  private QueryType type;
  private String source;
  private Order order;
  private TimeStamp start;
  private TimeStamp end;
  private List<MetaQuery> meta_query;

  protected DefaultBatchMetaQuery(final Builder builder) {
    from = builder.from;
    to = builder.to;
    aggregation_field = Strings.isNullOrEmpty(builder.aggregationField) ?
            null : builder.aggregationField;
    agg_size = builder.agg_size;
    type = builder.type;
    source = builder.source;
    order = builder.order;
    meta_query = builder.meta_query;

    if (!Strings.isNullOrEmpty(builder.start)) {
      start = new MillisecondTimeStamp(
              DateTime.parseDateTimeString(builder.start, builder.time_zone));
    }

    if (!Strings.isNullOrEmpty(builder.end)) {
      end = new MillisecondTimeStamp(
              DateTime.parseDateTimeString(builder.end, builder.time_zone));
    }

    if (type == QueryType.NAMESPACES) {
      if (meta_query.size() > 1) {
        throw new IllegalArgumentException("Namespaces query cannot have more than one query");
      }
    }
  }

  public String namespace() {
    return namespace;
  }

  public int from() {
    return from;
  }

  public int to() {
    return to;
  }

  public QueryFilter filter() {
    return filters;
  }

  public String aggregationField() {
    return aggregation_field;
  }

  public int aggregationSize() {
    return agg_size;
  }

  public QueryType type() {
    return type;
  }

  public Order order() {
    return order;
  }

  public TimeStamp start() {
    return start;
  }

  public TimeStamp end() {
    return end;
  }

  public List<MetaQuery> metaQueries() {
    return meta_query;
  }

  @Override
  public String source() {
    return source;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder through which the query is parsed and parameters are set
   */
  public static class Builder extends BatchMetaQuery.Builder {

    public BatchMetaQuery build() {
      return new DefaultBatchMetaQuery(this);
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
                              final JsonNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Cannot be empty");
    }
    final Builder builder = newBuilder();

    JsonNode n = node.get("from");
    if (n == null || n.isNull()) {
      builder.setFrom(0);
    } else {
      builder.setFrom(n.asInt());
    }

    n = node.get("to");
    if (n == null || n.isNull()) {
      throw new IllegalArgumentException("The to field must be set.");
    }
    builder.setTo(n.asInt());


    n = node.get("type");
    if (n == null || n.isNull()) {
      throw new IllegalArgumentException("Type cannot be null or empty.");
    }
    builder.setType(QueryType.valueOf(n.asText()));

    n = node.get("source");
    if (n != null && !n.isNull() && !n.asText().isEmpty()) {
      builder.setSource(n.asText());
    }

    n = node.get("order");
    if (n != null && !n.isNull()) {
      builder.setOrder(Order.valueOf(n.asText()));
    }

    n = node.get("start");
    if (n != null && !n.isNull()) {
      builder.setStart(n.asText());
    }

    n = node.get("end");
    if (n != null && !n.isNull()) {
      builder.setEnd(n.asText());
    }

    n = node.get("timeZone");
    if (n != null && !n.isNull()) {
      builder.setTimeZone(n.asText());
    }

    n = node.get("aggregationField");
    if (n != null && !n.isNull()) {
      builder.setAggregationField(n.asText());
    }

    n = node.get("aggregationSize");
    if (n != null && !n.isNull()) {
      builder.setAggregationSize(n.asInt());
    }

    n = node.get("queries");
    if (n != null && !n.isNull()) {
      List<MetaQuery> meta_query = new ArrayList<>();
      MetaDataStorageSchema plugin =
          tsdb.getRegistry().getPlugin(MetaDataStorageSchema.class, builder.source);
      if (null == plugin) {
        throw new IllegalArgumentException("Plugin not found for type: " + builder.source);
      }
      for (int i = 0; i < n.size(); i++) {
        meta_query.add(plugin.parse(tsdb, mapper, n.get(i), builder.type));
      }
      builder.setMetaQuery(meta_query);
    }
    return builder;
  }
}
