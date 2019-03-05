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
package net.opentsdb.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import net.opentsdb.core.TSDB;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents parameters to search for metadata.
 *
 * @since 3.0
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DefaultBatchEventQuery implements BatchEventQuery {

  private Long start_timestamp = 0L;

  private Long end_timestamp = 0L;

  private boolean is_summary;

  private String interval;

  private int from;

  private int to;

  private List<EventQuery> event_queries;

  protected DefaultBatchEventQuery(final Builder builder) {
    start_timestamp = builder.start_timestamp;
    end_timestamp = builder.end_timestamp;
    is_summary = builder.is_summary;
    interval = builder.interval;
    event_queries = builder.event_queries;
    to = builder.to;
    from = builder.from;



    if (start_timestamp == null) {
      throw new IllegalArgumentException("Please set a start timestamp");
    }

    if (end_timestamp == null) {
      throw new IllegalArgumentException("Please set a end timestamp");
    }

  }

  public long startTimestamp() {
    return start_timestamp;
  }

  public long endTimestamp() {
    return end_timestamp;
  }

  public List<EventQuery> eventQueries() {
    return event_queries;
  }

  public boolean isSummary() {
    return is_summary;
  }

  public String interval() { return interval; }

  public int from() { return from; }

  public int to() { return to; }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder through which the query is parsed and parameters are set
   */
  public static class Builder extends BatchEventQuery.Builder {

    public BatchEventQuery build() {
      return new DefaultBatchEventQuery(this);
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

    JsonNode n;
//    = node.get("namespace");
//    if (n == null || n.isNull()) {
//      throw new IllegalArgumentException(
//        "The namespace field cannot be null or empty");
//    }

    n = node.get("isSummary");
    builder.setIsSummary(n.asBoolean());

    if (builder.is_summary == true) {
      n = node.get("interval");
      builder.setInterval(n.asText());
    }

    if (builder.is_summary == false) {
      n = node.get("from");
      builder.setFrom(n.asInt());
      n = node.get("to");
      builder.setTo(n.asInt());
    }

    n = node.get("start");
    builder.setStartTimestamp(n.asLong());

    n = node.get("end");
    builder.setEndTimestamp(n.asLong());

    n = node.get("queries");

    if (n !=null && !n.isNull()) {
      List<EventQuery> event_query = new ArrayList<>();
      for (int i = 0; i < ((ArrayNode) n).size(); i++) {
        event_query.add(DefaultEventQuery.parse(tsdb, mapper, n.get(i))
          .build());
      }
      builder.setEventQuery(event_query);
    }

    return builder;

  }
}
