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
package net.opentsdb.query.idconverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;

/**
 * Super simple factory to return ID converters.
 * 
 * @since 3.0
 */
public class ByteToStringIdConverterFactory extends BaseQueryNodeFactory {

  public static final String TYPE = "ByteToStringIdConverter";

  @Override
  public QueryNodeConfig parseConfig(final ObjectMapper mapper, 
                                     final TSDB tsdb,
                                     final JsonNode node) {
    try {
      return mapper.treeToValue(node, ByteToStringIdConverterConfig.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse Json",e);
    }
  }

  @Override
  public void setupGraph(final TimeSeriesQuery query, 
                         final QueryNodeConfig config,
                         final QueryPlanner planner) {
    // no-op
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context) {
    return new ByteToStringIdConverter(this, context, 
        (ByteToStringIdConverterConfig) ByteToStringIdConverterConfig.newBuilder()
          .setId(TYPE)
          .build());
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context,
                           final QueryNodeConfig config) {
    return new ByteToStringIdConverter(this, context, 
        (ByteToStringIdConverterConfig) config);
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }
  
}
