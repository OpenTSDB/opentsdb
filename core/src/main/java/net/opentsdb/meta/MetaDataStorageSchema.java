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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesByteId;
import net.opentsdb.meta.MetaQuery;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.stats.Span;

import java.util.Map;

/**
 * Handles querying a meta data store for time series identifiers given
 * the query. Not used for the actual time series data.
 * 
 * @since 3.0
 */
public interface MetaDataStorageSchema {

  /**
   * Executes the given query, resolving it against a meta-data set.
   * @param query A non-null query source to resolve.
   * @param span An optional tracing span.
   * @return A deferred resolving to a meta data storage result or
   * an exception if something went very wrong. It's better to return
   * a result with the exception set.
   */
  public Deferred<Map<NamespacedKey, MetaDataStorageResult>> runQuery(
      final BatchMetaQuery query,
      final Span span);

  /**
   * Executes the given query, resolving it against a meta-data set.
   * @param context A non-null context to fetch the query from.
   * @param config A non-null query source to resolve.
   * @param span An optional tracing span.
   * @return A deferred resolving to a meta data storage result or
   * an exception if something went very wrong. It's better to return
   * a result with the exception set.
   */
  public Deferred<MetaDataStorageResult> runQuery(
          final QueryPipelineContext context,
          final TimeSeriesDataSourceConfig config,
          final Span span);

  /**
   * Parse the json node and create the MetaQuery
   * @param tsdb
   * @param mapper
   * @param jsonNode
   * @param type
   * @return
   */
  MetaQuery parse(TSDB tsdb, ObjectMapper mapper, JsonNode jsonNode, BatchMetaQuery.QueryType type);
}