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
package net.opentsdb.meta.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.BatchMetaQuery;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.stats.Span;

/**
 * The basic interface for interacting with an ElasticSearch cluster.
 *
 * @since 3.0
 */
public interface MetaClient<Q extends MetaQueryMarker, R extends MetaResponse> {

  /**
   * Executes the given query against a single or multiple clusters and returns the results in a
   * list, one per cluster.
   *
   * @param query The query to execute.
   * @param context The non-null query pipeline context.
   * @param span An optional tracing span.
   * @return A deferred resolving to a list of search response objects or an exception if the query
   *     couldn't execute.
   */
  Deferred<R> runQuery(final Q query, final QueryPipelineContext context, final Span span);

  Q buildQuery(BatchMetaQuery batchMetaQuery);

  Q buildMultiGetQuery(BatchMetaQuery batchMetaQuery);

  net.opentsdb.meta.MetaQuery parse(TSDB tsdb, ObjectMapper mapper, JsonNode jsonNode, BatchMetaQuery.QueryType type);
}
