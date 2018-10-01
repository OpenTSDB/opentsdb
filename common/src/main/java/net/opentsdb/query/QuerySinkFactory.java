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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;

/**
 * A factory used to generate sink instances.
 * 
 * @since 3.0
 */
public interface QuerySinkFactory extends TSDBPlugin {

  /** @return The ID of this factory. */
  public String id();
  
  /**
   * Returns a new sink instance.
   * @param context A non-null query context.
   * @param config An optional sink configuration.
   * @return A non-null sink instance.
   */
  public QuerySink newSink(final QueryContext context, 
                           final QuerySinkConfig config);

  /**
   * Parse the given JSON or YAML into the proper serdes config.
   * @param mapper A non-null mapper to use for parsing.
   * @param tsdb The non-null TSD to pull factories from.
   * @param node The non-null node to parse.
   * @return An instantiated node config if successful.
   */
  public QuerySinkConfig parseConfig(final ObjectMapper mapper,
                                     final TSDB tsdb, 
                                     final JsonNode node);
}
