// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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

/**
 * The factory used to generate a {@link QueryNode} for a new query execution.
 * Implementations can be single or multi-node.
 * 
 * @since 3.0
 */
public interface QueryNodeFactory {
  
  /**
   * The descriptive ID of the factory used when parsing queries.
   * @return A non-null unique ID of the factory.
   */
  public String id();
  
  /**
   * Parse the given JSON or YAML into the proper node config.
   * @param mapper A non-null mapper to use for parsing.
   * @param tsdb The non-null TSD to pull factories from.
   * @param node The non-null node to parse.
   * @return An instantiated node config if successful.
   */
  public QueryNodeConfig parseConfig(final ObjectMapper mapper,
                                     final TSDB tsdb, 
                                     final JsonNode node);
}
