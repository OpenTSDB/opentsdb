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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.opentsdb.core.TSDB;

/**
 * A factory used to generate {@link QueryFilter}s.
 * 
 * @since 3.0
 */
public interface QueryFilterFactory {
  
  /**
   * The descriptive and unique ID of the factory used during registration.
   * @return A non-null and non-empty unique ID of the factory.
   */
  public String getType();
  
  /**
   * Parses the given JSON or Yaml node into the proper filter.
   * @param tsdb The non-null TSDB instance in case we need other
   * factories from the registry.
   * @param mapper The non-null mapper.
   * @param node The non-null root node of this filter.
   * @return The parsed filter if successful.
   */
  public QueryFilter parse(final TSDB tsdb, 
                           final ObjectMapper mapper, 
                           final JsonNode node);
}
