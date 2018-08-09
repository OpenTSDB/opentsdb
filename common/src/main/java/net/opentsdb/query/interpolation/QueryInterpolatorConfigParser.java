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
package net.opentsdb.query.interpolation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.opentsdb.core.TSDB;

/**
 * Parses text into an interpolator config.
 * 
 * @since 3.0
 */
public interface QueryInterpolatorConfigParser {

  /**
   * Parses the node.
   * @param mapper A non-null mapper.
   * @param tsdb The non-null TSDB to fetch the registry from.
   * @param node The root config node.
   * @return A parsed interpolator config on success or an exception
   * if something went wrong.
   */
  public QueryInterpolatorConfig parse(final ObjectMapper mapper,
                                       final TSDB tsdb, 
                                       final JsonNode node);
  
}
