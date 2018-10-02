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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;

/**
 * Parses numeric and piped ranges similar to 
 * https://github.com/yahoo/range.
 * 
 * E.g. "web{1-4}" expands to "web1", "web2", "web3", "web4".
 * "web{1-2}.{dc1|cd2}.com" expands to "web1.dc1.com", "web2.dc1.com",
 * "web1.cd2.com" and "web2.cd2.com".
 * 
 * @since 3.0
 */
public class TagValueRangeFilterFactory implements QueryFilterFactory {

  static final String TYPE = "TagValueRange";
  
  @Override
  public String getType() {
    return TYPE;
  }

  public QueryFilter parse(final TSDB tsdb, 
                           final ObjectMapper mapper, 
                           final JsonNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    try {
      return (QueryFilter) mapper.treeToValue(node, TagValueRangeFilter.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse TagValueRegexFilter", e);
    }
  }

  @Override
  public String id() {
    return "TagValueRegex";
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

}
