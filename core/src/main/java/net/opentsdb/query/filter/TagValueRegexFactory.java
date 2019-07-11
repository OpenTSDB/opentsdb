// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

/**
 * Factory to construct the TagValueLiteralOr filter.
 * 
 * @since 3.0
 */
public class TagValueRegexFactory extends BaseTSDBPlugin
    implements QueryFilterFactory {

  static final String TYPE = "TagValueRegex";
  
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
    TagValueRegexFilter.Builder builder = TagValueRegexFilter.newBuilder();
    JsonNode n = node.get("tagKey");
    if (n != null && !n.isNull()) {
      builder.setKey(n.asText());
    }
    n = node.get("key");
    if (n != null && !n.isNull()) {
      builder.setKey(n.asText());
    }
    
    n = node.get("filter");
    if (n != null && !n.isNull()) {
      builder.setFilter(n.asText());
    } else {
      throw new IllegalArgumentException("Filter cannot be null or empty.");
    }
    return builder.build();
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    return Deferred.fromResult(null);
  }
  
  @Override
  public String version() {
    return "3.0.0";
  }
}
