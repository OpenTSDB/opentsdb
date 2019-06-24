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
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.Span;

import java.util.List;

public class UTFilterFactory extends BaseTSDBPlugin implements 
    QueryFilterFactory {

  static class UTQueryFilter implements QueryFilter {
    public String type;
    public String tag;
    public String filter;
    
    public UTQueryFilter() { }
    
    public UTQueryFilter(final String tag, final String filter) {
      this.tag = tag;
      this.filter = filter;
    }
    
    @Override
    public String getType() {
      return "UTFilter";
    }

    @Override
    public Deferred<Void> initialize(final Span span) {
      return INITIALIZED;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      final UTQueryFilter query = (UTQueryFilter) o;


      return Objects.equal(type, query.type)
              && Objects.equal(tag, query.tag)
              && Objects.equal(filter, query.filter);
    }

    @Override
    public int hashCode() {
      return buildHashCode().asInt();
    }

    /** @return A HashCode object for deterministic, non-secure hashing */
    public HashCode buildHashCode() {
      final HashCode hc = Const.HASH_FUNCTION().newHasher()
              .putString(Strings.nullToEmpty(type), Const.UTF8_CHARSET)
              .putString(Strings.nullToEmpty(tag), Const.UTF8_CHARSET)
              .putString(Strings.nullToEmpty(filter), Const.UTF8_CHARSET)
              .hash();

      return hc;
    }

  }

  @Override
  public String getType() {
    return "UTQueryFilter";
  }

  @Override
  public QueryFilter parse(TSDB tsdb, ObjectMapper mapper, JsonNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    try {
      return (QueryFilter) mapper.treeToValue(node, UTQueryFilter.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse UTQueryFilter", e);
    }
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public String type() {
    return "UTQueryFilter";
  }
  
}
