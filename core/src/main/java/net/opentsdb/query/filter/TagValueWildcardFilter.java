// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Const;
import net.opentsdb.stats.Span;

/**
 * A wildcard or glob match on tag values given a literal tag key.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = TagValueWildcardFilter.Builder.class)
public class TagValueWildcardFilter extends BaseTagValueFilter {

  /** Whether or not the filter had a postfix asterisk */
  protected final boolean has_postfix;
  
  /** Whether or not the filter had a prefix asterisk */
  protected final boolean has_prefix;
  
  /** The individual components to match on */
  protected final String[] components;
  
  /** Whether or not the wildcard would match-all. */
  protected final boolean matches_all;
  
  /**
   * Protected ctor.
   * @param builder The non-null builder.
   */
  protected TagValueWildcardFilter(final Builder builder) {
    super(builder.key, builder.filter);
    if (!filter.contains("*")) {
      throw new IllegalArgumentException("Filter must contain an asterisk");
    }
    String actual = filter.trim();
    if (actual.trim().equals("*")) {
      matches_all = true;
    } else {
      matches_all = false;
    }
    
    if (actual.charAt(0) == '*') {
      has_postfix = true;
      while (actual.charAt(0) == '*') {
        if (actual.length() < 2) {
          break;
        }
        actual = actual.substring(1);
      }
    } else {
      has_postfix = false;
    }
    if (actual.charAt(actual.length() - 1) == '*') {
      has_prefix = true;
      while(actual.charAt(actual.length() - 1) == '*') {
        if (actual.length() < 2) {
          break;
        }
        actual = actual.substring(0, actual.length() - 1);
      }
    } else {
      has_prefix = false;
    }
    if (actual.indexOf('*') > 0) {
      components = actual.split("\\*");
    } else {
      components = new String[1];
      components[0] = actual;
    }
  }

  @Override
  public boolean matches(final Map<String, String> tags) {
    String tagv = tags.get(tag_key);
    if (tagv == null) {
      return false;
    } else if (matches_all || 
        components.length == 1 && 
        components[0].equals("*")) {
      // match all
      return true;
    }
    
    if (has_postfix && !has_prefix && 
        !tagv.endsWith(components[components.length-1])) {
      return false;
    }
    if (has_prefix && !has_postfix && !tagv.startsWith(components[0])) {
      return false;
    }
    int idx = 0;
    for (int i = 0; i < components.length; i++) {
      if (tagv.indexOf(components[i], idx) < 0) {
        return false;
      }
      idx += components[i].length();
    }
    return true;
  }

  @Override
  public boolean matches(final String tag_key, final String tag_value) {
    if (Strings.isNullOrEmpty(tag_key)) {
      return false;
    }
    if (!this.tag_key.equals(tag_key)) {
      return false;
    }
    if (Strings.isNullOrEmpty(tag_value)) {
      return false;
    }
    if (matches_all || 
        components.length == 1 && 
        components[0].equals("*")) {
      // match all
      return true;
    }
    if (has_postfix && !has_prefix && 
        !tag_value.endsWith(components[components.length-1])) {
      return false;
    }
    if (has_prefix && !has_postfix && !tag_value.startsWith(components[0])) {
      return false;
    }
    int idx = 0;
    for (int i = 0; i < components.length; i++) {
      if (tag_value.indexOf(components[i], idx) < 0) {
        return false;
      }
      idx += components[i].length();
    }
    return true;
  }
  
  @Override
  public boolean matches(final String tag_value) {
    if (matches_all || 
        components.length == 1 && 
        components[0].equals("*")) {
      // match all
      return true;
    }
    if (has_postfix && !has_prefix && 
        !tag_value.endsWith(components[components.length-1])) {
      return false;
    }
    if (has_prefix && !has_postfix && !tag_value.startsWith(components[0])) {
      return false;
    }
    int idx = 0;
    for (int i = 0; i < components.length; i++) {
      if (tag_value.indexOf(components[i], idx) < 0) {
        return false;
      }
      idx += components[i].length();
    }
    return true;
  }
  
  @Override
  public String getType() {
    return TagValueWildcardFactory.TYPE;
  }
  
  /** Whether or not the wildcard would match all strings. */
  public boolean matchesAll() {
    return matches_all;
  }
  
  /** @return The components of the filter. */
  public String[] components() {
    return components;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    // is this necessary?
    if (!super.equals(o)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final List<HashCode> hashes =
            Lists.newArrayListWithCapacity(2);

    hashes.add(super.buildHashCode());

    final HashCode hc = Const.HASH_FUNCTION().newHasher()
            .putString(Strings.nullToEmpty(getType()), Const.UTF8_CHARSET)
            .hash();

    hashes.add(hc);

    return Hashing.combineOrdered(hashes);
  }
  
  @Override
  public Deferred<Void> initialize(final Span span) {
    return INITIALIZED;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {
    @JsonProperty
    private String key;
    @JsonProperty
    private String filter;
    
    public Builder setKey(final String tag_key) {
      this.key = tag_key;
      return this;
    }
    
    public Builder setFilter(final String filter) {
      this.filter = filter;
      return this;
    }
    
    public TagValueWildcardFilter build() {
      return new TagValueWildcardFilter(this);
    }
  }
  
}
