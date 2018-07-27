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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

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
    super(builder.tagKey, builder.filter);
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

  /** Whether or not the wildcard would match all strings. */
  public boolean matchesAll() {
    return matches_all;
  }
  
  /** @return The components of the filter. */
  public String[] components() {
    return components;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {
    @JsonProperty
    private String tagKey;
    @JsonProperty
    private String filter;
    
    public Builder setTagKey(final String tag_key) {
      this.tagKey = tag_key;
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
