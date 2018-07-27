// This file is part of OpenTSDB.
// Copyright (C) 2015-2018  The OpenTSDB Authors.
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
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import net.opentsdb.utils.StringUtils;

/**
 * Filters on a set of one or more case sensitive tag value strings.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = TagValueLiteralOrFilter.Builder.class)
public class TagValueLiteralOrFilter extends BaseTagValueFilter 
   implements TagValueFilter {
  
  /** A list of strings to match on */
  protected final List<String> literals;
  
  /**
   * Protected builder ctor
   * @param builder The non-null builder.
   * @throws IllegalArgumentException if the tagk or filter were empty or null
   */
  protected TagValueLiteralOrFilter(final Builder builder) {
    super(builder.tagKey, builder.filter);
    if (filter.length() == 1 && filter.charAt(0) == '|') {
      throw new IllegalArgumentException("Filter must contain more than just a pipe");
    }
    final String[] split = StringUtils.splitString(filter, '|');
    
    // dedupe
    final Set<String> dedupe = Sets.newHashSetWithExpectedSize(split.length);
    literals = Lists.newArrayListWithCapacity(split.length);
    for (String value : split) {
      value = value.trim();
      if (Strings.isNullOrEmpty(value)) {
        continue;
      }
      if (!dedupe.contains(value)) {
        dedupe.add(value);
        literals.add(value);
      }
    }
  }
  
  @Override
  public boolean matches(final Map<String, String> tags) {
    final String tagv = tags.get(tag_key);
    if (tagv == null) {
      return false;
    }
    return literals.contains(tagv);
  }
  
  /** @return The collection of literal strings for resolution. */
  public List<String> literals() {
    return literals;
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
    
    public TagValueLiteralOrFilter build() {
      return new TagValueLiteralOrFilter(this);
    }
  }
}
