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
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * Attempts to walk all of the child filters and extract the tag keys
 * to make sure the time series matched overall include all of the tags
 * and only those tags.
 * <p>
 * Note that if no tags were pulled from the child filter(s) then the
 * filter will always return false for series containing tags.
 * 
 * @since 3.0
 */
public class ExplicitTagsFilter implements TagKeyFilter {

  /** The filter to invert. */
  private final QueryFilter filter;
  
  /** The set of explicit tag keys we want to find. */
  private Set<String> tag_keys;
  
  /**
   * Protected ctor.
   * @param builder The non-null filter.
   */
  protected ExplicitTagsFilter(final Builder builder) {
    if (builder.filter == null) {
      throw new IllegalArgumentException("Filter cannot be null.");
    }
    filter = builder.filter;
    tag_keys = Sets.newHashSet();
    processFilter(filter);
  }
  
  @Override
  public String filter() {
    return "null";
  }

  /** @return The list of tag keys to match on. */
  public Set<String> tagKeys() {
    return tag_keys;
  }
  
  public boolean matches(final Map<String, String> tags) {
    int matches = 0;
    for (final String tag_key : tags.keySet()) {
      if (!tag_keys.contains(tag_key)) {
        return false;
      }
      matches++;
    }
    if (matches != tag_keys.size()) {
      return false;
    }
    return true;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private QueryFilter filter;
    
    public Builder setFilter(final QueryFilter filter) {
      this.filter = filter;
      return this;
    }
    
    public ExplicitTagsFilter build() {
      return new ExplicitTagsFilter(this);
    }
  }

  private void processFilter(final QueryFilter filter) {
    if (filter instanceof NotFilter) {
      // TODO - don't know if this is right, but for now, it *could*
      // mean that the user *wants* to have some tag keys with some
      // values but skip the ones explicitly defined
      return;
    }
    
    // TODO - tag key filters.
    
    if (filter instanceof TagValueFilter) {
      tag_keys.add(((TagValueFilter) filter).tagKey());
      return;
    }
    if (filter instanceof ChainFilter) {
      for (final QueryFilter child : ((ChainFilter) filter).getFilters()) {
        processFilter(child);
      }
    }
  }
}
