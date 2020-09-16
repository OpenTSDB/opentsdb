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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Const;
import net.opentsdb.stats.Span;

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
public class ExplicitTagsFilter implements TagKeyFilter, NestedQueryFilter {

  /** The nested filters. */
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
  public QueryFilter getFilter() {
    return filter;
  }
  
  @Override
  public String filter() {
    return "null";
  }

  /** @return The list of tag keys to match on. */
  public Set<String> tagKeys() {
    return tag_keys;
  }
  
  @Override
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

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final ExplicitTagsFilter otherExplicitFilter = (ExplicitTagsFilter) o;

    return Objects.equal(filter, otherExplicitFilter.getFilter())
            && Objects.equal(tag_keys, otherExplicitFilter.tagKeys());

  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final List<HashCode> hashes =
            Lists.newArrayListWithCapacity(2);

    if (filter != null) {
      hashes.add(filter.buildHashCode());
    }


    if (tag_keys != null) {
      final List<String> keys = Lists.newArrayList(tag_keys);
      Collections.sort(keys);
      final Hasher hasher = Const.HASH_FUNCTION().newHasher();
      for (final String key : keys) {
        hasher.putString(key, Const.UTF8_CHARSET);
      }
      hasher.putString(Strings.nullToEmpty(getType()), Const.UTF8_CHARSET);
      hashes.add(hasher.hash());
    }

    return Hashing.combineOrdered(hashes);
  }
  
  @Override
  public boolean matches(final String tag_key) {
    return tag_keys.contains(tag_key);
  }
  
  @Override
  public String getType() {
    return ExplicitTagsFilterFactory.TYPE;
  }
  
  @Override
  public Deferred<Void> initialize(final Span span) {
    return filter.initialize(span);
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
    
    if (filter instanceof TagValueFilter) {
      tag_keys.add(((TagValueFilter) filter).getTagKey());
      return;
    }
    
    if (filter instanceof TagKeyFilter) {
      tag_keys.add(((TagKeyFilter) filter).filter());
      return;
    }
    
    if (filter instanceof ChainFilter) {
      for (final QueryFilter child : ((ChainFilter) filter).getFilters()) {
        processFilter(child);
      }
    }
  }
}
