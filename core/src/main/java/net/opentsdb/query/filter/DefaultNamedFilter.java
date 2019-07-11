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

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import net.opentsdb.core.Const;

import java.util.List;

/**
 * A default implementation of the NamedFilter.
 * 
 * @since 3.0
 */
public class DefaultNamedFilter implements NamedFilter {

  /** The non-null ID. */
  protected final String id;
  
  /** The non-null filter. */
  protected final QueryFilter filter;
  
  /**
   * Protected ctor.
   * @param builder A non-null builder.
   * @throws IllegalArgumentException if the id was null, empty or the 
   * filter was null.
   */
  protected DefaultNamedFilter(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.id)) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    if (builder.filter == null) {
      throw new IllegalArgumentException("Filter cannot be null.");
    }
    id = builder.id;
    filter = builder.filter;
  }
  
  @Override
  public String getId() {
    return id;
  }

  @Override
  public QueryFilter getFilter() {
    return filter;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final DefaultNamedFilter otherNamedFilter = (DefaultNamedFilter) o;

    return Objects.equal(id, otherNamedFilter.getId())
            && Objects.equal(filter, otherNamedFilter.getFilter());

  }


  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }


  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
            .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
            .hash();
    final List<HashCode> hashes =
            Lists.newArrayListWithCapacity(2);

    hashes.add(hc);

    if (filter != null) {
      hashes.add(filter.buildHashCode());
    }

    return Hashing.combineOrdered(hashes);
  }

  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private String id;
    private QueryFilter filter;
    
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    public Builder setFilter(final QueryFilter filter) {
      this.filter = filter;
      return this;
    }
    
    public DefaultNamedFilter build() {
      return new DefaultNamedFilter(this);
    }
  }
}
