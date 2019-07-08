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
import com.google.common.hash.HashCode;
import net.opentsdb.core.Const;

/**
 * A base class for tag value filters including the raw filter and the
 * tag key to check on.
 */
public abstract class BaseTagValueFilter implements TagValueFilter {

  /** The tag key to filter on. */
  protected final String tag_key;
  
  /** The raw filter from the user. */
  protected final String filter;
  
  /**
   * Default ctor.
   * @param tag The non-null and non-empty tag key.
   * @param filter The non-null and non-empty filter.
   * @throws IllegalArgumentException if either argument is null or empty.
   */
  protected BaseTagValueFilter(final String tag, final String filter) {
    if (Strings.isNullOrEmpty(tag)) {
      throw new IllegalArgumentException("Tag cannot be null or empty.");
    }
    if (Strings.isNullOrEmpty(filter)) {
      throw new IllegalArgumentException("Filter cannot be null.");
    }
    this.tag_key = tag;
    this.filter = filter;
  }
  
  /** @return The tag key to filter on. */
  public String getTagKey() {
    return tag_key;
  }
  
  /** @return The raw filter given by the user. */
  @Override
  public String getFilter() {
    return filter;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final BaseTagValueFilter otherBaseFilter = (BaseTagValueFilter) o;

    return Objects.equal(tag_key, otherBaseFilter.getTagKey())
            && Objects.equal(filter, otherBaseFilter.getFilter());

  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }


  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
            .putString(Strings.nullToEmpty(tag_key), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(filter), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(getType()), Const.UTF8_CHARSET)
            .hash();

    return hc;
  }
}
