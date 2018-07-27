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

import com.google.common.base.Strings;

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
  public String tagKey() {
    return tag_key;
  }
  
  /** @return The raw filter given by the user. */
  @Override
  public String filter() {
    return filter;
  }
}
