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
package net.opentsdb.storage.schemas.tsdb1x;

import java.util.List;

import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagValueFilter;

/**
 * A resolved tag value filter. The tag key should be resolved for all
 * types (assuming the string was found) but only the literal filter
 * will have resolved tag values.
 * 
 * @since 3.0
 */
public class ResolvedTagValueFilter implements ResolvedQueryFilter {

  /** The original filter. */
  private final TagValueFilter filter;

  /** The resolved tag value. May be null if not found. */
  private byte[] tag_key;
  
  /** The optional list of resolved tag values. May be null for
   * non-literal filters and nulls may be present in the list. */
  private List<byte[]> tag_values;
  
  /**
   * Default ctor.
   * @param filter The non-null original filter.
   */
  public ResolvedTagValueFilter(final TagValueFilter filter) {
    this.filter = filter;
  }
  
  @Override
  public QueryFilter filter() {
    return filter;
  }

  /** @return The tag key. May be null of not found. */
  public byte[] getTagKey() {
    return tag_key;
  }
  
  /** @return The list of tag values. Can be null for non-literal filters
   * and may contain nulls for tags that weren't found. */
  public List<byte[]> getTagValues() {
    return tag_values;
  }
  
  /** @param tag_key The resolved tag key. May be null. */
  public void setTagKey(final byte[] tag_key) {
    this.tag_key = tag_key;
  }
  
  /** @param tag_values The list of resolved tag values. */
  public void setTagValues(final List<byte[]> tag_values) {
    this.tag_values = tag_values;
  }
  
}
