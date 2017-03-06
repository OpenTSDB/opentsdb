// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.processor;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.common.Const;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Join;
import net.opentsdb.utils.Bytes;

/**
 * Configuration class for Join processors. Requires a {@link Join} config and
 * optionally a set of {@link Filter}s.
 * <p>
 * <b>Note:</b> If {@link Join#getUseQueryTags()} is true, then the {@link #filters}
 * must be non-null or the builder will throw an exception.
 * TODO - encoding decoding of the tags, e.g. if we have the UIDs up this far we
 * need to convert strings to UIDs for byte comparisson.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = JoinConfig.Builder.class)
public class JoinConfig implements Comparable<JoinConfig> {

  /** The join config to use. */
  private Join join;
  
  /** Optional filters to use in the join. */
  private Filter filters;
  
  /** Optional list of tag keys to join on. 
   * NOTE: Do not account for this in equals, compare or hash code. Comes from 
   * Join and Filter. */
  private List<byte[]> tag_keys;
  
  /**
   * Private CTor to construct from a builder.
   * @param builder A non-null builder.
   */
  protected JoinConfig(final Builder builder) {
    join = builder.join;
    filters = builder.filters;
    
    // TODO - proper encoding
    if ((join.getTags() != null && !join.getTags().isEmpty()) || 
        join.getUseQueryTags()) {
      tag_keys = Lists.newArrayList();
      if (join.getTags() != null && !join.getTags().isEmpty()) {
        for (final String tag : join.getTags()) {
          tag_keys.add(tag.getBytes(Const.UTF8_CHARSET));
        }
      } else {
        for (final TagVFilter filter : filters.getTags()) {
          tag_keys.add(filter.getTagk().getBytes(Const.UTF8_CHARSET));
        }
      }
      Collections.sort(tag_keys, Bytes.MEMCMP);
    }
  }
  
  /** @return The join config to use. */
  public Join getJoin() {
    return join;
  }
  
  /** @return Optional filters to use if the join requires them. */
  public Filter getFilters() {
    return filters;
  }
  
  /** @return An optional list of tag keys converted to byte arrays. */
  public List<byte[]> getTagKeys() {
    return tag_keys;
  }
  
  /** @return A new builder for the expression config. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    
    final JoinConfig that = (JoinConfig) o;
    return Objects.equal(that.join, join) &&
           Objects.equal(that.filters, filters);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(2);
    hashes.add(join.buildHashCode());
    if (filters != null) {
      hashes.add(filters.buildHashCode());
    }
    return Hashing.combineOrdered(hashes);
  }
  
  @Override
  public int compareTo(final JoinConfig o) {
    return ComparisonChain.start()
        .compare(join, o.join)
        .compare(filters, o.filters)
        .result();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    @JsonProperty
    private Join join;
    @JsonProperty
    private Filter filters;    
    
    public Builder setJoin(final Join join) {
      this.join = join;
      return this;
    }
    
    public Builder setFilters(final Filter filters) {
      this.filters = filters;
      return this;
    }
    
    public JoinConfig build() {
      if (join == null) {
        throw new IllegalArgumentException("Join cannot be null.");
      }
      join.validate();
      if (join.getUseQueryTags()) {
        if (filters == null) {
          throw new IllegalArgumentException("Filters cannot be null when "
              + "useQueryTags is true in the join config.");
        }
        filters.validate();
      }
      
      return new JoinConfig(this);
    }
  }
}
