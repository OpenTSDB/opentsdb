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
package net.opentsdb.query.joins;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import net.opentsdb.core.Const;
import net.opentsdb.query.BaseQueryNodeConfig;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The serializable configuration for a time series join.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = JoinConfig.Builder.class)
public class JoinConfig extends BaseQueryNodeConfig<JoinConfig.Builder, JoinConfig> {

  /** The type of join to execute. */
  public static enum JoinType {
    /** Present in A and B. Cross product */
    INNER,
    /** Present in A or B. Cross product */
    OUTER,
    /** Present in A xor B. No cross product */
    OUTER_DISJOINT,
    /** Present in A and/or B. Cross product */
    LEFT,
    /** Present in A not B. No cross product */
    LEFT_DISJOINT,
    /** PResent in B and/or A. Cross product */
    RIGHT,
    /** Present in B not A. No cross product */
    RIGHT_DISJOINT,
    /** Full tag join in both. No cross product */
    NATURAL,
    /** A full outer based on the full tag set. No Cross Product. */
    NATURAL_OUTER,
    /** A full cross-join. */
    CROSS,
  }
  
  /** The type of join to execute. */
  protected final JoinType type;
  
  /** The possibly empty map of joins. If natural this can be empty, for 
   * all others it must be non-empty and non-null. */
  protected final Map<String, String> joins;
  
  /** Set to true if the series must contain values for all of the join
   * tags and <b>only</b> those join tags. */
  protected final boolean explicit_tags;
  
  /**
   * Default protected ctor.
   * @param builder A non-null builder.
   */
  protected JoinConfig(final Builder builder) {
    super(builder);
    if (builder.joinType == null) {
      throw new IllegalArgumentException("Type cannot be null");
    }
    type = builder.joinType;
    if (builder.joins == null) {
      if (type == JoinType.NATURAL || 
          type == JoinType.NATURAL_OUTER || 
          type == JoinType.CROSS) {
        joins = Collections.emptyMap();
      } else {
        throw new IllegalArgumentException("One or more join tag pairs "
            + "must be provided for non-natural joins.");
      }
    } else {
      joins = builder.joins;
    }
    explicit_tags = builder.explicitTags;
  }
  
  /** @return The type of join to execute. */
  public JoinType getJoinType() {
    return type;
  }
  
  /** @return The possibly empty list of join tag pairs. */
  public Map<String, String> getJoins() {
    return joins;
  }
  
  /** @return True if the series must contain values for all of the join
   * tags and <b>only</b> those join tags. */
  public boolean getExplicitTags() {
    return explicit_tags;
  }

  @Override
  public boolean pushDown() {
    // TODO Auto-generated method stub
    return false;
  }
  
  @Override
  public boolean joins() {
    return true;
  }

  @Override
  public Builder toBuilder() {

    Builder cloneBuilder = new Builder();

    cloneBuilder(this, cloneBuilder);

    return cloneBuilder;
  }

  public static void cloneBuilder(
          final JoinConfig joinConfig, final Builder builder) {

    builder.setId(joinConfig.getId())
            .setType(joinConfig.getType())
            .setOverrides(joinConfig.getOverrides() == null ? null : Maps.newHashMap(joinConfig.getOverrides()))
            .setSources(joinConfig.getSources() == null ? null : Lists.newArrayList(joinConfig.getSources()))
            .setJoins(joinConfig.getJoins() == null ? null : Maps.newHashMap(joinConfig.getJoins()))
            .setJoinType(joinConfig.getJoinType())
            .setExplicitTags(joinConfig.getExplicitTags());

  }

  @Override
  public HashCode buildHashCode() {
    final Hasher hasher = Const.HASH_FUNCTION().newHasher()
        .putString(id == null ? "null" : id, Const.UTF8_CHARSET)
        .putString(type.toString(), Const.ASCII_CHARSET)
        .putBoolean(explicit_tags);
    final Map<String, String> sorted = new TreeMap<String, String>(joins);
    for (final Entry<String, String> tags : sorted.entrySet()) {
      hasher.putString(tags.getKey(), Const.UTF8_CHARSET);
      hasher.putString(tags.getValue(), Const.UTF8_CHARSET);
    }
    return hasher.hash();
  }

  @Override
  public int compareTo(final JoinConfig o) {
    if (o == null) {
      return 1;
    }

    return ComparisonChain.start()
        .compare(id, o.id)
        .compare(type.ordinal(), o.type.ordinal())
        .compare(explicit_tags, o.explicit_tags)
        .compare(joins, o.joins, JOIN_CMP)
        .result();
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    
    final JoinConfig other = (JoinConfig) o;
    return Objects.equals(id, other.id) && 
           Objects.equals(type, other.type) &&
           Objects.equals(explicit_tags, other.explicit_tags) &&
           Objects.equals(joins, other.joins);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  public static Builder newBuilder(){
    return new Builder();
  }
  
  public static class Builder extends BaseQueryNodeConfig.Builder<Builder, JoinConfig> {
    @JsonProperty
    private JoinType joinType;
    @JsonProperty
    private Map<String, String> joins;
    @JsonProperty
    private boolean explicitTags;
    
    protected Builder() {
      setId("Join");
      setType("Join");
      joinType = JoinType.NATURAL_OUTER;
    }
    
    public Builder setJoinType(final JoinType type) {
      this.joinType = type;
      return this;
    }
    
    public Builder setJoins(final Map<String, String> joins) {
      this.joins = joins;
      return this;
    }
    
    public Builder addJoins(final String left_tag, final String right_tag) {
      if (joins == null) {
        joins = Maps.newHashMap();
      }
      joins.put(left_tag, right_tag);
      return this;
    }
    
    public Builder setExplicitTags(final boolean explicit_tags) {
      this.explicitTags = explicit_tags;
      return this;
    }
    
    @Override
    public JoinConfig build() {
      return new JoinConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }

  }
 
  static class ListComparator implements Comparator<Map<String, String>> {

    @Override
    public int compare(final Map<String, String> a,
                       final Map<String, String> b) {
      if (a == null && b == null) {
        return 0;
      }
      if (a != null && b == null) {
        return 1;
      }
      if (a == null && b != null) {
        return -1;
      }
      int diff = a.size() - b.size();
      if (diff != 0) {
        return diff;
      }
      
      for (final Entry<String, String> entry : a.entrySet()) {
        final String right = b.get(entry.getKey());
        if (right == null) {
          return 1;
        }
        diff = entry.getValue().compareTo(right);
        if (diff != 0) {
          return diff;
        }
      }
      return 0;
    }
    
  }
  static final ListComparator JOIN_CMP = new ListComparator();

}