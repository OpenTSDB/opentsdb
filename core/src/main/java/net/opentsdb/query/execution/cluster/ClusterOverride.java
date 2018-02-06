// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.execution.cluster;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.core.Const;

/**
 * A named override for a {@link ClusterConfigPlugin} that allow for users to 
 * query a subset or entirely different set of clusters using the plugin's 
 * execution graph. E.g. if the default has 3 clusters but a user only wants to
 * query 2 of them (or 1 for debugging) then they could use a named override
 * instead of having to provide a full config with their query.
 * <p>
 * <b>Note:</b> The {@link #getClusters()} list is purposely not sorted as 
 * the order of clusters may be important in the plugin implementation.
 *
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = ClusterOverride.Builder.class)
public class ClusterOverride implements Comparable<ClusterOverride> {
  /** The ID of this override. */
  protected String id;
  
  /** The cluster descriptors that will be substituted. */
  protected List<ClusterDescriptor> clusters;
  
  /**
   * Protected ctor.
   * @param builder A non-null builder.
   * @throws IllegalArgumentException if the ID or clusters were null or empty. 
   */
  protected ClusterOverride(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.id)) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    if (builder.clusters == null || builder.clusters.isEmpty()) {
      throw new IllegalArgumentException("Clusters cannot be null or empty.");
    }
    id = builder.id;
    clusters = builder.clusters;
  }
  
  /** @return The ID of the override. */
  public String getId() {
    return id;
  }
  
  /** @return A list of cluster descriptors for overriding. */
  public List<ClusterDescriptor> getClusters() {
    return Collections.unmodifiableList(clusters);
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    
    final ClusterOverride override = (ClusterOverride) o;
    
    return Objects.equal(id, override.id)
        && Objects.equal(clusters, override.clusters);
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
    if (clusters != null) {
      final List<HashCode> hashes = 
          Lists.newArrayListWithCapacity(clusters.size() + 1);
      hashes.add(hc);
      for (final ClusterDescriptor cluster : clusters) {
        hashes.add(cluster.buildHashCode());
      }
      return Hashing.combineOrdered(hashes);
    }
    return hc;
  }
  
  @Override
  public int compareTo(final ClusterOverride o) {
    return ComparisonChain.start()
        .compare(id, o.id, Ordering.natural().nullsFirst())
        .compare(clusters, o.clusters, 
            Ordering.<ClusterDescriptor>natural().lexicographical().nullsFirst())
        .result();
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("id=")
        .append(id)
        .append(", clusters=")
        .append(clusters)
        .toString();
  }
  
  /** @return A new builder to construct an override with. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Clones the override into a new builder, making copies of the cluster
   * descriptors.
   * @param override A non-null cluster to copy from.
   * @return A clone of the original override.
   */
  public static Builder newBuilder(final ClusterOverride override) {
    final Builder builder = new Builder()
        .setId(override.id);
    if (override.clusters != null) {
      for (final ClusterDescriptor cluster : override.clusters) {
        builder.addCluster(ClusterDescriptor.newBuilder(cluster));
      }
    }
    return builder;
  }
  
  /** A builder for the ClusterOverride. */
  public static class Builder {
    @JsonProperty
    private String id;
    @JsonProperty
    private List<ClusterDescriptor> clusters;

    /**
     * @param id A non-null and non-empty ID for the override.
     * @return The builder.
     */
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    /**
     * @param clusters A non-null and non-empty list of cluster descriptors.
     * @return The builder.
     */
    public Builder setClusters(final List<ClusterDescriptor> clusters) {
      this.clusters = clusters;
      // Purposely not sorting here.
      //if (this.clusters != null) {
      //  Collections.sort(this.clusters);
      //}
      return this;
    }
    
    /**
     * @param cluster A non-null cluster to add to the list.
     * @return The builder.
     */
    public Builder addCluster(final ClusterDescriptor cluster) {
      if (clusters == null) {
        clusters = Lists.newArrayList(cluster);
      } else {
        clusters.add(cluster);
        // Purposely not sorting here.
        //Collections.sort(clusters);
      }
      return this;
    }
    
    /**
     * @param cluster A non-null cluster to add to the list.
     * @return The builder.
     */
    @JsonIgnore
    public Builder addCluster(final ClusterDescriptor.Builder cluster) {
      if (clusters == null) {
        clusters = Lists.newArrayList(cluster.build());
      } else {
        clusters.add(cluster.build());
        // Purposely not sorting here.
        //Collections.sort(clusters);
      }
      return this;
    }
    
    /** @return An instantiated cluster override or exceptions if required 
     * parameters were missing. */
    public ClusterOverride build() {
      return new ClusterOverride(this);
    }
  }
}
