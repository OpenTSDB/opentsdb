// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.query.hacluster;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.BaseTimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.utils.Comparators;
import net.opentsdb.utils.DateTime;

/**
 * The config for a high-availability cluster query wherein the same data
 * is written to multiple locations and the query is executed against all
 * of them, merging the results.
 *
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = HAClusterConfig.Builder.class)
public class HAClusterConfig extends BaseTimeSeriesDataSourceConfig<
    HAClusterConfig.Builder, HAClusterConfig> {

  /** The non-null and non-empty list of sources to query. */
  private final List<String> data_sources;

  /** A list of custom, fully defined data sources. */
  private final List<TimeSeriesDataSourceConfig> data_source_configs;

  /** The non-null and non-empty aggregator to use to merge results. */
  private final String merge_aggregator;

  /** An optional timeout for the secondary (etc) sources. */
  private final String secondary_timeout;

  /** An optional timeout for the primary source when a secondary
   * returns first. */
  private final String primary_timeout;

  /**
   * Fails if any of the sources timeout/error out
   */
  private final String fail_on_any_error;

  /** A hash that calculates and stores the hash code once. */
  private int hash;

  /**
   * Default ctor.
   * @param builder A non-null builder.
   * @throws IllegalArgumentException if the ID was null or empty, if the
   * cluster config was null or the execution graph config was null.
   */
  protected HAClusterConfig(final Builder builder) {
    super(builder);
    data_sources = builder.dataSources == null ?
        Collections.emptyList() : builder.dataSources;
    data_source_configs = builder.dataSourceConfigs == null ?
        Collections.emptyList() : builder.dataSourceConfigs;
    merge_aggregator = builder.mergeAggregator;
    secondary_timeout = builder.secondaryTimeout;
    primary_timeout = builder.primaryTimeout;
    fail_on_any_error = builder.failOnAnyError;
    // validate the timeouts
    if (!Strings.isNullOrEmpty(secondary_timeout)) {
      DateTime.parseDuration(secondary_timeout);
    }
    if (!Strings.isNullOrEmpty(primary_timeout)) {
      DateTime.parseDuration(primary_timeout);
    }
    hash = buildHashCode().asInt();
  }

  /** @return The non-null list of sources to query. The first entry is
   * primary. */
  public List<String> getDataSources() {
    return data_sources;
  }

  /** @return The non-null list of data source config overrides. */
  public List<TimeSeriesDataSourceConfig> getDataSourceConfigs() {
    return data_source_configs;
  }

  /** @return The non-null and non-empty aggregator to use to merge results. */
  public String getMergeAggregator() {
    return merge_aggregator;
  }

  /** @return An optional timeout for the secondary (etc) sources. */
  public String getSecondaryTimeout() {
    return secondary_timeout;
  }

  /**@return */
  public String failOnAnyError() {
    return fail_on_any_error;
  }

  /** @return An optional timeout for the primary when a secondary
   * responds first. */
  public String getPrimaryTimeout() {
    return primary_timeout;
  }

  /** @return Whether or not this node has gone through the planner setup
   * step. */
  public boolean getHasBeenSetup() {
    return has_been_setup;
  }

  @Override
  public boolean pushDown() {
    return false;
  }

  @Override
  public boolean joins() {
    // NOTE: We purposely leave this false so that we don't treat it
    // as a source.
    return false;
  }

  @Override
  public int compareTo(HAClusterConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    if (!super.equals(o)) {
      return false;
    }

    final HAClusterConfig haconfig = (HAClusterConfig) o;


    final boolean result = Objects.equal(merge_aggregator, haconfig.getMergeAggregator())
            && Objects.equal(primary_timeout, haconfig.getPrimaryTimeout())
            && Objects.equal(secondary_timeout, haconfig.getSecondaryTimeout());

    if (!result) {
      return false;
    }

    // comparing data sources
    if (!Comparators.ListComparison.equalLists(data_sources, haconfig.getDataSources())) {
      return false;
    }

    // comparing data sources configs
    if (!Comparators.ListComparison.equalLists(data_source_configs, haconfig.getDataSourceConfigs())) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    if (cached_hash != null) {
      return cached_hash;
    }
    
    final Hasher hc = Const.HASH_FUNCTION().newHasher()
            .putString(Strings.nullToEmpty(merge_aggregator), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(secondary_timeout), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(primary_timeout), Const.UTF8_CHARSET);
    final List<HashCode> hashes =
            Lists.newArrayListWithCapacity(2 +
                    (data_source_configs != null ? data_source_configs.size() : 0));

    hashes.add(super.buildHashCode());

    if (data_sources != null) {
      final List<String> keys = Lists.newArrayList(data_sources);
      Collections.sort(keys);
      for (final String key : keys) {
        hc.putString(key, Const.UTF8_CHARSET);
      }
      hashes.add(hc.hash());
    }

    if (data_source_configs != null) {
      for (final TimeSeriesDataSourceConfig node : data_source_configs) {
        hashes.add(node.buildHashCode());
      }
    }

    cached_hash = Hashing.combineOrdered(hashes);
    return cached_hash;
  }

  @Override
  public Builder toBuilder() {
    final Builder builder = new Builder()
        .setMergeAggregator(merge_aggregator)
        .setSecondaryTimeout(secondary_timeout)
        .setPrimaryTimeout(primary_timeout)
        .setFailOnAnyError(fail_on_any_error)
        .setHasBeenSetup(has_been_setup);
    if (!data_sources.isEmpty()) {
      builder.setDataSources(Lists.newArrayList(data_sources));
    }
    if (!data_source_configs.isEmpty()) {
      builder.setDataSourceConfigs(Lists.newArrayList(data_source_configs));
    }
    BaseTimeSeriesDataSourceConfig.cloneBuilder(this, builder);
    return builder;
  }

  /** @return A new builder to construct a Cluster Config */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** The builder class for cluster configs. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseTimeSeriesDataSourceConfig.Builder<
      Builder, HAClusterConfig> {
    @JsonProperty
    private List<String> dataSources;
    @JsonProperty
    private List<TimeSeriesDataSourceConfig> dataSourceConfigs;
    @JsonProperty
    private String mergeAggregator;
    @JsonProperty
    private String secondaryTimeout;
    @JsonProperty
    private String primaryTimeout;
    @JsonProperty
    private String failOnAnyError;

    Builder() {
      setType("HAClusterConfig");
    }

    /**
     * @param data_sources The list of sources to query from.
     * @return The builder.
     */
    public Builder setDataSources(final List<String> data_sources) {
      this.dataSources = data_sources;
      return this;
    }

    public Builder addDataSource(final String source) {
      if (dataSources == null) {
        dataSources = Lists.newArrayList();
      }
      dataSources.add(source);
      return this;
    }

    public Builder setDataSourceConfigs(
        final List<TimeSeriesDataSourceConfig> data_source_configs) {
      dataSourceConfigs = data_source_configs;
      return this;
    }

    public Builder addDataSourceConfig(final TimeSeriesDataSourceConfig config) {
      if (dataSourceConfigs == null) {
        dataSourceConfigs = Lists.newArrayList();
      }
      dataSourceConfigs.add(config);
      return this;
    }

    /**
     * @param merge_aggregator The aggregation function to use.
     * @return The builder.
     */
    @JsonIgnore
    public Builder setMergeAggregator(final String merge_aggregator) {
      mergeAggregator = merge_aggregator;
      return this;
    }

    /**
     * @param secondary_timeout The amount of time to wait after the
     * primary call before returning data. E.g. "5s".
     * @return The builder.
     */
    public Builder setSecondaryTimeout(final String secondary_timeout) {
      secondaryTimeout = secondary_timeout;
      return this;
    }

    /**
     * @param primary_timeout The amount of time to wait after a
     * secondary response comes in before returning data. E.g. "5s".
     * @return The builder.
     */
    public Builder setPrimaryTimeout(final String primary_timeout) {
      primaryTimeout = primary_timeout;
      return this;
    }

    public Builder setFailOnAnyError(final String fail_on_any_error) {
      failOnAnyError = fail_on_any_error;
      return this;
    }

    public List<String> dataSources() {
      return dataSources == null ? Collections.emptyList() : dataSources;
    }

    public List<TimeSeriesDataSourceConfig> dataSourceConfigs() {
      return dataSourceConfigs == null ? Collections.emptyList() : dataSourceConfigs;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public String sourceId() {
      return sourceId;
    }

    public String mergeAggregator() {
      return mergeAggregator;
    }

    /** @return The instantiated ClusterConfig on success or exceptions on
     * failure. */
    public HAClusterConfig build() {
      return new HAClusterConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }

  }

  public static HAClusterConfig parse(final ObjectMapper mapper,
                                      final TSDB tsdb,
                                      final JsonNode node) {
    Builder builder = new Builder();
    BaseTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node, builder);

    JsonNode n = node.get("dataSources");
    if (n != null) {
      try {
        builder.setDataSources(mapper.treeToValue(n, List.class));
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Failed to parse json", e);
      }
    }

    n = node.get("id");
    if (n != null) {
      builder.setId(n.asText());
    }

    n = node.get("mergeAggregator");
    if (n != null) {
      builder.setMergeAggregator(n.asText());
    }

    n = node.get("secondaryTimeout");
    if (n != null) {
      builder.setSecondaryTimeout(n.asText());
    }

    n = node.get("primaryTimeout");
    if (n != null) {
      builder.setPrimaryTimeout(n.asText());
    }

    return (HAClusterConfig) builder.build();
  }

}
