// This file is part of OpenTSDB.
// Copyright (C) 2018-2021  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.merge;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import com.google.common.hash.Hashing;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryResultId;

import java.util.List;

/**
 * Configures a time series merger for either multi-data center queries
 * {@link MergeMode#HA} where we should have the same data and we pick one or
 * the other values OR for cross source queries {@link MergeMode#SPLIT} where
 * different sources have different time slices of data.
 *
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = MergerConfig.Builder.class)
public class MergerConfig extends BaseQueryNodeConfigWithInterpolators<MergerConfig.Builder, MergerConfig> {

  public enum MergeMode {
    /** Split queries where different sources have different time slices of the
     * data we need. */
    SPLIT,

    /** Multi-data center queries where each source _should_ have the same data
     * and we query both in case one or the other have problems. */
    HA,

    /** A mode wherein we query across multiple instances that contain subsets of
     * the data. */
    SHARD
  }

  /** The mode. */
  private final MergeMode mode;

  /** The data source we'll send up. */
  private final String data_source;
  
  /** The raw aggregator. */
  private final String aggregator;
  private final int aggregatorArraySize;
  private final TimeStamp firstDataTimestamp;
  private final String aggregatorInterval;
  
  /** Whether or not NaNs are infectious. */
  private final boolean infectious_nan;

  /** Whether or not to allow partial results from split and HA queries if one
   * result has an error. */
  private final boolean allowPartialResults;

  /** Sorted on most recent data to latest or primary and secondary, tertiary, etc */
  private final List<String> sortedDataSources;

  /** matches the sorted sources. */
  private final List<String> timeouts;

  protected MergerConfig(final Builder builder) {
    super(builder);
    if (builder.mode == null) {
      throw new IllegalArgumentException("Mode cannot be null.");
    }
    if (Strings.isNullOrEmpty(builder.aggregator) && builder.mode != MergeMode.SHARD) {
      throw new IllegalArgumentException("Aggregator cannot be null or empty.");
    }
    if (Strings.isNullOrEmpty(builder.dataSource)) {
      throw new IllegalArgumentException("Data source cannot be null or empty.");
    }
    mode = builder.mode;
    data_source = builder.dataSource;
    aggregator = builder.aggregator;
    aggregatorArraySize = builder.aggregatorArraySize;
    firstDataTimestamp = builder.firstDataTimestamp;
    aggregatorInterval = builder.aggregatorInterval;
    sortedDataSources = builder.sortedDataSources;
    timeouts = builder.timeouts;
    infectious_nan = builder.infectious_nan;
    allowPartialResults = builder.allowPartialResults;
    result_ids = Lists.newArrayList(
        new DefaultQueryResultId(id, data_source));
  }

  /** @return The non-null merge mode. */
  public MergeMode getMode() {
    return mode;
  }

  /** @return The non-null data source. */
  public String getDataSource() {
    return data_source;
  }

  /** @return The aggregation name. May be null or empty if the mode is set to
   * SHARD. May not be null or empty for other nodes. */
  public String getAggregator() {
    return aggregator;
  }

  public int getAggregatorArraySize() {
    return aggregatorArraySize;
  }
  
  /** @return Whether or not NaNs should be treated as sentinels or considered 
   * in arithmetic. */
  public boolean getInfectiousNan() {
    return infectious_nan;
  }

  public List<String> sortedSources() {
    return sortedDataSources;
  }

  public List<String> timeouts() {
    return timeouts;
  }

  public TimeStamp firstDataTimestamp() {
    return firstDataTimestamp;
  }

  public String aggregatorInterval() {
    return aggregatorInterval;
  }

  public boolean getAllowPartialResults() {
    return allowPartialResults;
  }

  @Override
  public Builder toBuilder() {
    final Builder builder = new Builder()
        .setMode(mode)
        .setDataSource(data_source)
        .setAggregator(aggregator)
        .setAggregatorArraySize(aggregatorArraySize)
        .setAggregatorInterval(aggregatorInterval)
        .setFirstDataTimestamp(firstDataTimestamp)
        .setSortedDataSources(sortedDataSources)
        .setTimeouts(timeouts)
        .setInfectiousNan(infectious_nan)
        .setAllowPartialResults(allowPartialResults);
    super.toBuilder(builder);
    return builder;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    // is this necessary?
    if (!super.equals(o)) {
      return false;
    }

    final MergerConfig merger = (MergerConfig) o;

    return Objects.equal(mode, merger.mode) &&
           Objects.equal(id, merger.getId()) &&
           Objects.equal(aggregator, merger.getAggregator()) &&
           Objects.equal(aggregatorArraySize, merger.getAggregatorArraySize()) &&
           Objects.equal(infectious_nan, merger.getInfectiousNan() &&
           Objects.equal(sortedDataSources, merger.sortedSources()) &&
           Objects.equal(timeouts, merger.timeouts())) &&
           Objects.equal(allowPartialResults, merger.getAllowPartialResults());

  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  @Override
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
            .putInt(mode.ordinal())
            .putString(Strings.nullToEmpty(aggregator), Const.UTF8_CHARSET)
            .putInt(aggregatorArraySize)
            .putBoolean(infectious_nan)
            .putBoolean(allowPartialResults)
            .hash();
    final List<HashCode> hashes =
            Lists.newArrayListWithCapacity(2);

    hashes.add(super.buildHashCode());
    hashes.add(hc);

    return Hashing.combineOrdered(hashes);
  }
  
  @Override
  public boolean pushDown() {
    return false;
  }

  @Override
  public boolean joins() {
    return true;
  }

  @Override
  public int compareTo(MergerConfig o) {
    throw new UnsupportedOperationException();
  }

  public static MergerConfig parse(final ObjectMapper mapper,
                                   final TSDB tsdb,
                                   final JsonNode node) {
    Builder builder = new Builder();
    parse(builder, mapper, tsdb, node);

    JsonNode temp = node.get("mode");
    if (temp != null && !temp.isNull()) {
      builder.setMode(MergeMode.valueOf(temp.asText()));
    }

    temp = node.get("aggregator");
    if (temp != null && !temp.isNull()) {
      builder.setAggregator(temp.asText());
    }

    temp = node.get("aggregatorInterval");
    if (temp != null && !temp.isNull()) {
      builder.setAggregatorArraySize(temp.asInt());
    }

    temp = node.get("dataSource");
    if (temp != null && !temp.isNull()) {
      builder.setDataSource(temp.asText());
    }

    temp = node.get("infectiousNan");
    if (temp != null && !temp.isNull()) {
      builder.setInfectiousNan(temp.asBoolean());
    }

    temp = node.get("allowPartialResults");
    if (temp != null && !temp.isNull()) {
      builder.setAllowPartialResults(temp.asBoolean());
    }

    return builder.build();
  }

  /** @return A new builder to work from. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseQueryNodeConfigWithInterpolators.Builder<Builder, MergerConfig> {
    @JsonProperty
    private MergeMode mode;
    @JsonProperty
    private String dataSource;
    @JsonProperty
    private String aggregator;
    @JsonProperty
    private boolean infectious_nan;
    @JsonProperty
    private boolean allowPartialResults;
    private int aggregatorArraySize;
    private String aggregatorInterval;
    private TimeStamp firstDataTimestamp;
    private List<String> sortedDataSources;
    private List<String> timeouts;
    
    Builder() {
      setType(MergerFactory.TYPE);
    }

    public Builder setMode(final MergeMode mode) {
      this.mode = mode;
      return this;
    }

    public Builder setDataSource(final String data_source) {
      dataSource = data_source;
      return this;
    }
    
    /**
     * @param aggregator A non-null and non-empty aggregation function.
     * @return The builder.
     */
    public Builder setAggregator(final String aggregator) {
      this.aggregator = aggregator;
      return this;
    }
    
    /**
     * @param infectious_nan Whether or not NaNs should be sentinels or included
     * in arithmetic.
     * @return The builder.
     */
    public Builder setInfectiousNan(final boolean infectious_nan) {
      this.infectious_nan = infectious_nan;
      return this;
    }

    public Builder setSortedDataSources(final List<String> sortedDataSources) {
      this.sortedDataSources = sortedDataSources;
      return this;
    }

    public Builder setTimeouts(final List<String> timeouts) {
      this.timeouts = timeouts;
      return this;
    }

    public Builder setAllowPartialResults(final boolean allowPartialResults) {
      this.allowPartialResults = allowPartialResults;
      return this;
    }

    public Builder setAggregatorArraySize(final int aggregatorArraySize) {
      this.aggregatorArraySize = aggregatorArraySize;
      return this;
    }

    public Builder setFirstDataTimestamp(final TimeStamp firstDataTimestamp) {
      this.firstDataTimestamp = firstDataTimestamp;
      return this;
    }

    public Builder setAggregatorInterval(final String aggregatorInterval) {
      this.aggregatorInterval = aggregatorInterval;
      return this;
    }

    @Override
    public MergerConfig build() {
      return new MergerConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }
  }
}
