// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query;

import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import com.google.common.hash.Hashing;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.Const;
import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.utils.Comparators;
import net.opentsdb.utils.Pair;

/**
 * A simple wrapper around a config that allows for changing the ID.
 * 
 * @since 3.0
 */
public class WrappedTimeSeriesDataSourceConfig implements TimeSeriesDataSourceConfig {
  
  /** The non-null and non-empty ID. */
  private final String id;
  
  /** The source config. */
  private final TimeSeriesDataSourceConfig config;
  
  /** Whether or not we've been setup. */
  public final boolean has_been_setup;
  
  /**
   * Default ctor.
   * @param id A non-null and non-empty ID.
   * @param config A non-null config.
   */
  public WrappedTimeSeriesDataSourceConfig(
      final String id, 
      final TimeSeriesDataSourceConfig config,
      final boolean has_been_setup) {
    if (Strings.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    this.id = id;
    this.config = config;
    this.has_been_setup = has_been_setup;
  }
  
  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getType() {
    return config.getType();
  }

  @Override
  public String getDataSourceId() {
    return config.getDataSourceId();
  }
  
  @Override
  public List<String> getSources() {
    return config.getSources();
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

    final WrappedTimeSeriesDataSourceConfig wrappedconfig = (WrappedTimeSeriesDataSourceConfig) o;

    final boolean result = Objects.equal(id, wrappedconfig.getId())
            && Objects.equal(getNamespace(), wrappedconfig.getNamespace())
            && Objects.equal(getSourceId(), wrappedconfig.getSourceId())
            && Objects.equal(getFilterId(), wrappedconfig.getFilterId())
            && Objects.equal(getMetric(), wrappedconfig.getMetric())
            && Objects.equal(getFilter(), wrappedconfig.getFilter())
            && Objects.equal(getFetchLast(), wrappedconfig.getFetchLast())
            && Objects.equal(getTimeShiftInterval(), wrappedconfig.getTimeShiftInterval());

    if (!result) {
      return false;
    }

    // comparing types
    if (!Comparators.ListComparison.equalLists(getTypes(), wrappedconfig.getTypes())) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  @Override
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
            .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
            .hash();
    final List<HashCode> hashes =
            Lists.newArrayListWithCapacity(2);

    hashes.add(hc);

    hashes.add(config.buildHashCode());

    return Hashing.combineOrdered(hashes);
  }

  @Override
  public boolean pushDown() {
    return config.pushDown();
  }

  @Override
  public boolean joins() {
    return config.joins();
  }

  @Override
  public Map<String, String> getOverrides() {
    return config.getOverrides();
  }

  @Override
  public String getString(Configuration config, String key) {
    return this.config.getString(config, key);
  }

  @Override
  public int getInt(Configuration config, String key) {
    return this.config.getInt(config, key);
  }

  @Override
  public long getLong(Configuration config, String key) {
    return this.config.getLong(config, key);
  }

  @Override
  public boolean getBoolean(Configuration config, String key) {
    return this.config.getBoolean(config, key);
  }

  @Override
  public double getDouble(Configuration config, String key) {
    return this.config.getDouble(config, key);
  }

  @Override
  public boolean hasKey(String key) {
    return config.hasKey(key);
  }

  @Override
  public QueryNodeConfig.Builder toBuilder() {
    return config.toBuilder().setId(id);
  }

  @Override
  public String getSourceId() {
    return config.getSourceId();
  }

  @Override
  public List<String> getTypes() {
    return config.getTypes();
  }

  @Override
  public String getNamespace() {
    return config.getNamespace();
  }

  @Override
  public int getFrom() {
    return config.getFrom();
  }

  @Override
  public int getSize() {
    return config.getSize();
  }

  /** @return The non-null metric filter. */
  public MetricFilter getMetric() {
    return config.getMetric();
  }

  @Override
  public String getFilterId() {
    return config.getFilterId();
  }

  @Override
  public QueryFilter getFilter() {
    return config.getFilter();
  }

  @Override
  public boolean getFetchLast() {
    return config.getFetchLast();
  }

  @Override
  public List<QueryNodeConfig> getPushDownNodes() {
    return config.getPushDownNodes();
  }

  @Override
  public Collection<String> pushDownSinks() {
    return config.pushDownSinks();
  }
  
  @Override
  public String getSummaryInterval() {
    return config.getSummaryInterval();
  }

  @Override
  public List<String> getSummaryAggregations() {
    return config.getSummaryAggregations();
  }

  @Override
  public List<String> getRollupIntervals() {
    return config.getRollupIntervals();
  }

  @Override
  public String getPrePadding() {
    return config.getPrePadding();
  }

  @Override
  public String getPostPadding() {
    return config.getPostPadding();
  }

  @Override
  public String getTimeShiftInterval() {
    return config.getTimeShiftInterval();
  }

  @Override
  public boolean hasBeenSetup() {
    return has_been_setup;
  }
  
  @Override
  public Pair<Boolean, TemporalAmount> timeShifts() {
    return config.timeShifts();
  }

  @Override
  public int compareTo(Object o) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
