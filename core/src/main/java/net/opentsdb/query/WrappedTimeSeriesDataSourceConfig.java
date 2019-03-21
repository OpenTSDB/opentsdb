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
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.hash.HashCode;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.utils.Pair;

/**
 * A simple wrapper around a config that allows for changing the ID.
 * 
 * @since 3.0
 */
public class WrappedTimeSeriesDataSourceConfig implements 
    TimeSeriesDataSourceConfig {
  
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
  public List<String> getSources() {
    return config.getSources();
  }

  @Override
  public HashCode buildHashCode() {
    // TODO - implement properly with new ID
    return config.buildHashCode();
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
  public net.opentsdb.query.QueryNodeConfig.Builder toBuilder() {
    return config.toBuilder()
        .setId(id);
  }

  @Override
  public int compareTo(QueryNodeConfig o) {
    throw new UnsupportedOperationException("Not implemented yet");
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
  public int getPreviousIntervals() {
    return config.getPreviousIntervals();
  }

  @Override
  public int getNextIntervals() {
    return config.getNextIntervals();
  }

  @Override
  public boolean hasBeenSetup() {
    return has_been_setup;
  }
  
  @Override
  public Map<String, Pair<Boolean, TemporalAmount>> timeShifts() {
    return config.timeShifts();
  }

}
