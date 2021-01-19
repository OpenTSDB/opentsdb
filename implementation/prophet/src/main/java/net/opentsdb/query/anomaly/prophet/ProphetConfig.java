// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.query.anomaly.prophet;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.anomaly.BaseAnomalyConfig;

/**
 * The config mapping to Prophet execution configs.
 * 
 * TODO - handle seasonality settings
 * TODO - handle holiday settings
 * TODO - Timezone support
 * 
 * @since 3.0
 */
public class ProphetConfig extends BaseAnomalyConfig {

  /**
   * The growth expected. Python code has "flat" but that isn't mentioned 
   * elsewhere. May need to drop it.
   */
  public static enum Growth {
    LINEAR,
    LOGISTIC,
    FLAT
  }
  
  /**
   * TODO - doc this.
   */
  public static enum Seasonality {
    ADDITIVE,
    MULTIPLICATIVE
  }
  
  private Growth growth;
  //private List<Object> changepoints; // TODO - list of CP dates
  private int number_of_change_points;
  private double change_point_range;
  private Boolean yearly_seasonality; // null == auto, true or false
  private Boolean weekly_seasonality;
  private Boolean daily_seasonality;
  //private List<Object> holidays; // TODO
  private Seasonality seasonality_mode;
  private double seasonality_prior_scale;
  private double holidays_prior_scale;
  private double change_point_prior_scale;
  private int mcmc_samples;
  private double uncertainty_interval_width;
  private int uncertainty_samples;
  
  protected ProphetConfig(final Builder builder) {
    super(builder);
    // TODO - validations
    growth = builder.growth;
    number_of_change_points = builder.number_of_change_points;
    change_point_range = builder.change_point_range;
    yearly_seasonality = builder.yearly_seasonality;
    weekly_seasonality = builder.weekly_seasonality;
    daily_seasonality = builder.daily_seasonality;
    seasonality_mode = builder.seasonality_mode;
    seasonality_prior_scale = builder.seasonality_prior_scale;
    holidays_prior_scale = builder.holidays_prior_scale;
    change_point_prior_scale = builder.change_point_prior_scale;
    mcmc_samples = builder.mcmc_samples;
    uncertainty_interval_width = builder.uncertainty_interval_width;
    uncertainty_samples = builder.uncertainty_samples;
  }
  
  public Growth getGrowth() {
    return growth;
  }
  
  public int getNumberOfChangePoints() {
    return number_of_change_points;
  }
  
  public double getChangePointRange() {
    return change_point_range;
  }
  
  public Boolean getYearlySeasonality() {
    return yearly_seasonality;
  }
  
  public Boolean getWeeklySeasonality() {
    return weekly_seasonality;
  }
  
  public Boolean getDailySeasonality() {
    return daily_seasonality;
  }
  
  public Seasonality getSeasonality() {
    return seasonality_mode;
  }
  
  public double getSeasonalityPriorScale() {
    return seasonality_prior_scale;
  }
  
  public double getHolidaysPriorScale() {
    return holidays_prior_scale;
  }
  
  public double getChangePointPriorScale() {
    return change_point_prior_scale;
  }
  
  public int getMcmcSamples() {
    return mcmc_samples;
  }
  
  public double getUncertaintyIntervalWidth() {
    return uncertainty_interval_width;
  }
  
  public int getUncertaintySamples() {
    return uncertainty_samples;
  }

  @Override
  public boolean pushDown() {
    return false;
  }

  @Override
  public boolean joins() {
    return false;
  }

  @Override
  public Builder toBuilder() {
    final Builder builder = new Builder()
        .setGrowth(growth)
        .setNumberOfChangePoints(number_of_change_points)
        .setChangePointRange(change_point_range)
        .setYearlySeasonality(yearly_seasonality)
        .setWeeklySeasonality(weekly_seasonality)
        .setDailySeasonality(daily_seasonality)
        .setSeasonalityMode(seasonality_mode)
        .setSeasonalityPriorScale(seasonality_prior_scale)
        .setHolidaysPriorScale(holidays_prior_scale)
        .setChangePointPriorScale(change_point_prior_scale)
        .setMCMCSamples(mcmc_samples)
        .setUncertaintyIntervalWidth(uncertainty_interval_width)
        .setUncertaintySamples(uncertainty_samples);
    super.toBuilder(builder);
    return builder;
  }

  @Override
  public int compareTo(Object o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    
    if (!(o instanceof ProphetConfig)) {
      return false;
    }
    
    final ProphetConfig config = (ProphetConfig) o;
    return Objects.equals(growth, config.growth) &&
           Objects.equals(number_of_change_points, config.number_of_change_points) &&
           Objects.equals(change_point_range, config.change_point_range) &&
           Objects.equals(yearly_seasonality, config.yearly_seasonality) &&
           Objects.equals(weekly_seasonality, config.weekly_seasonality) &&
           Objects.equals(daily_seasonality, config.daily_seasonality) &&
           Objects.equals(seasonality_mode, config.seasonality_mode) &&
           Objects.equals(seasonality_prior_scale, config.seasonality_prior_scale) &&
           Objects.equals(holidays_prior_scale, config.holidays_prior_scale) &&
           Objects.equals(change_point_prior_scale, config.change_point_prior_scale) &&
           Objects.equals(mcmc_samples, config.mcmc_samples) &&
           Objects.equals(uncertainty_interval_width, config.uncertainty_interval_width) &&
           Objects.equals(uncertainty_samples, config.uncertainty_samples) &&
           super.equals(config);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  @Override
  public HashCode buildHashCode() {
    Hasher hasher = Const.HASH_FUNCTION().newHasher()
        .putInt(growth.ordinal())
        .putInt(number_of_change_points)
        .putDouble(change_point_range)
        .putInt(seasonality_mode.ordinal())
        .putDouble(seasonality_prior_scale)
        .putDouble(holidays_prior_scale)
        .putDouble(change_point_prior_scale)
        .putInt(mcmc_samples)
        .putDouble(uncertainty_interval_width)
        .putInt(uncertainty_samples);
    if (yearly_seasonality != null) {
      hasher.putBoolean(yearly_seasonality);
    }
    if (weekly_seasonality != null) {
      hasher.putBoolean(weekly_seasonality);
    }
    if (daily_seasonality != null) {
      hasher.putBoolean(daily_seasonality);
    }
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(2);
    hashes.add(hasher.hash());
    hashes.add(super.buildHashCode());
    return Hashing.combineOrdered(hashes);
  }
  
  public static ProphetConfig parse(final ObjectMapper mapper,
                                    final TSDB tsdb,
                                    final JsonNode node) {
    Builder builder = new Builder();
    JsonNode n = node.get("growth");
    if (n != null && !n.isNull()) {
      builder.setGrowth(Growth.valueOf(n.asText()));
    }
    
    n = node.get("numberOfChangePoints");
    if (n != null && !n.isNull()) {
      builder.setNumberOfChangePoints(n.asInt());
    }
    
    n = node.get("changePointRange");
    if (n != null && !n.isNull()) {
      builder.setChangePointRange(n.asDouble());
    }
    
    n = node.get("yearlySeasonality");
    if (n != null && !n.isNull()) {
      builder.setYearlySeasonality(n.asBoolean());
    }
    
    n = node.get("weeklySeasonality");
    if (n != null && !n.isNull()) {
      builder.setWeeklySeasonality(n.asBoolean());
    }
    
    n = node.get("dailySeasonality");
    if (n != null && !n.isNull()) {
      builder.setDailySeasonality(n.asBoolean());
    }
    
    n = node.get("seasonalityMode");
    if (n != null && !n.isNull()) {
      builder.setSeasonalityMode(Seasonality.valueOf(n.asText()));
    }
    
    n = node.get("seasonalityPriorScale");
    if (n != null && !n.isNull()) {
      builder.setSeasonalityPriorScale(n.asDouble());
    }
    
    n = node.get("holidaysPriorScale");
    if (n != null && !n.isNull()) {
      builder.setHolidaysPriorScale(n.asDouble());
    }
    
    n = node.get("changePointScale");
    if (n != null && !n.isNull()) {
      builder.setChangePointPriorScale(n.asDouble());
    }
    
    n = node.get("MCMCSamples");
    if (n != null && !n.isNull()) {
      builder.setMCMCSamples(n.asInt());
    }
    
    n = node.get("uncertaintyIntervalWidth");
    if (n != null && !n.isNull()) {
      builder.setUncertaintyIntervalWidth(n.asDouble());
    }
    
    n = node.get("uncertaintySamples");
    if (n != null && !n.isNull()) {
      builder.setUncertaintySamples(n.asInt());
    }
    
    BaseAnomalyConfig.Builder.parseConfig(mapper, tsdb, node, builder);
    return builder.build();
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseAnomalyConfig.Builder<
      Builder, ProphetConfig> {

    private Growth growth = Growth.LINEAR;
    //private List<Object> changepoints; // TODO - list of CP dates
    private int number_of_change_points = 25;
    private double change_point_range;
    private Boolean yearly_seasonality; // null == auto, true or false
    private Boolean weekly_seasonality;
    private Boolean daily_seasonality;
    //private List<Object> holidays; // TODO
    private Seasonality seasonality_mode = Seasonality.ADDITIVE;
    private double seasonality_prior_scale = 10;
    private double holidays_prior_scale = 10;
    private double change_point_prior_scale = 0.05;
    private int mcmc_samples;
    private double uncertainty_interval_width = 0.8;
    private int uncertainty_samples = 1000;
    
    Builder() {
      setType(ProphetFactory.TYPE);
    }
    
    public Builder setGrowth(final Growth growth) {
      this.growth = growth;
      return this;
    }
    
    public Builder setNumberOfChangePoints(final int number_of_changepoints) {
      this.number_of_change_points = number_of_changepoints;
      return this;
    }
    
    public Builder setChangePointRange(final double change_point_range) {
      this.change_point_range = change_point_range;
      return this;
    }
    
    public Builder setYearlySeasonality(final Boolean yearly_seasonality) {
      this.yearly_seasonality = yearly_seasonality;
      return this;
    }
    
    public Builder setWeeklySeasonality(final Boolean weekly_seasonality) {
      this.weekly_seasonality = weekly_seasonality;
      return this;
    }
    
    public Builder setDailySeasonality(final Boolean daily_seasonality) {
      this.daily_seasonality = daily_seasonality;
      return this;
    }
    
    public Builder setSeasonalityMode(final Seasonality seasonality_mode) {
      this.seasonality_mode = seasonality_mode;
      return this;
    }
    
    public Builder setSeasonalityPriorScale(final double seasonality_prior_scale) {
      this.seasonality_prior_scale = seasonality_prior_scale;
      return this;
    }
    
    public Builder setHolidaysPriorScale(final double holidays_prior_scale) {
      this.holidays_prior_scale = holidays_prior_scale;
      return this;
    }
    
    public Builder setChangePointPriorScale(final double change_point_prior_scale) {
      this.change_point_prior_scale = change_point_prior_scale;
      return this;
    }
    
    public Builder setMCMCSamples(final int mcmc_samples) {
      this.mcmc_samples = mcmc_samples;
      return this;
    }
    
    public Builder setUncertaintyIntervalWidth(final double uncertainty_interval_width) {
      this.uncertainty_interval_width = mcmc_samples;
      return this;
    }
    
    public Builder setUncertaintySamples(final int uncertainty_samples) {
      this.uncertainty_samples = uncertainty_samples;
      return this;
    }
    
    @Override
    public ProphetConfig build() {
      return new ProphetConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }
    
  }
}
