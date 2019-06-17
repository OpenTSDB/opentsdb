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
package net.opentsdb.data.types.numeric.aggregators;

/**
 * Config for exponential moving average. Alpha must be > 0 and < 1.
 * 
 * @since 3.0
 */
public class ExponentialWeightedMovingAverageConfig implements 
    NumericAggregatorConfig {

  /** The alpha. */
  private final double alpha;
  
  /** Whether or not to average the values to get the initialization value. */
  private final boolean avg_initial;
  
  protected ExponentialWeightedMovingAverageConfig(final Builder builder) {
    if (builder.alpha < 0 || builder.alpha >= 1) {
      throw new IllegalArgumentException("Alpha must be a value greater than "
          + " or equal to zero (auto mode) and less than 1.");
    }
    alpha = builder.alpha;
    avg_initial = builder.avg_initial;
  }
  
  @Override
  public boolean infectiousNan() {
    // always false for now.
    return false;
  }

  /** @return The alpha. If 0, use the default or calculate one. */
  public double alpha() {
    return alpha;
  }
  
  /** @return Whether or not to average the values for the initial. */
  public boolean averageInitial() {
    return avg_initial;
  }
  
  /** @return A new builder instance. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private double alpha;
    private boolean avg_initial;
    
    public Builder setAlpha(final double alpha) {
      this.alpha = alpha;
      return this;
    }
    
    public Builder setAverageInitial(final boolean avg_initial) {
      this.avg_initial = avg_initial;
      return this;
    }
    
    public ExponentialWeightedMovingAverageConfig build() {
      return new ExponentialWeightedMovingAverageConfig(this);
    }
  }
}
