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
package net.opentsdb.data.types.numeric;

import java.util.Collection;

import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

/**
 * The basic class to handle filling numeric summaries.
 * 
 * @3.0
 */
public class BaseNumericSummaryFillPolicy implements 
    QueryFillPolicy<NumericSummaryType>, 
    NumericSummaryType {

  /** The config this fill policy is using. */
  protected final NumericSummaryInterpolatorConfig config;
  
  /** The value filled when {@link #fill()} is called. */
  protected final MutableNumericSummaryType value;
  
  /** Whether or not the value has been nulled. */
  protected boolean nulled;
  
  /**
   * Default ctor.
   * @param config A non-null config.
   * @throws IllegalArgumentException if the config was null.
   */
  public BaseNumericSummaryFillPolicy(final QueryInterpolatorConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    this.config = (NumericSummaryInterpolatorConfig) config;
    value = new MutableNumericSummaryType();
    nulled = true;
  }

  @Override
  public Collection<Integer> summariesAvailable() {
    return nulled ? null : value.summariesAvailable();
  }

  @Override
  public NumericType value(int summary) {
    return nulled ? null : value.value(summary);
  }

  @Override
  public NumericSummaryType fill() {
    // re-do fill
    value.clear();
    for (final int summary : config.expectedSummaries()) {
      FillPolicy fill = config.fillPolicy(summary);
      if (fill == null) {
        fill = config.defaultFillPolicy();
      }
      switch(fill) {
      case NONE:
      case NULL:
        nulled = true;
        break;
      case ZERO:
        value.set(summary, 0L);
        nulled = false;
        break;
      case NOT_A_NUMBER:
        value.set(summary, Double.NaN);
        nulled = false;
        break;
      case MIN:
        value.set(summary, Double.MIN_VALUE);
        nulled = false;
        break;
      case MAX:
        value.set(summary, Double.MAX_VALUE);
        nulled = false;
        break;
      default:
        throw new UnsupportedOperationException("Fill type: " + fill 
            + " is not supported at this time.");
      }
    }
    return nulled ? null : this;
  }

  @Override
  public FillWithRealPolicy realPolicy() {
    return config.defaultRealFillPolicy();
  }
  
  public FillWithRealPolicy realPolicy(final int summary) {
    return config.realFillPolicy(summary);
  }

  @Override
  public QueryInterpolatorConfig config() {
    return config;
  }
}