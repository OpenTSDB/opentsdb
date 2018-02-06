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
package net.opentsdb.query.interpolation.types.numeric;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.query.QueryIteratorInterpolator;
import net.opentsdb.query.QueryIteratorInterpolatorConfig;
import net.opentsdb.query.QueryIteratorInterpolatorFactory;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.pojo.FillPolicy;

/**
 * Default numeric interpolator factories.
 * 
 * TODO - add more and register them.
 * 
 * @since 3.0
 */
public class NumericInterpolatorFactory {

  /**
   * The default factory that builds interpolators from the given config.
   */
  public static class Default implements QueryIteratorInterpolatorFactory {
    @Override
    public QueryIteratorInterpolator<? extends TimeSeriesDataType> newInterpolator(
        final TypeToken<? extends TimeSeriesDataType> type,
        final TimeSeries source, 
        final QueryIteratorInterpolatorConfig config) {
      if (config == null) {
        throw new IllegalArgumentException("Config cannot be null.");
      }
      if (!(config instanceof NumericInterpolatorConfig)) {
        throw new IllegalArgumentException("Config was not of the type "
            + "NumericInterpolatorConfig: " + config.getClass());
      }
      return new NumericInterpolator(source, 
          (NumericInterpolatorConfig) config);
    }
  }
  
  /**
   * Parses an older style OpenTSDB aggregator with fill in the format:
   * <code>
   * #unit-aggregator-fill
   * 6h-pfsum-nan
   * 6h-pfsum
   * </code>
   * @param param A non-null and non-empty parameter.
   * @return An interpolator config with the defaults being none.
   */
  public static NumericInterpolatorConfig parse(final String param) {
    if (Strings.isNullOrEmpty(param)) {
      throw new IllegalArgumentException("Param cannot be null or empty.");
    }
    
    final String aggregator;
    final String fill;
    final String[] parts = param.toLowerCase().split("-");
    if (parts.length == 3) {
      aggregator = parts[1];
      fill = parts[2];
    } else if (parts.length == 2) {
      aggregator = parts[1];
      fill = null;
    } else {
      return NumericInterpolatorConfig.newBuilder()
          .setFillPolicy(FillPolicy.NONE)
          .setRealFillPolicy(FillWithRealPolicy.NONE)
          .build();
    }
    
    final FillPolicy fill_policy;
    if (Strings.isNullOrEmpty(fill) && aggregator.equals("zimsum")) {
      fill_policy = FillPolicy.ZERO;
    } else if (Strings.isNullOrEmpty(fill)) {
      fill_policy = FillPolicy.NONE;
    } else {
      fill_policy = FillPolicy.fromString(fill);
    }
    
    final FillWithRealPolicy real_policy;
    // TODO - old version only had pfsum, may need more.
    if (aggregator.equals("pfsum")) {
      real_policy = FillWithRealPolicy.PREFER_PREVIOUS;
    } else {
      real_policy = FillWithRealPolicy.NONE;
    }
    return NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(fill_policy)
        .setRealFillPolicy(real_policy)
        .build();
  }
}
