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
package net.opentsdb.query.interpolation;

import java.util.Iterator;

import com.google.common.reflect.TypeToken;

import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;

/**
 * A factory interface for generating interator interpolators.
 * 
 * @since 3.0
 */
public interface QueryInterpolatorFactory extends TSDBPlugin {

  /**
   * Returns a new interpolator for the given data type if present.
   * @param type A non-null data type for the interpolator to work on.
   * @param source A non-null time series source.
   * @param config An optional config for the interpolator.
   * @return An instantiated interpolator or null if an implementation does not
   * exist for the requested type. In such an event, the series should log and 
   * be dropped.
   */
  public QueryInterpolator<? extends TimeSeriesDataType> newInterpolator(
      final TypeToken<? extends TimeSeriesDataType> type, 
      final TimeSeries source, 
      final QueryInterpolatorConfig config);

  /**
   * Returns a new interpolator for the given data type if present.
   * @param type A non-null data type for the interpolator to work on.
   * @param iterator A non-null time series source.
   * @param config An optional config for the interpolator.
   * @return An instantiated interpolator or null if an implementation does not
   * exist for the requested type. In such an event, the series should log and 
   * be dropped.
   */
  public QueryInterpolator<? extends TimeSeriesDataType> newInterpolator(
      final TypeToken<? extends TimeSeriesDataType> type, 
      final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator, 
      final QueryInterpolatorConfig config);

  /**
   * Called to register an interpolator for the given data type and ID
   * of the factory.
   * @param type A non-null type.
   * @param clazz A non-null class with a constructor accepting the 
   * following parameters: "TimeSeries, QueryInterpolatorConfig" or 
   * "Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>, QueryInterpolatorConfig".
   */
  public void register(final TypeToken<? extends TimeSeriesDataType> type,
                       final Class<? extends QueryInterpolator<?>> clazz); 
}