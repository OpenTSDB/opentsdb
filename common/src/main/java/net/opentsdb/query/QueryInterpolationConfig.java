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
package net.opentsdb.query;

import java.util.Iterator;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;

/**
 * A query configuration that determines how to interpolate values for
 * various data types.
 * 
 * @since 3.0
 */
public interface QueryInterpolationConfig {

  /**
   * Returns an interpolator config for the specific data type.
   * @param type A non-null data type.
   * @return A config for an interpolator of the given type if present,
   * null if the data cannot be interpolated or was not configured.
   */
  public QueryInterpolatorConfig config(
      final TypeToken<? extends TimeSeriesDataType> type);
  
  /**
   * Return an interpolator for the given data type with a config override.
   * @param type A non-null data type.
   * @param source A non-null data source.
   * @param config A non-null config for the interpolator.
   * @return An instantiated interpolator if the factory was able to 
   * return one for the given data type.
   */
  public QueryInterpolator<? extends TimeSeriesDataType> newInterpolator(
      final TypeToken<? extends TimeSeriesDataType> type, 
      final TimeSeries source, 
      final QueryInterpolatorConfig config);
  
  /**
   * Return an interpolator for the given data type using the proper
   * type configuration.
   * @param type A non-null data type.
   * @param source A non-null data source.
   * @return An instantiated interpolator if the factory was able to 
   * return one for the given data type.
   */
  public QueryInterpolator<? extends TimeSeriesDataType> newInterpolator(
      final TypeToken<? extends TimeSeriesDataType> type, 
      final TimeSeries source);
  
  /**
   * Return an interpolator for the given data type with a config override.
   * @param type A non-null data type.
   * @param iterator A non-null data source.
   * @param config A non-null config for the interpolator.
   * @return An instantiated interpolator if the factory was able to 
   * return one for the given data type.
   */
  public QueryInterpolator<? extends TimeSeriesDataType> newInterpolator(
      final TypeToken<? extends TimeSeriesDataType> type, 
      final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator, 
      final QueryInterpolatorConfig config);
  
  /**
   * Return an interpolator for the given data type using the proper
   * type configuration.
   * @param type A non-null data type.
   * @param iterator A non-null data source.
   * @return An instantiated interpolator if the factory was able to 
   * return one for the given data type.
   */
  public QueryInterpolator<? extends TimeSeriesDataType> newInterpolator(
      final TypeToken<? extends TimeSeriesDataType> type, 
      final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator);
}