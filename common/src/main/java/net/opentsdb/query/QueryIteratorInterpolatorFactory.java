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
package net.opentsdb.query;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;

/**
 * A factory interface for generating interator interpolators.
 * 
 * @since 3.0
 */
public interface QueryIteratorInterpolatorFactory {

  /**
   * Returns a new interpolator for the given data type if present.
   * @param type A non-null data type for the interpolator to work on.
   * @param source A non-null time series source.
   * @param config An optional config for the interpolator.
   * @return An instantiated interpolator or null if an implementation does not
   * exist for the requested type. In such an event, the series should log and 
   * be dropped.
   */
  public QueryIteratorInterpolator<? extends TimeSeriesDataType> newInterpolator(
      final TypeToken<? extends TimeSeriesDataType> type, 
      final TimeSeries source, 
      final QueryIteratorInterpolatorConfig config);
}
