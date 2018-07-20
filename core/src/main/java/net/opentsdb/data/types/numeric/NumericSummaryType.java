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

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.rollup.DefaultRollupConfig;

/**
 * A summary of numeric data, e.g. a time interval rollup (downsample)
 * or a pre-aggregation of some data. E.g this type can contain
 * a number of sumaries for the timestamp such as the SUM and COUNT of
 * values or the MAX, MIN, AVG and others.
 * 
 * TODO - For now these summary integer IDs map to those found in the 
 * {@link DefaultRollupConfig#getAggregationIds()} map. Eventually we should 
 * have it in a better, shareable location.
 * 
 * @since 3.0
 */
public interface NumericSummaryType extends TimeSeriesDataType {
  public static final TypeToken<NumericSummaryType> TYPE = 
      TypeToken.of(NumericSummaryType.class);
  
  @Override
  default TypeToken<? extends TimeSeriesDataType> type() {
    return TYPE;
  }
  
  /**
   * The summaries available for this data point. If no values are 
   * available, the result is an empty, non-null collection.
   * @return A non-null collection of zero or more summaries available.
   */
  public Collection<Integer> summariesAvailable();
  
  /**
   * Fetches the value for the given summary type.
   * @param summary A summary ID.
   * @return A value for the given summary or null if the summary did
   * not exist or did not have data for this value.
   */
  public NumericType value(final int summary);
  
  
}