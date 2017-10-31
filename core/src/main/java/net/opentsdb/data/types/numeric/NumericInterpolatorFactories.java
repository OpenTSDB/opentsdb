// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.data.types.numeric;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.query.QueryIteratorInterpolator;
import net.opentsdb.query.QueryIteratorInterpolatorConfig;
import net.opentsdb.query.QueryIteratorInterpolatorFactory;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.NumericInterpolator;

/**
 * Default numeric interpolator factories.
 * 
 * TODO - add more and register them.
 * 
 * @since 3.0
 */
public class NumericInterpolatorFactories {

  /**
   * A non-interpolating interpolator. Returns nulls in all instances, ignoring
   * reals.
   */
  public static class Null implements QueryIteratorInterpolatorFactory {
    @Override
    public QueryIteratorInterpolator<? extends TimeSeriesDataType> newInterpolator(
        final TypeToken<? extends TimeSeriesDataType> type,
        final TimeSeries source, 
        final QueryIteratorInterpolatorConfig config) {
      return new NumericInterpolator(source, 
          new BaseNumericFillPolicy(FillPolicy.NULL), FillWithRealPolicy.NONE);
    }
  }
  
  /**
   * An interpolator that returns NaNs when not present and ignores reals.
   */
  public static class NaN implements QueryIteratorInterpolatorFactory {

    @Override
    public QueryIteratorInterpolator<? extends TimeSeriesDataType> newInterpolator(
        final TypeToken<? extends TimeSeriesDataType> type,
        final TimeSeries source, 
        final QueryIteratorInterpolatorConfig config) {
      return new NumericInterpolator(source, 
          new BaseNumericFillPolicy(FillPolicy.NOT_A_NUMBER), FillWithRealPolicy.NONE);
    }
    
  }
  
  /**
   * An interpolator returning a scalar value when not present and ignores
   * reals. Requires a config.
   */
  public static class Scalar implements QueryIteratorInterpolatorFactory {

    @Override
    public QueryIteratorInterpolator<? extends TimeSeriesDataType> newInterpolator(
        final TypeToken<? extends TimeSeriesDataType> type,
        final TimeSeries source, 
        final QueryIteratorInterpolatorConfig config) {
      if (config == null) {
        throw new IllegalArgumentException("Config cannot be null.");
      }
      if (!(config instanceof NumericInterpolator.Config)) {
        throw new IllegalArgumentException("Wrong type of config!");
      }
      return new NumericInterpolator(source, 
          new ScalarNumericFillPolicy(((NumericInterpolator.Config) config).scalar), 
          FillWithRealPolicy.NONE);
    }
    
  }

}
