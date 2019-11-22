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
package net.opentsdb.data.types.alert;

import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.types.numeric.NumericType;

/**
 * A class denoting information surrounding an alert or issue with a time series.
 * 
 * TODO - make it generic so it can take any kidn of data point instead of just
 * NumericType.
 * 
 * @since 3.0
 */
public interface AlertType extends TimeSeriesDataType<AlertType>{
  
  public static final TypeToken<AlertType> TYPE = TypeToken.of(AlertType.class);
  
  public static final Set<TypeToken<? extends TimeSeriesDataType>> TYPES = 
      Sets.newHashSet(TYPE);
  
  /**
   * 
   * The state of an alert.
   * TODO - revisit these.
   */
  public static enum State {
    BAD,
    WARN,
    OK,
    RECOVER
  }
  
  /** @return The non-null state of the alert. */
  public State state();
  
  /** @return An optional message surrounding the alert. */
  public String message();
  
  /** @return The data point that triggered an alert. */
  public NumericType dataPoint();
  
  /** @return The expected or threshold value the data point exceeded. */
  public NumericType threshold();
  
  /** @return An optional message about the type of threshold exceeded, e.g.
   * upper, lower, anomaly. */
  public String thresholdType();
}