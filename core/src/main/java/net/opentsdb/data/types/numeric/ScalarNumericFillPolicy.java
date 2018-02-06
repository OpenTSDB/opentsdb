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
package net.opentsdb.data.types.numeric;

import net.opentsdb.query.interpolation.types.numeric.ScalarNumericInterpolatorConfig;

/**
 * Implements a fixed numeric fill policy, returning the same value every time
 * {@link #fill()} is called.
 * 
 * @since 3.0
 */
public class ScalarNumericFillPolicy extends BaseNumericFillPolicy {
  
  /** The config policy. */
  protected ScalarNumericInterpolatorConfig config;
  
  /** The value to fill with. */
  protected long fill;
  
  /**
   * Ctor for integers.
   * @param config The config policy that includes the value to fill with.
   */
  public ScalarNumericFillPolicy(final ScalarNumericInterpolatorConfig config) {
    super(config);
    this.config = config;
  }
  
  @Override
  public NumericType fill() {
    return this;
  }
  
  @Override
  public boolean isInteger() {
    return config.isInteger();
  }
  
  @Override
  public long longValue() {
    return config.longValue();
  }
  
  @Override
  public double doubleValue() {
    return config.doubleValue();
  }
  
  @Override
  public double toDouble() {
    return config.toDouble();
  }
}
