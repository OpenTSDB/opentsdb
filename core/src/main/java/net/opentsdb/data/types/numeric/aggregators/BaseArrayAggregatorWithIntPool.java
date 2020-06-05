// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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

import net.opentsdb.pools.PooledObject;

/**
 * A base implementation for numeric array aggregation functions with an integer
 * array as well that's used for tracking counts or indices.
 * 
 * @since 3.0
 */
public abstract class BaseArrayAggregatorWithIntPool extends BaseArrayAggregator {

  protected int[] int_array;
  protected PooledObject int_pooled;
  
  public BaseArrayAggregatorWithIntPool(final boolean infectious_nans, 
                                        final BaseArrayFactory factory) {
    super(infectious_nans, factory);
  }

  public BaseArrayAggregatorWithIntPool(final NumericArrayAggregatorConfig config,
                                        final BaseArrayFactory factory) {
    super(config, factory);
  }
  
  @Override
  public void close() {
    super.close();
    if (int_pooled != null) {
      int_pooled.release();
      int_pooled = null;
    }
    int_array = null;
  }
}
