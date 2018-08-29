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
package net.opentsdb.data.types.numeric.aggregators;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;

/**
 * Returns a Sum array aggregator.
 * 
 * @since 3.0
 */
public class ArraySumFactory implements NumericArrayAggregatorFactory {

  @Override
  public NumericArrayAggregator newAggregator(boolean infectious_nan) {
    return new ArraySum(infectious_nan);
  }
  
  @Override
  public String id() {
    return "sum";
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }
  
}