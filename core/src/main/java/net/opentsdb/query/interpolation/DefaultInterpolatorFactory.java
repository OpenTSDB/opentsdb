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
package net.opentsdb.query.interpolation;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolator;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolator;

/**
 * The default interpolation factory stored as the default plugin with
 * built-in data type interpolators configured.
 * <p>
 * As types are added, please register them here.
 * 
 * @since 3.0
 */
public class DefaultInterpolatorFactory extends BaseQueryIntperolatorFactory {

  @Override
  public String id() {
    return "Default";
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    // Add defaults for the built-in types
    register(NumericType.TYPE, NumericInterpolator.class);
    register(NumericSummaryType.TYPE, NumericSummaryInterpolator.class);
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
