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
package net.opentsdb.query.interpolation.types.numeric;

import java.lang.reflect.Constructor;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.interpolation.BaseQueryIntperolatorFactory;
import net.opentsdb.utils.Pair;

/**
 * LERP interpolators.
 * 
 * @since 3.0
 */
public class LERPFactory extends BaseQueryIntperolatorFactory {

  @Override
  public String id() {
    return "LERP";
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    register(NumericType.TYPE, NumericLERP.class);
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

  @VisibleForTesting
  Map<TypeToken<?>, Pair<Constructor<?>, Constructor<?>>> types() {
    return types;
  }
}
