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

import net.opentsdb.core.TSDB;
import net.opentsdb.pools.ArrayObjectPool;
import net.opentsdb.pools.DoubleArrayPool;
import net.opentsdb.pools.IntArrayPool;
import net.opentsdb.pools.LongArrayPool;

/**
 * Simple base to implement the plugin.
 * 
 * @since 3.0
 */
public abstract class BaseArrayFactoryWithIntPool extends BaseArrayFactory {
  
  protected ArrayObjectPool int_pool;
  
  /**
   * Fetches the object pool implementations from storage.
   * @param tsdb The non-null TSDB to pull the pools from.
   */
  protected void setPools(final TSDB tsdb) {
    long_pool = (ArrayObjectPool) tsdb.getRegistry().getObjectPool(
        LongArrayPool.TYPE);
    double_pool = (ArrayObjectPool) tsdb.getRegistry().getObjectPool(
        DoubleArrayPool.TYPE);
    int_pool = (ArrayObjectPool) tsdb.getRegistry().getObjectPool(
        IntArrayPool.TYPE);
  }
  
  ArrayObjectPool intPool() {
    return int_pool;
  }
  
}