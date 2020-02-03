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
package net.opentsdb.pools;

import java.time.temporal.ChronoUnit;

/**
 * An extension of the {@link ObjectPool} that allows for pooling of arrays
 * where the caller can claim one of a given length and if it's not of the right
 * size then the allocator can return a new one of the proper length.
 * 
 * Note that we don't want the pool to store re-sized arrays as we need to 
 * maintain a fixed heap size.
 * 
 * @since 3.0
 */
public interface ArrayObjectPool extends ObjectPool {

  /**
   * Attempts to claim an object from the pool immediately, allocating a new
   * one if they're all in use. If the requested length is greater than the 
   * configured pool array length, a new array is allocated and will be GC'd.
   * 
   * @param length The length of the array to return.
   * @return A non-null pooled object.
   */
  public PooledObject claim(final int length);
  
  /**
   * Attempts to claim an object from the pool, allowing for retries until the
   * given time span has expired. If all objects are still in use then it will
   * return a new object. If the requested length is greater than the 
   * configured pool array length, a new array is allocated and will be GC'd.
   * 
   * @param length The length of the array to return.
   * @param time The number of time.
   * @param unit The units reflecting the numeric value.
   * @return A non-null pooled object.
   */
  public PooledObject claim(final int length, 
                            final long time, 
                            final ChronoUnit unit);
  
}
