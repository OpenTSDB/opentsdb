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
package net.opentsdb.pools;

import java.time.temporal.ChronoUnit;

import com.stumbleupon.async.Deferred;

/**
 * A generic object pool implementation that will take an 
 * {@link ObjectPoolConfig} with a {@link ObjectPoolAllocator} to handle 
 * objects. This will be returned from an {@link ObjectPoolFactory}.
 * 
 * @since 3.0
 */
public interface ObjectPool {

  /**
   * Attempts to claim an object from the pool immediately, allocating a new
   * one if they're all in use.
   * 
   * @return A non-null pooled object.
   */
  public PooledObject claim();
  
  /**
   * Attempts to claim an object from the pool, allowing for retries until the
   * given time span has expired. If all objects are still in use then it will
   * return a new object.
   * 
   * @param time The number of time.
   * @param unit The units reflecting the numeric value.
   * @return A non-null pooled object.
   */
  public PooledObject claim(final long time, final ChronoUnit unit);
  
  /** @return The non-null ID of this pool. */
  public String id();
  
  /** @return A deferred to wait on when shutting down the pool. */
  public Deferred<Object> shutdown();
  
}
