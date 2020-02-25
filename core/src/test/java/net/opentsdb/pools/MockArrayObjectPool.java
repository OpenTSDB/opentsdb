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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.stumbleupon.async.Deferred;

public class MockArrayObjectPool implements ArrayObjectPool {

  /** The config. */
  private final ObjectPoolConfig config;
  
  /** The queue we'll circulate objects through. */
  public final BlockingQueue<PooledObject> pool;
  
  public int claim_success;
  public int claim_empty_pool;
  public int claim_too_big;
  public int released;
  
  public MockArrayObjectPool(final ObjectPoolConfig config) {
    this.config = config;
    pool = new ArrayBlockingQueue<PooledObject>(config.initialCount());
    for (int i = 0; i < config.initialCount(); i++) {
      final Object obj = ((ArrayObjectPoolAllocator) 
          config.allocator()).allocate(config.arrayLength());
      pool.offer(new LocalPooled(obj, true));
    }
  }
  
  @Override
  public PooledObject claim() {
    PooledObject obj = pool.poll();
    if (obj == null) {
      claim_empty_pool++;
      return new LocalPooled(((ArrayObjectPoolAllocator) 
          config.allocator()).allocate(config.arrayLength()), false);
    } else {
      claim_success++;
      return obj;
    }
  }

  @Override
  public PooledObject claim(long time, ChronoUnit unit) {
    return claim();
  }

  @Override
  public String id() {
    return config.id();
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public PooledObject claim(final int length) {
    if (length > config.arrayLength()) {
      claim_too_big++;
      return new LocalPooled(((ArrayObjectPoolAllocator) 
          config.allocator()).allocate(length), false);
    }
    return claim();
  }

  @Override
  public PooledObject claim(int length, long time, ChronoUnit unit) {
    return claim(length);
  }
  
  public void resetCounters() {
    claim_success = 0;
    claim_empty_pool = 0;
    claim_too_big = 0;
    released = 0;
  }

  /**
   * Super simple wrapper.
   */
  private class LocalPooled implements PooledObject {
    final Object obj;
    final boolean was_pooled;
    
    protected LocalPooled(final Object obj, final boolean was_pooled) {
      this.obj = obj;
      if (obj instanceof CloseablePooledObject) {
        ((CloseablePooledObject) obj).setPooledObject(this);
      }
      this.was_pooled = was_pooled;
    }
    
    @Override
    public Object object() {
      return obj;
    }

    @Override
    public void release() {
      if (was_pooled) {
        if (!pool.offer(this)) {
          throw new IllegalStateException("Failed to return a pooled object "
              + "to the pool for " + config.id());
        }
        released++;
      }
    }
    
  }
}