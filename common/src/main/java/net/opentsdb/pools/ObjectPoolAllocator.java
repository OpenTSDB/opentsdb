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

import com.google.common.reflect.TypeToken;

import net.opentsdb.core.TSDBPlugin;

/**
 * A plugin that allocates objects for an object pool on demand. E.g. this could
 * allocate arrays of bytes or numbers.
 * 
 * @since 3.0
 */
public interface ObjectPoolAllocator extends TSDBPlugin {
  
  /** @return The initial size of an allocated object in bytes. */
  public int size();
  
  /** @return A new object of type {@link #dataType()}. */
  public Object allocate();
  
  /**
   * Called by the pool when an object is to be deallocated/GC'd.
   * @param object The non-null object deallocated by the pool.
   */
  public void deallocate(final Object object);
  
  /** @return The type of data this allocator returns. */
  public TypeToken<?> dataType();
  
}
