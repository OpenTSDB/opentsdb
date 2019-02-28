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

/**
 * A configuration for an object pool.
 * 
 * @since 3.0
 */
public interface ObjectPoolConfig {
  
  /** @return The non-null allocator. */
  public ObjectPoolAllocator allocator();
  
  /** @return The number of objects to instantiate on creation. */
  public int initialCount();
  
  /** @return The maximum number of objects to keep in the pool. */
  public int maxCount();
  
  /** @return A non-null and non-empty ID for the config and the pool. */
  public String id();
}
