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

import com.google.common.base.Strings;

/**
 * The base config for object pools with common fields.
 * 
 * @since 3.0
 */
public abstract class BaseObjectPoolConfig implements ObjectPoolConfig {
  protected final ObjectPoolAllocator allocator;
  protected final int initial_count;
  protected final int max_count;
  protected final String id;
  
  /**
   * Default builder ctor.
   * @param builder A non-null builder.
   */
  protected BaseObjectPoolConfig(final Builder builder) {
    if (builder.allocator == null) {
      throw new IllegalArgumentException("Allocator cannot be null.");
    }
    if (Strings.isNullOrEmpty(builder.id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    if (builder.max_count < builder.initial_count) {
      throw new IllegalArgumentException("Max count cannot be less than initial count.");
    }
    
    allocator = builder.allocator;
    initial_count = builder.initial_count;
    max_count = builder.max_count;
    id = builder.id;
  }
  
  @Override
  public ObjectPoolAllocator allocator() {
    return allocator;
  }
  
  @Override
  public int initialCount() {
    return initial_count;
  }
  
  @Override
  public int maxCount() {
    return max_count;
  }
  
  @Override
  public String id() {
    return id;
  }
  
  /**
   * An abstract builder implementation to set common fields.
   * 
   * @since 3.0
   */
  public static abstract class Builder {
    protected ObjectPoolAllocator allocator;
    protected int initial_count;
    protected int max_count;
    protected String id;
    
    public Builder setAllocator(final ObjectPoolAllocator allocator) {
      this.allocator = allocator;
      return this;
    }
    
    public Builder setInitialCount(final int initial_count) {
      this.initial_count = initial_count;
      return this;
    }
    
    public Builder setMaxCount(final int max_count) {
      this.max_count = max_count;
      return this;
    }
    
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    public abstract ObjectPoolConfig build();
    
  }
}