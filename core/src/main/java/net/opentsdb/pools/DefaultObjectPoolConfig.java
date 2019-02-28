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
 * The default config for object pools.
 * 
 * @since 3.0
 */
public class DefaultObjectPoolConfig extends BaseObjectPoolConfig {

  /**
   * Default ctor.
   * @param builder A non-null builder.
   */
  protected DefaultObjectPoolConfig(final Builder builder) {
    super(builder);
  }
  
  /** @return A new non-null builder instance. */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** A builder for the default config. */
  public static class Builder extends BaseObjectPoolConfig.Builder {

    @Override
    public ObjectPoolConfig build() {
      return new DefaultObjectPoolConfig(this);
    }
    
  }
}