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
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;

/**
 * An allocator and pool for string builders used during serialization.
 * 
 * @since 3.0
 */
public class StringBuilderPool extends BaseObjectPoolAllocator {
  public static final String TYPE = "StringBuilderPool";
  public static final TypeToken<?> TYPE_TOKEN = TypeToken.of(StringBuilder.class);
  
  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    if (Strings.isNullOrEmpty(id)) {
      this.id = TYPE;
    } else {
      this.id = id;
    }
    
    registerConfigs(tsdb.getConfig(), TYPE);
    
    final ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
        .setAllocator(this)
        .setInitialCount(Math.max(4096, 
            tsdb.getConfig().getInt(configKey(COUNT_KEY, TYPE))))
        .setMaxCount(Math.max(4096, 
            tsdb.getConfig().getInt(configKey(COUNT_KEY, TYPE))))
        .setId(this.id)
        .build();
    try {
      createAndRegisterPool(tsdb, config, TYPE);
      return Deferred.fromResult(null);
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }
  
  @Override
  public Object allocate() {
    return new StringBuilder(65536);
  }
  
  @Override
  public TypeToken<?> dataType() {
    return TYPE_TOKEN;
  }
  
}
