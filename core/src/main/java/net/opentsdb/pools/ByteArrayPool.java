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

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.TSDB;

/**
 * An allocator and pool for primitive byte arrays.
 * 
 * @since 3.0
 */
public class ByteArrayPool extends BaseObjectPoolAllocator {
  public static final String TYPE = "ByteArrayPool";
  
  private static final String LENGTH_KEY = "primitive.array.length";
  private static final TypeToken<?> TYPE_TOKEN = TypeToken.of(long[].class);
  
  /** The length of each array to allocate. */
  private int length;
  
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
    length = tsdb.getConfig().getInt(configKey(LENGTH_KEY, TYPE));
    size = (8 * length) +  + 16 /* 64-bit overhead */;
    
    final ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
        .setAllocator(this)
        .setInitialCount(tsdb.getConfig().getInt(configKey(COUNT_KEY, TYPE)))
        .setMaxCount(tsdb.getConfig().getInt(configKey(COUNT_KEY, TYPE)))
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
    return new byte[length];
  }
  
  @Override
  public TypeToken<?> dataType() {
    return TYPE_TOKEN;
  }
  
  @Override
  protected void registerConfigs(final Configuration config, final String type) {
    super.registerConfigs(config, type);
    if (!config.hasProperty(configKey(LENGTH_KEY, TYPE))) {
      config.register(configKey(LENGTH_KEY, TYPE), 8192, false, 
          "The length of each array to allocate");
    }
  }
}