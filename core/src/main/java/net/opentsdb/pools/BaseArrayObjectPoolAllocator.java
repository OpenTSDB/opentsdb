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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.TSDB;

/**
 * Base class for the pooled array allocators.
 * 
 * @since 3.0
 */
public abstract class BaseArrayObjectPoolAllocator 
    extends BaseObjectPoolAllocator implements ArrayObjectPoolAllocator {
  private static final Logger LOG = LoggerFactory.getLogger(
      BaseArrayObjectPoolAllocator.class);
  
  protected int pool_length;
  
  @Override
  public int pooledLength() {
    return pool_length;
  }
  
  @Override
  protected void registerConfigs(final Configuration config, final String type) {
    super.registerConfigs(config, type);
    if (!config.hasProperty(configKey(LENGTH_KEY, type))) {
      config.register(configKey(LENGTH_KEY, type), 1024, false, 
          "The length of arrays in this object pool.");
    }
  }
  
  @Override
  protected void createAndRegisterPool(final TSDB tsdb, 
                                       final ObjectPoolConfig config,
                                       final String type) {
    pool_length = tsdb.getConfig().getInt(configKey(LENGTH_KEY, type));
    initial_count = tsdb.getConfig().getInt(configKey(COUNT_KEY, type));
    final String factory_id = tsdb.getConfig()
        .getString(configKey(POOL_ID_KEY, type));
    final ArrayObjectPoolFactory factory = 
        tsdb.getRegistry().getPlugin(ArrayObjectPoolFactory.class, factory_id);
    if (factory == null) {
      LOG.warn("No object pool factory found for ID: " 
          + (factory_id == null ? "Default" : factory_id) 
              + ". Using the DummyObjectPool instead for " + config.id());
      tsdb.getRegistry().registerObjectPool(new DummyArrayObjectPool(tsdb, config));
    } else {
      final ObjectPool pool = factory.newPool(config);
      if (pool != null) {
        tsdb.getRegistry().registerObjectPool(pool);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Successfully registered object pool: " + config.id() 
            + " using factory " + (factory_id == null ? "Default" : factory_id));
        }
      } else {
        throw new ObjectPoolException("Null pool returned for: " + factory_id);
      }
    }
  }
}
