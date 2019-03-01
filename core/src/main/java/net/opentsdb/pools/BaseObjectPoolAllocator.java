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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.TSDB;

/**
 * A useful base class for object pool allocators.
 * 
 * @since 3.0
 */
public abstract class BaseObjectPoolAllocator implements ObjectPoolAllocator {
  private static final Logger LOG = LoggerFactory.getLogger(
      BaseObjectPoolAllocator.class);
  
  /** The ID of this pool. */
  protected String id;
  
  /** The size of each object (array length plus overhead). */
  protected int size;
  
  @Override
  public String id() {
    return id;
  }
  
  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public int size() {
    return size;
  }
  
  @Override
  public void deallocate(final Object object) {
    // no-op
  }
  
  /**
   * Creates a TSDB config key.
   * @param suffix The non-null and non-empty suffix.
   * @param type The non-null and non-empty type to use to determine whether we
   * insert the ID in the key or not.
   * @return The full config key.
   */
  protected String configKey(final String suffix, final String type) {
    return PREFIX + (id == null || id.equals(type) ? "" : id + ".") + suffix;
  }
  
  /**
   * Registers the default configs for every pool.
   * @param config A non-null configuration class from the TSD.
   * @param type The non-null and non-empty type to use to determine whether we
   * insert the ID in the key or not.
   */
  protected void registerConfigs(final Configuration config, final String type) {
    if (!config.hasProperty(configKey(POOL_ID_KEY, type))) {
      config.register(configKey(POOL_ID_KEY, type), null, false, 
          "The ID of an object pool factory plugin to use for this pool. "
              + "Can be null to use the default.");
    }
    if (!config.hasProperty(configKey(COUNT_KEY, type))) {
      config.register(configKey(COUNT_KEY, type), 1024, false, 
          "The number of initial objects to allocate in this pool.");
    }
  }
  
  /**
   * Creates and registers the pool with the registry. If no default or factory
   * with the given ID is found, uses the {@link DummyObjectPool}.
   * @param tsdb The non-null TSD.
   * @param config The non-null config.
   * @param type The non-null and non-empty type to use to determine whether we
   * insert the ID in the key or not. 
   */
  protected void createAndRegisterPool(final TSDB tsdb, 
                                       final ObjectPoolConfig config,
                                       final String type) {
    final String factory_id = tsdb.getConfig()
        .getString(configKey(POOL_ID_KEY, type));
    final ObjectPoolFactory factory = 
        tsdb.getRegistry().getPlugin(ObjectPoolFactory.class, factory_id);
    if (factory == null) {
      LOG.warn("No object pool factory found for ID: " 
          + (factory_id == null ? "Default" : factory_id) 
              + ". Using the DummyObjectPool instead.");
      tsdb.getRegistry().registerObjectPool(new DummyObjectPool(tsdb, config));
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
