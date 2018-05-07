// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.concurrent.ExecutorService;

import com.stumbleupon.async.Deferred;

import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryInterpolatorFactory;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.storage.TimeSeriesDataStore;

/**
 * A shared location for registering context, mergers, plugins, etc.
 *
 * @since 3.0
 */
public interface Registry {

  /**
   * Initializes the registry including loading the plugins if specified.
   * @param load_plugins Whether or not to load plugins.
   * @return A non-null deferred to wait on for initialization to complete.
   */
  public Deferred<Object> initialize(final boolean load_plugins);
  
  /** @return The cleanup thread pool for post-query or other tasks. */
  public ExecutorService cleanupPool();
  
  /**
   * Registers the given plugin in the map. If a plugin with the ID is already
   * present, an exception is thrown.
   * @param clazz The type of plugin to be stored.
   * @param id An ID for the plugin (may be null if it's a default).
   * @param plugin A non-null and initialized plugin to register.
   * @throws IllegalArgumentException if the class or plugin was null or if
   * a plugin was already registered with the given ID. Also thrown if the
   * plugin given is not an instance of the class.
   */
  public void registerPlugin(final Class<?> clazz, 
                             final String id, 
                             final TSDBPlugin plugin);
  
  /**
   * Retrieves the default plugin of the given type (i.e. the ID was null when
   * registered).
   * @param clazz The type of plugin to be fetched.
   * @return An instantiated plugin if found, null if not.
   * @throws IllegalArgumentException if the clazz was null.
   */
  public <T> T getDefaultPlugin(final Class<T> clazz);
  
  /**
   * Retrieves the plugin with the given class type and ID.
   * @param clazz The type of plugin to be fetched.
   * @param id An optional ID, may be null if the default is fetched.
   * @return An instantiated plugin if found, null if not.
   * @throws IllegalArgumentException if the clazz was null.
   */
  public <T> T getPlugin(final Class<T> clazz, final String id);

  /**
   * Registers a shared object in the concurrent map if the object was not
   * present. If an object was already present, the existing object is returned.
   * @param id A non-null and non-empty ID for the shared object.
   * @param obj A non-null object.
   * @return Null if the object was inserted successfully, a non-null object
   * if something with the given ID was already present.
   * @throws IllegalArgumentException if the ID was null or empty or the
   * object was null.
   */
  public Object registerSharedObject(final String id, final Object object);
  
  /**
   * Returns the shared object for this Id if it exists.
   * @param id A non-null and non-empty ID.
   * @return The object if present, null if not.
   */
  public Object getSharedObject(final String id);
  
  /**
   * Registers a query node factory using the name as the ID.
   * @param factory The non-null factory to register.
   */
  public void registerFactory(final QueryNodeFactory factory);
  
  /**
   * Fetches a query node factory from the cache or plugin store.
   * @param id A non-null and non-empty factory ID.
   * @return The factory if present for the given ID. May be null.
   */
  public QueryNodeFactory getQueryNodeFactory(final String id);
  
  /**
   * Fetch a {@link QueryInterpolatorFactory} if present in the 
   * interpolator cache or plugin map.
   * @param id A non-null and non-empty ID for the interpolator factory.
   * @return The factory if found, null if such an inerpolator does not exist.
   */
  public QueryInterpolatorFactory 
      getQueryIteratorInterpolatorFactory(final String id);
  
  /**
   * Fetches a {@link QueryIteratorFactory} if present in the iterator cache
   * for plugin map.
   * @param id A non-null and non-empty ID for the iterator factory.
   * @return The factory if found, null if such an iterator does not exist.
   */
  public QueryIteratorFactory getQueryIteratorFactory(final String id);
  
  public TimeSeriesDataStore getDefaultStore();
  
  public TimeSeriesDataStore getStore(final String id);
  
  /** @return Package private shutdown returning the deferred to wait on. */
  public Deferred<Object> shutdown();
}
