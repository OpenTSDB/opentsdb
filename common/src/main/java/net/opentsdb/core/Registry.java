// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.util.concurrent.ExecutorService;

import com.stumbleupon.async.Deferred;

import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryIteratorInterpolatorFactory;
import net.opentsdb.query.QueryNodeFactory;

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
  public TSDBPlugin getDefaultPlugin(final Class<?> clazz);
  
  /**
   * Retrieves the plugin with the given class type and ID.
   * @param clazz The type of plugin to be fetched.
   * @param id An optional ID, may be null if the default is fetched.
   * @return An instantiated plugin if found, null if not.
   * @throws IllegalArgumentException if the clazz was null.
   */
  public TSDBPlugin getPlugin(final Class<?> clazz, final String id);

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
   * Fetch a {@link QueryIteratorInterpolatorFactory} if present in the 
   * interpolator cache or plugin map.
   * @param id A non-null and non-empty ID for the interpolator factory.
   * @return The factory if found, null if such an inerpolator does not exist.
   */
  public QueryIteratorInterpolatorFactory 
      getQueryIteratorInterpolatorFactory(final String id);
  
  /**
   * Fetches a {@link QueryIteratorFactory} if present in the iterator cache
   * for plugin map.
   * @param id A non-null and non-empty ID for the iterator factory.
   * @return The factory if found, null if such an iterator does not exist.
   */
  public QueryIteratorFactory getQueryIteratorFactory(final String id);
  
  /** @return Package private shutdown returning the deferred to wait on. */
  public Deferred<Object> shutdown();
}
