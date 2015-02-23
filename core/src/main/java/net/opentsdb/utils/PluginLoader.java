// This file is part of OpenTSDB.
// Copyright (C) 2013-2014  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import java.util.List;
import java.util.ServiceLoader;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Super simple ServiceLoader based plugin framework for OpenTSDB that lets us
 * search for a specific plugin type or any plugins that match a given class.
 * This isn't meant to be a rich plugin manager, it only handles the basics
 * of searching and instantiating a given class.
 * <p>   
 * Plugin creation is pretty simple, just implement the abstract plugin class,
 * create a Manifest file, add the "services" folder and plugin file and export 
 * a jar file.
 * <p>
 * <b>Note:</b> All plugins must have a parameterless constructor for the 
 * ServiceLoader to work. This means you can't have final class variables, but
 * we'll make a promise to call an initialize() method with the proper 
 * parameters, such as configs or the TSDB object, immediately after loading a
 * plugin and before trying to access any of its methods. 
 * <p>
 * <b>Note:</b> All plugins must also implement a shutdown() method to clean up 
 * gracefully.
 * 
 * @since 2.0
 */
public final class PluginLoader {
  private static final Logger LOG = LoggerFactory.getLogger(PluginLoader.class);

  /**
   * Searches the class path for the a plugin with the given name and type.
   * <p>
   * <b>WARNING:</b> If there are multiple versions of the request plugin in the
   * class path, only one will be returned, so check the logs to see that the
   * correct version was loaded.
   * 
   * @param name The specific name of a plugin to search for, e.g. 
   *   net.opentsdb.search.ElasticSearch
   * @param type The class type to search for
   * @return An instantiated object of the given type
   * @throws IllegalArgumentException if the plugin name is null or empty or
   * no plugin with the requested name was found
   * @throws ServiceConfigurationError if the plugin cannot be instantiated
   */
  public static <T> T loadSpecificPlugin(final String name, final Class<T> type) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name),
            "Missing plugin name");

    final List<T> plugins = loadPlugins(type);

    for (final T plugin : plugins) {
      if (plugin.getClass().getName().equals(name)) {
        return plugin;
      }
    }

    throw new IllegalArgumentException("Unable to locate plugin with name " +
            name + " of type " + type);
  }
  
  /**
   * Searches the class path for implementations of the given type, returning a 
   * list of all plugins that were found.
   * <p>
   * <b>WARNING:</b> If there are multiple versions of the request plugin in the 
   * class path, only one will be returned, so check the logs to see that the
   * correct version was loaded.
   * 
   * @param type The class type to search for
   * @return An list of objects of the given type. If no objects were found
   * an empty list will be returned.
   * @throws ServiceConfigurationError if any of the plugins could not be 
   * instantiated
   */
  public static <T> List<T> loadPlugins(final Class<T> type) {
    ServiceLoader<T> serviceLoader = ServiceLoader.load(type);
    final ImmutableList<T> plugins = ImmutableList.copyOf(serviceLoader);
    LOG.info("Found {} plugins of type {}", plugins.size(), type);
    return plugins;
  }
}
