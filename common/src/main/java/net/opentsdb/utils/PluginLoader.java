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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Super simple ServiceLoader based plugin framework for OpenTSDB that lets us
 * add files or directories to the class path after startup and then search for
 * a specific plugin type or any plugins that match a given class. This isn't 
 * meant to be a rich plugin manager, it only handles the basics of searching
 * and instantiating a given class.
 * <p>
 * Before attempting any of the plugin loader calls, users should call one or 
 * more of the jar loader methods to append files to the class path that may
 * have not been loaded on startup. This is particularly useful for plugins that
 * have dependencies not included by OpenTSDB. 
 * <p>
 * For example, a typical process may be:
 * <ul>
 * <li>loadJARs(&lt;plugin_path&gt;) where &lt;plugin_path&gt; contains JARs of 
 * the plugins and their dependencies</li>
 * <li>loadSpecificPlugin() or loadPlugins() to instantiate the proper plugin
 * types</li>
 * </ul>     
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
  
  /** Static list of types for the class loader */
  private static final Class<?>[] PARAMETER_TYPES = new Class[] {
    URL.class
  };
  
  /**
   * Searches the class path for the specific plugin of a given type
   * <p>
   * <b>Note:</b> If you want to load JARs dynamically, you need to call 
   * {@link #loadJAR} or {@link #loadJARs} methods with the proper file
   * or directory first, otherwise this will only search whatever was loaded
   * on startup.
   * <p>
   * <b>WARNING:</b> If there are multiple versions of the request plugin in the
   * class path, only one will be returned, so check the logs to see that the
   * correct version was loaded.
   * 
   * @param name The specific name of a plugin to search for, e.g. 
   *   net.opentsdb.search.ElasticSearch
   * @param type The class type to search for
   * @return An instantiated object of the given type if found, null if the
   * class could not be found
   * @throws ServiceConfigurationError if the plugin cannot be instantiated
   * @throws IllegalArgumentName if the plugin name is null or empty
   */
  public static <T> T loadSpecificPlugin(final String name, 
      final Class<T> type) {
    if (name.isEmpty()) {
      throw new IllegalArgumentException("Missing plugin name");
    }
    ServiceLoader<T> serviceLoader = ServiceLoader.load(type);
    Iterator<T> it = serviceLoader.iterator();
    if (!it.hasNext()) {
      LOG.warn("Unable to locate any plugins of the type: " + type.getName());
      return null;
    }
    
    while(it.hasNext()) {
      T plugin = it.next();
      if (plugin.getClass().getName().equals(name) || plugin.getClass().getSuperclass().getName().equals(name)) {
        return plugin;
      }
    }
    
    LOG.warn("Unable to locate plugin: " + name);
    return null;
  }
  
  /**
   * Searches the class path for implementations of the given type, returning a 
   * list of all plugins that were found
   * <p>
   * <b>Note:</b> If you want to load JARs dynamically, you need to call 
   * {@link #loadJAR} or {@link #loadJARs} methods with the proper file
   * or directory first, otherwise this will only search whatever was loaded
   * on startup.
   * <p>
   * <b>WARNING:</b> If there are multiple versions of the request plugin in the 
   * class path, only one will be returned, so check the logs to see that the
   * correct version was loaded.
   * 
   * @param type The class type to search for
   * @return An instantiated list of objects of the given type if found, null 
   * if no implementations of the type were found
   * @throws ServiceConfigurationError if any of the plugins could not be 
   * instantiated
   */
  public static <T> List<T> loadPlugins(final Class<T> type) {
    ServiceLoader<T> serviceLoader = ServiceLoader.load(type);
    Iterator<T> it = serviceLoader.iterator();
    if (!it.hasNext()) {
      LOG.warn("Unable to locate any plugins of the type: " + type.getName());
      return null;
    }
    
    ArrayList<T> plugins = new ArrayList<T>();
    while(it.hasNext()) {
      plugins.add(it.next());
    }
    if (plugins.size() > 0) {
      return plugins;
    }
    
    LOG.warn("Unable to locate plugins for type: " + type.getName());
    return null;
  }
  
  /**
   * Attempts to load the given jar into the class path
   * @param jar Full path to a .jar file
   * @throws IOException if the file does not exist or cannot be accessed
   * @throws SecurityException if there is a security manager present and the
   * operation is denied
   * @throws IllegalArgumentException if the filename did not end with .jar
   * @throws NoSuchMethodException if there is an error with the class loader 
   * @throws IllegalAccessException if a security manager is present and the
   * operation was denied
   * @throws InvocationTargetException if there is an issue loading the jar
   */
  public static void loadJAR(String jar) throws IOException, SecurityException, 
  IllegalArgumentException, NoSuchMethodException, IllegalAccessException, 
  InvocationTargetException {
    if (!jar.toLowerCase().endsWith(".jar")) {
      throw new IllegalArgumentException(
          "File specified did not end with .jar");
    }
    File file = new File(jar);
    if (!file.exists()) {
      throw new FileNotFoundException(jar);
    }
    addFile(file);
  }
  
  /**
   * Recursively traverses a directory searching for files ending with .jar and
   * loads them into the class path
   * <p>
   * <b>WARNING:</b> This can be pretty slow if you have a directory with many 
   * sub-directories. Keep the directory structure shallow.
   * 
   * @param directory The directory 
   * @throws IOException if the directory does not exist or cannot be accessed
   * @throws SecurityException if there is a security manager present and the
   * operation is denied
   * @throws IllegalArgumentException if the path was not a directory
   * @throws NoSuchMethodException if there is an error with the class loader 
   * @throws IllegalAccessException if a security manager is present and the
   * operation was denied
   * @throws InvocationTargetException if there is an issue loading the jar
   */
  public static void loadJARs(String directory) throws SecurityException, 
  IllegalArgumentException, IOException, NoSuchMethodException, 
  IllegalAccessException, InvocationTargetException {
    File file = new File(directory);
    if (!file.isDirectory()) {
      throw new IllegalArgumentException(
          "The path specified was not a directory");
    }
    
    ArrayList<File> jars = new ArrayList<File>();
    searchForJars(file, jars);
    if (jars.size() < 1) {
      LOG.debug("No JAR files found in path: " + directory);
      return;
    }
    
    for (File jar : jars) {
      addFile(jar);
    }
  }
  
  /**
   * Recursive method to search for JAR files starting at a given level
   * @param file The directory to search in
   * @param jars A list of file objects that will be loaded with discovered
   * jar files
   * @throws SecurityException if a security manager exists and prevents reading
   */
  private static void searchForJars(final File file, List<File> jars) {
    if (file.isFile()) {
      if (file.getAbsolutePath().toLowerCase().endsWith(".jar")) {
        jars.add(file);
        LOG.debug("Found a jar: " + file.getAbsolutePath());
      }
    } else if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files == null) {
        // if this is null, it's due to a security issue
        LOG.warn("Access denied to directory: " + file.getAbsolutePath());
      } else {
        for (File f : files) {
          searchForJars(f, jars);
        }
      }
    }    
  }
  
  /**
   * Attempts to add the given file object to the class loader
   * @param f The JAR file object to load
   * @throws IOException if the file does not exist or cannot be accessed
   * @throws SecurityException if there is a security manager present and the
   * operation is denied
   * @throws IllegalArgumentException if the file was invalid
   * @throws NoSuchMethodException if there is an error with the class loader 
   * @throws IllegalAccessException if a security manager is present and the
   * operation was denied
   * @throws InvocationTargetException if there is an issue loading the jar
   */
  private static void addFile(File f) throws IOException, SecurityException, 
  IllegalArgumentException, NoSuchMethodException, IllegalAccessException, 
  InvocationTargetException {
    addURL(f.toURI().toURL());
  }
  
  /**
   * Attempts to add the given file/URL to the class loader
   * @param url Full path to the file to add
   * @throws SecurityException if there is a security manager present and the
   * operation is denied
   * @throws IllegalArgumentException if the path was not a directory
   * @throws NoSuchMethodException if there is an error with the class loader 
   * @throws IllegalAccessException if a security manager is present and the
   * operation was denied
   * @throws InvocationTargetException if there is an issue loading the jar
   */
  private static void addURL(final URL url) throws SecurityException, 
  NoSuchMethodException, IllegalArgumentException, IllegalAccessException, 
  InvocationTargetException {
    URLClassLoader sysloader = (URLClassLoader)ClassLoader.getSystemClassLoader();
    Class<?> sysclass = URLClassLoader.class;
    
    Method method = sysclass.getDeclaredMethod("addURL", PARAMETER_TYPES);
    method.setAccessible(true);
    method.invoke(sysloader, new Object[]{ url }); 
    LOG.debug("Successfully added JAR to class loader: " + url.getFile());
  }
}
