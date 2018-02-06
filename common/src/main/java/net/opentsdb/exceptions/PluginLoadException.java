// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.exceptions;

/**
 * A simple exception thrown when attempting to instantiate a plugin.
 * 
 * @since 3.0
 */
public class PluginLoadException extends RuntimeException {
  private static final long serialVersionUID = 56063244701855014L;

  /** The class name of the plugin. */
  protected final String plugin;
  
  /**
   * Default ctor.
   * @param msg A descriptive message about the exception.
   * @param plugin The class name of the plugin.
   */
  public PluginLoadException(final String msg, final String plugin) {
    super(msg);
    this.plugin = plugin;
  }
  
  /**
   * Ctor with a cause.
   * @param msg A descriptive message about the exception.
   * @param plugin The class name of the plugin.
   * @param e An optional exception that caused this exception.
   */
  public PluginLoadException(final String msg, final String plugin, 
      final Exception e) {
    super(msg, e);
    this.plugin = plugin;
  }
  
  /** @return The class name of the plugin. */
  public String getPlugin() {
    return plugin;
  }
}
