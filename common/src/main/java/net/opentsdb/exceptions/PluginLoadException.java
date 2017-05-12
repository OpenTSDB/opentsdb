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
