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

import net.opentsdb.plugin.DummyPlugin;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public final class TestPluginLoader {
  @Test
  public void loadSpecificPlugin() throws Exception {
    DummyPlugin plugin = PluginLoader.loadSpecificPlugin(
        "net.opentsdb.plugin.DummyPluginA", 
        DummyPlugin.class);
    assertNotNull(plugin);
    assertEquals("Dummy Plugin A", plugin.myname);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void loadSpecificPluginImplementationNotFound() throws Exception {
    PluginLoader.loadSpecificPlugin(
        "net.opentsdb.plugin.DummyPluginC", 
        DummyPlugin.class);
  }

  @Test (expected = IllegalArgumentException.class)
  public void loadSpecificPluginNotFound() throws Exception {
    PluginLoader.loadSpecificPlugin(
        "net.opentsdb.plugin.DummyPluginC", 
        DummyPluginBad.class);
  }

  @Test
  public void loadPlugins() throws Exception {
    List<DummyPlugin> plugins = PluginLoader.loadPlugins(
        DummyPlugin.class);
    assertNotNull(plugins);
    assertEquals(2, plugins.size());
  }
  
  @Test
  public void loadPluginsNotFound() throws Exception {
    List<DummyPluginBad> plugins = PluginLoader.loadPlugins(
        DummyPluginBad.class);
    assertEquals(0, plugins.size());
  }
  
  public abstract class DummyPluginBad {
    protected String myname;
    
    public DummyPluginBad() {
      myname = "";
    }
    
    public abstract String mustImplement();
  }
}
