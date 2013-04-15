// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.FileNotFoundException;
import java.util.List;

import net.opentsdb.plugin.DummyPlugin;
import net.opentsdb.utils.PluginLoader;

import org.junit.Test;

public final class TestPluginLoader {

  @Test
  public void loadJar() throws Exception {
    PluginLoader.loadJAR("plugin_test.jar");
  }
  
  @Test (expected = FileNotFoundException.class)
  public void loadJarDoesNotExist() throws Exception {
    PluginLoader.loadJAR("jardoesnotexist.jar");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void loadJarDoesNotAJar() throws Exception {
    PluginLoader.loadJAR("notajar.png");
  }
  
  @Test (expected = NullPointerException.class)
  public void loadJarNull() throws Exception {
    PluginLoader.loadJAR(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void loadJarEmpty() throws Exception {
    PluginLoader.loadJAR("");
  }
  
  // todo - test for security exceptions?
  
  @Test
  public void loadJars() throws Exception {
    PluginLoader.loadJARs("./");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void loadJarsDoesNotExist() throws Exception {
    PluginLoader.loadJARs("./dirdoesnotexist");
  }
  
  @Test (expected = NullPointerException.class)
  public void loadJarsNull() throws Exception {
    PluginLoader.loadJARs(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void loadJarsEmpty() throws Exception {
    PluginLoader.loadJARs("");
  }
  
  @Test
  public void loadSpecificPlugin() throws Exception {
    PluginLoader.loadJAR("plugin_test.jar");
    DummyPlugin plugin = PluginLoader.loadSpecificPlugin(
        "net.opentsdb.plugin.DummyPluginA", 
        DummyPlugin.class);
    assertNotNull(plugin);
    assertEquals("Dummy Plugin A", plugin.myname);
  }
  
  @Test
  public void loadSpecificPluginImplementationNotFound() throws Exception {
    PluginLoader.loadJAR("plugin_test.jar");
    DummyPlugin plugin = PluginLoader.loadSpecificPlugin(
        "net.opentsdb.plugin.DummyPluginC", 
        DummyPlugin.class);
    assertNull(plugin);
  }
  
  @Test
  public void loadSpecificPluginNotFound() throws Exception {
    PluginLoader.loadJAR("plugin_test.jar");
    DummyPluginBad plugin = PluginLoader.loadSpecificPlugin(
        "net.opentsdb.plugin.DummyPluginC", 
        DummyPluginBad.class);
    assertNull(plugin);
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
    assertNull(plugins);
  }
  
  public abstract class DummyPluginBad {
    protected String myname;
    
    public DummyPluginBad() {
      myname = "";
    }
    
    public abstract String mustImplement();
  }
}
