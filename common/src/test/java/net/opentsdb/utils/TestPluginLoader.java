// This file is part of OpenTSDB.
// Copyright (C) 2013-2017  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.util.List;

import net.opentsdb.utils.PluginLoader;

import org.junit.Test;

/**
 * Note: for this to work the "plugin_test.jar" file must be created. Maven
 * will do it for us.
 */
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
        "net.opentsdb.utils.DummyPluginA", 
        DummyPlugin.class);
    assertNotNull(plugin);
    assertEquals("Dummy Plugin A", plugin.myname);
  }
  
  @Test
  public void loadSpecificPluginImplementationNotFound() throws Exception {
    PluginLoader.loadJAR("plugin_test.jar");
    DummyPlugin plugin = PluginLoader.loadSpecificPlugin(
        "net.opentsdb.utils.DummyPluginC", 
        DummyPlugin.class);
    assertNull(plugin);
  }
  
  @Test
  public void loadSpecificPluginNotFound() throws Exception {
    PluginLoader.loadJAR("plugin_test.jar");
    DummyPluginBad plugin = PluginLoader.loadSpecificPlugin(
        "net.opentsdb.utils.DummyPluginC", 
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
  public void loadPluginsInterface() throws Exception {
    List<DummyInterface> plugins = PluginLoader.loadPlugins(
        DummyInterface.class);
    assertNotNull(plugins);
    assertEquals(2, plugins.size());
    boolean matched_raw = false;
    boolean matched_abstract = false;
    for (final DummyInterface plugin : plugins) {
      if (plugin instanceof DummyInterfaceImp) {
        matched_raw = true;
      } else if (plugin instanceof DummyExtendedInterfaceImp) {
        matched_abstract = true;
      } else {
        throw new AssertionError("Unexpected class type: " + plugin.getClass());
      }
    }
    assertTrue(matched_raw);
    assertTrue(matched_abstract);
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

  /** A test interface for plugin loading. */
  public static interface DummyInterface {
    public void hello();
  }
  
  /** Top-level implementations are fine (or statics in a nest)... */
  public static class DummyInterfaceImp implements DummyInterface {
    @Override
    public void hello() { }
  }
  
  /** ... but child classes won't be loaded since they need the parent
   * in their ctor. */
  public class DummyBadImp implements DummyInterface {
    @Override
    public void hello() { }
  }
  
  /** Likewise, we can have an abstract in a plugin... */
  public static abstract class DummyExtendedInterface 
    implements DummyInterface {
  }
  
  /** ... and if it's a static or top-level, all good! */
  public static class DummyExtendedInterfaceImp 
    extends DummyExtendedInterface {
    @Override
    public void hello() { }
  }
  
  /** But children are bad! */
  public abstract class DummyBadInterface extends DummyExtendedInterface {
  }
  
  public class DummyBadInterfaceImp extends DummyBadInterface {
    @Override
    public void hello() { }
  }
}
