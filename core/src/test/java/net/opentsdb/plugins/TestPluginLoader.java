
package net.opentsdb.plugins;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.List;

public final class TestPluginLoader {
  @Test
  public void loadSpecificPlugin() {
    DummyPlugin plugin = PluginLoader.loadSpecificPlugin(
        "net.opentsdb.plugins.DummyPluginImplementation",
        DummyPlugin.class);
    assertTrue(plugin instanceof DummyPluginImplementation);
  }

  @Test (expected = IllegalArgumentException.class)
  public void loadSpecificPluginNotFound() {
    PluginLoader.loadSpecificPlugin("NoImplementation", DummyPlugin.class);
  }

  @Test
  public void loadPlugins() throws Exception {
    List<DummyPlugin> plugins = PluginLoader.loadPlugins(DummyPlugin.class);
    assertEquals(2, plugins.size());
  }
  
  @Test
  public void loadPluginsNotFound() {
    List<DummyPlugin> plugins = PluginLoader.loadPlugins(DummyPlugin.class);
    assertEquals(0, plugins.size());
  }
}
