package net.opentsdb.core;

import net.opentsdb.utils.Config;

import org.junit.Before;
import org.junit.Test;

public class TsdbBuilderTest {
  private Config config;

  @Before
  public void before() throws Exception {
    config = new Config(false);
  }

  @Test
  public void loadPluginsDefaults() {
    // no configured plugin path, plugins disabled, no exceptions
    TsdbBuilder.createFromConfig(config);
  }

  @Test
  public void loadPluginsSearch() throws Exception {
    config.overrideConfig("tsd.core.plugin_path", "./");
    config.overrideConfig("tsd.search.enable", "true");
    config.overrideConfig("tsd.search.plugin", "net.opentsdb.search.DummySearchPlugin");
    config.overrideConfig("tsd.search.DummySearchPlugin.hosts", "localhost");
    config.overrideConfig("tsd.search.DummySearchPlugin.port", "42");
    TsdbBuilder.createFromConfig(config);
  }

  @Test (expected = RuntimeException.class)
  public void loadPluginsSearchNotFound() throws Exception {
    config.overrideConfig("tsd.search.enable", "true");
    config.overrideConfig("tsd.search.plugin", "net.opentsdb.search.DoesNotExist");
    TsdbBuilder.createFromConfig(config);
  }

}