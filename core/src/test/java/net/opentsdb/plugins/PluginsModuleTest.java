package net.opentsdb.plugins;

import static org.junit.Assert.assertTrue;

import net.opentsdb.DaggerTestComponent;
import net.opentsdb.TestComponent;
import net.opentsdb.core.ConfigModule;
import net.opentsdb.search.DefaultSearchPlugin;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.junit.Before;
import org.junit.Test;

public class PluginsModuleTest {
  private TestComponent component;

  @Before
  public void setUp() throws Exception {
    component = DaggerTestComponent.create();
  }

  @Test(expected = IllegalStateException.class)
  public void testProvideRealTimePublisherThrowsExceptionWrongConfig() {
    final Config config = ConfigFactory.load()
        .withValue("tsd.rtpublisher.plugin", ConfigValueFactory.fromAnyRef("doesNotExist"));

    final TestComponent component = DaggerTestComponent.builder()
        .configModule(new ConfigModule(config))
        .build();

    component.realTimePublisher();
  }

  @Test
  public void testProvideRealtimePublisherDefaultPublisher() {
    assertTrue(component.realTimePublisher() instanceof DefaultRealtimePublisher);
  }

  @Test
  public void testProvideSearchPluginDefaultPlugin() {
    assertTrue(component.searchPlugin() instanceof DefaultSearchPlugin);
  }

  @Test(expected = IllegalStateException.class)
  public void testProvideSearchPluginThrowsExceptionWrongConfig() {
    final Config config = ConfigFactory.load()
        .withValue("tsd.search.plugin", ConfigValueFactory.fromAnyRef("doesNotExist"));

    final TestComponent component = DaggerTestComponent.builder()
        .configModule(new ConfigModule(config))
        .build();

    component.searchPlugin();
  }
}