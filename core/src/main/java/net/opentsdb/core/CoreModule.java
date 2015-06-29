package net.opentsdb.core;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import dagger.Module;
import dagger.Provides;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import javax.inject.Singleton;

@Module
public class CoreModule {
  private static final Logger LOG = LoggerFactory.getLogger(CoreModule.class);

  private final Config config;

  public CoreModule(final File configFile) {
    this(ConfigFactory.parseFileAnySyntax(configFile,
        ConfigParseOptions.defaults().setAllowMissing(false)));
  }

  public CoreModule(final File configFile, final Config overrides) {
    this(ConfigFactory.parseFileAnySyntax(configFile,
        ConfigParseOptions.defaults().setAllowMissing(false)), overrides);
  }

  public CoreModule() {
    this(ConfigFactory.load());
  }

  /**
   * Create a core module that uses the provided Config instance.
   *
   * @param config The config object to read from
   */
  public CoreModule(final Config config) {
    this.config = config.withFallback(
        ConfigFactory.parseResourcesAnySyntax("reference"));
    LOG.info("Loaded config from {}", config.origin());
  }

  private CoreModule(final Config config, final Config overrides) {
    this(overrides.withFallback(config));
  }

  @Provides
  @Singleton
  Config provideConfig() {
    return config;
  }

  @Provides
  @Singleton
  EventBus provideEventBus() {
    return new EventBus();
  }

  @Provides
  @Singleton
  MetricRegistry provideMetricRegistry() {
    MetricRegistry registry = new MetricRegistry();
    registry.registerAll(new ClassLoadingGaugeSet());
    registry.registerAll(new GarbageCollectorMetricSet());
    registry.registerAll(new MemoryUsageGaugeSet());
    registry.registerAll(new ThreadStatesGaugeSet());
    registry.register("descriptor-usage", new FileDescriptorRatioGauge());
    return registry;
  }
}
