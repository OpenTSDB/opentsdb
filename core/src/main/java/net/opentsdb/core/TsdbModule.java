package net.opentsdb.core;

import com.codahale.metrics.MetricRegistry;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.plugins.PluginsModule;
import net.opentsdb.storage.StoreModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.File;

/**
 * This is the main dagger module for the TSDB core library. It is not complete
 * however, it needs to be complemented with an extending module that provides a
 * {@link com.typesafe.config.Config}, a {@link net.opentsdb.storage.TsdbStore}
 * and a {@link com.codahale.metrics.MetricRegistry}.
 */
@Module(library = true,
        includes = {
            CoreModule.class,
            PluginsModule.class,
            StoreModule.class
        },
        injects = {
            Config.class,
            TSDB.class,
            UniqueIdClient.class,
            MetaClient.class,
            DataPointsClient.class,
            MetricRegistry.class
        })
public class TsdbModule {
  private static final Logger LOG = LoggerFactory.getLogger(TsdbModule.class);

  private final Config config;

  @Deprecated
  public TsdbModule(final String configFile) {
    this(new File(configFile));
  }

  public TsdbModule(final File configFile) {
    this(ConfigFactory.parseFileAnySyntax(configFile,
        ConfigParseOptions.defaults().setAllowMissing(false)));
  }

  public TsdbModule(final File configFile, final Config overrides) {
    this(ConfigFactory.parseFileAnySyntax(configFile,
        ConfigParseOptions.defaults().setAllowMissing(false)), overrides);
  }

  private TsdbModule(final Config config) {
    this.config = config.withFallback(
        ConfigFactory.parseResourcesAnySyntax("reference"));
    LOG.info("Loaded config from {}", config.origin());
  }

  private TsdbModule(final Config config, final Config overrides) {
    this(overrides.withFallback(config));
  }

  @Provides @Singleton
  Config provideConfig() {
    return config;
  }
}
