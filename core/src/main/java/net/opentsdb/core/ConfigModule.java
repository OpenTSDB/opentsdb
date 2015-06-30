package net.opentsdb.core;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import dagger.Module;
import dagger.Provides;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import javax.inject.Singleton;

/**
 * A dagger module that provides a config object that is loaded from the indicated file or with
 * optional overrides.
 *
 * <p>This class may be extended to provide a module that can load other files by default instead of
 * the default "application" one dictated by the config library we use by overriding {@link
 * #ConfigModule()}.
 */
@Module
public class ConfigModule {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigModule.class);

  protected final Config config;

  public ConfigModule(final File configFile) {
    this(ConfigFactory.parseFileAnySyntax(configFile,
        ConfigParseOptions.defaults().setAllowMissing(false)));
  }

  public ConfigModule(final File configFile, final Config overrides) {
    this(ConfigFactory.parseFileAnySyntax(configFile,
        ConfigParseOptions.defaults().setAllowMissing(false)), overrides);
  }

  public ConfigModule() {
    this(ConfigFactory.load());
  }

  /**
   * Create a config module that uses the provided Config instance.
   *
   * @param config The config object to read from
   */
  protected ConfigModule(final Config config) {
    this.config = config.withFallback(
        ConfigFactory.parseResourcesAnySyntax("reference"));
    LOG.info("Loaded config from {}", config.origin());
  }

  private ConfigModule(final Config config, final Config overrides) {
    this(overrides.withFallback(config));
  }

  @Provides
  @Singleton
  Config provideConfig() {
    return config;
  }
}
