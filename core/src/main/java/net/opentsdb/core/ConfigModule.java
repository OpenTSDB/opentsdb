package net.opentsdb.core;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import dagger.Module;
import dagger.Provides;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import javax.annotation.Nonnull;
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
  private static final ConfigParseOptions DEFAULT_PARSE_OPTIONS = ConfigParseOptions.defaults()
      .setAllowMissing(false);

  private final Config config;

  /**
   * Create a new config module that will provide a configuration loaded from the default
   * (application) file as dictated by the config library.
   */
  public ConfigModule() {
    this(ConfigFactory.load(DEFAULT_PARSE_OPTIONS));
  }

  /**
   * Create a new config module that will provide the given config instance to the dagger object
   * graph.
   *
   * @param config The config object that will be provided
   */
  protected ConfigModule(final Config config) {
    this.config = config.withFallback(
        ConfigFactory.parseResourcesAnySyntax("reference", DEFAULT_PARSE_OPTIONS));
    LOG.info("Loaded config from {}", config.origin());
  }

  private ConfigModule(final Config config, final Config overrides) {
    this(overrides.withFallback(config));
  }

  /**
   * Create a new config module that will provide a configuration loaded from the default
   * (application) file as dictated by the config library with the provided configuration values
   * overridden.
   *
   * @param overrides The configuration values that should override the default ones
   * @return A newly instantiated config module
   * @throws com.typesafe.config.ConfigException.IO if the default config is missing
   */
  @Nonnull
  public static ConfigModule defaultWithOverrides(final Map<String, ?> overrides) {
    final Config configOverrides = ConfigFactory.parseMap(overrides, "overrides");
    return new ConfigModule(ConfigFactory.load(DEFAULT_PARSE_OPTIONS), configOverrides);
  }

  /**
   * Create a new config module that will provide a configuration that has been loaded from the
   * provided file.
   *
   * @param configFile A file object that points to the configuration file to load
   * @return A newly instantiated config module
   * @throws com.typesafe.config.ConfigException.IO if the file is missing
   */
  @Nonnull
  public static ConfigModule fromFile(final File configFile) {
    return new ConfigModule(ConfigFactory.parseFileAnySyntax(configFile, DEFAULT_PARSE_OPTIONS));
  }

  @Provides
  @Singleton
  public Config provideConfig() {
    return config;
  }
}
