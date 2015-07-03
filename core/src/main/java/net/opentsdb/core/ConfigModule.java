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

  public ConfigModule() {
    this(ConfigFactory.load(DEFAULT_PARSE_OPTIONS));
  }

  /**
   * Create a config module that uses the provided Config instance.
   *
   * @param config The config object to read from
   */
  protected ConfigModule(final Config config) {
    this.config = config.withFallback(
        ConfigFactory.parseResourcesAnySyntax("reference", DEFAULT_PARSE_OPTIONS));
    LOG.info("Loaded config from {}", config.origin());
  }

  private ConfigModule(final Config config, final Config overrides) {
    this(overrides.withFallback(config));
  }

  @Nonnull
  public static ConfigModule defaultWithOverrides(final Map<String, ?> overrides) {
    final Config configOverrides = ConfigFactory.parseMap(overrides, "overrides");
    return new ConfigModule(ConfigFactory.load(DEFAULT_PARSE_OPTIONS), configOverrides);
  }

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
