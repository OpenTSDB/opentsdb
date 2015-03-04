package net.opentsdb.idmanager;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.TsdbModule;
import com.typesafe.config.Config;

import javax.inject.Singleton;
import java.io.File;

@Module(includes = TsdbModule.class,
    injects = {
        Assign.class
    })
public class AssignModule {
  private final Config config;

  public AssignModule(final File configFile) {
    this(ConfigFactory.parseFile(configFile,
        ConfigParseOptions.defaults().setAllowMissing(false)));
  }

  public AssignModule(final File configFile, final Config overrides) {
    this(ConfigFactory.parseFile(configFile,
        ConfigParseOptions.defaults().setAllowMissing(false)), overrides);
  }

  private AssignModule(final Config config) {
    this.config = config.withFallback(
        ConfigFactory.parseResourcesAnySyntax("reference"));
  }

  private AssignModule(final Config config, final Config overrides) {
    this(overrides.withFallback(config));
  }

  @Provides @Singleton
  Config provideConfig() {
    return config;
  }
}
