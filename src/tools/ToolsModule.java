package net.opentsdb.tools;

import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.TsdbModule;
import net.opentsdb.utils.Config;

import javax.inject.Singleton;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is the main dagger entrypoint for all tools.
 *
 * @see net.opentsdb.core.TsdbModule
 */
@Module(includes = TsdbModule.class,
        injects = {
                Config.class
        })
public class ToolsModule {
  private final ArgP argp;

  public ToolsModule(final ArgP argp) {
    this.argp = checkNotNull(argp);
  }

  @Provides @Singleton
  Config provideConfig() {
    try {
      return CliOptions.getConfig(argp);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to load config");
    }
  }
}
