package net.opentsdb.tools;

import com.codahale.metrics.MetricRegistry;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.TsdbModule;
import net.opentsdb.utils.Config;

import javax.inject.Singleton;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

@Module(includes = TsdbModule.class)
public class ToolsModule {
  private final ArgP argp;

  public ToolsModule(final ArgP argp) {
    this.argp = checkNotNull(argp);
  }

  @Provides
  MetricRegistry provideMetricRegistry() {
    return new MetricRegistry();
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
