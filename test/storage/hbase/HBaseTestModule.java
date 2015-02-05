package net.opentsdb.storage.hbase;

import com.codahale.metrics.MetricRegistry;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.TsdbModule;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.utils.Config;
import org.hbase.async.HBaseClient;

import java.io.IOException;

@Module(includes = TsdbModule.class,
        overrides = true)
class HBaseTestModule {
  private final HBaseClient client;

  HBaseTestModule(final HBaseClient client) {
    this.client = client;
  }

  @Provides
  Config provideConfig() {
    try {
      return new Config(false);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to load config", e);
    }
  }

  @Provides
  MetricRegistry provideMetricRegistry() {
    return new MetricRegistry();
  }

  @Provides
  TsdbStore provideStore(final Config config) {
    return new HBaseStore(client, config);
  }
}
