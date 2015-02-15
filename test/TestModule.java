package net.opentsdb;

import com.codahale.metrics.MetricRegistry;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.MetaClientUIDMetaTest;
import net.opentsdb.core.TreeClientTest;
import net.opentsdb.core.TsdbModule;
import net.opentsdb.core.UniqueIdClientTest;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.utils.Config;

import javax.inject.Singleton;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A dagger module that inherits from {@link net.opentsdb.core.TsdbModule} and
 * both overrides it and complements it.
 *
 * This module complements the {@link net.opentsdb.core.TsdbModule} by providing
 * a config.
 *
 * The module will return an instance of {@link net.opentsdb.storage.MemoryStore}
 * but it will not expose this, it is instead exposed as a regular {@link
 * net.opentsdb.storage.TsdbStore}. This detail is important as we want to test
 * a general {@link net.opentsdb.storage.TsdbStore} implementation and not the
 * behavior of the {@link net.opentsdb.storage.MemoryStore}. Because of this tests
 * should always strive to use this module as a base.
 *
 * @see net.opentsdb.TestModuleMemoryStore
 */
@Module(includes = TsdbModule.class,
        overrides = true,
        injects = {
                MetaClientUIDMetaTest.class,
                TreeClientTest.class,
                UniqueIdClientTest.class
        })
public class TestModule {
  private final Config config;

  public TestModule(final Config config) {
    this.config = checkNotNull(config);
  }

  public TestModule() {
    this(new Config());
  }

  @Provides
  Config provideConfig() {
    return config;
  }

  @Provides
  MetricRegistry provideMetricRegistry() {
    return new MetricRegistry();
  }

  @Provides @Singleton
  TsdbStore provideStore() {
    return new MemoryStore();
  }
}
