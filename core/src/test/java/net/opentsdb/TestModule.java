package net.opentsdb;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.CoreModule;
import net.opentsdb.core.MetaClientUIDMetaTest;
import net.opentsdb.core.PluginsModule;
import net.opentsdb.core.UniqueIdClientTest;
import net.opentsdb.storage.StoreModule;
import net.opentsdb.storage.StoreModuleTest;

import javax.inject.Singleton;

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
@Module(includes = {
            CoreModule.class,
            PluginsModule.class,
            StoreModule.class
        },
        injects = {
            MetaClientUIDMetaTest.class,
            UniqueIdClientTest.class,
            StoreModuleTest.class
        })
public class TestModule {
  private final Config config;

  public TestModule(final Config overrides) {
    this.config = overrides.withFallback(ConfigFactory.load());
  }

  public TestModule() {
    this.config = ConfigFactory.load();
  }

  @Provides
  @Singleton
  Config provideConfig() {
    return config;
  }
}
