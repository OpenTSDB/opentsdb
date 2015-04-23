package net.opentsdb;

import com.typesafe.config.ConfigFactory;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.CoreModule;
import net.opentsdb.core.DataPointsClientExecuteQueryTest;
import net.opentsdb.core.MetaClientAnnotationTest;
import net.opentsdb.core.MetaClientTSMetaTest;
import net.opentsdb.core.PluginsModule;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.StoreModule;
import net.opentsdb.storage.TsdbStore;
import com.typesafe.config.Config;
import net.opentsdb.uid.TestUidFormatter;
import net.opentsdb.uid.TestUidResolver;

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
 * and it will expose this fact. However we want to test a general {@link
 * net.opentsdb.storage.TsdbStore} implementation and not the behavior of the
 * {@link net.opentsdb.storage.MemoryStore}, thus this module should be avoided
 * as much as possible but it is useful in legacy tests.
 *
 * @see net.opentsdb.TestModule
 */
@Module(includes = {
            CoreModule.class,
            PluginsModule.class,
            StoreModule.class
        },
        overrides = true,
        injects = {
            DataPointsClientExecuteQueryTest.class,
            MetaClientAnnotationTest.class,
            MemoryStore.class,
            MetaClientTSMetaTest.class,
            TestUidFormatter.class,
            TestUidResolver.class
        })
public class TestModuleMemoryStore {
  private final Config config;
  private final MemoryStore store;

  public TestModuleMemoryStore(final Config config) {
    this.config = checkNotNull(config);
    this.store = new MemoryStore();
  }

  public TestModuleMemoryStore() {
    this(ConfigFactory.load());
  }

  @Provides
  Config provideConfig() {
    return config;
  }

  @Provides @Singleton
  MemoryStore provideMemoryStore() {
    return store;
  }

  @Provides @Singleton
  TsdbStore provideStore() {
    return store;
  }
}
