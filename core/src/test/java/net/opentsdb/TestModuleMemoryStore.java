package net.opentsdb;

import static com.google.common.base.Preconditions.checkNotNull;

import net.opentsdb.core.CoreModule;
import net.opentsdb.core.MetaClientAnnotationTest;
import net.opentsdb.plugins.PluginsModule;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.StoreModule;
import net.opentsdb.storage.TsdbStore;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

/**
 * A dagger module that inherits from {@link net.opentsdb.core.TsdbModule} and both overrides it and
 * complements it.
 *
 * <p>This module complements the {@link net.opentsdb.core.TsdbModule} by providing a config.
 *
 * <p>The module will return an instance of {@link net.opentsdb.storage.MemoryStore} and it will
 * expose this fact. However we want to test a general {@link net.opentsdb.storage.TsdbStore}
 * implementation and not the behavior of the {@link net.opentsdb.storage.MemoryStore}, thus this
 * module should be avoided as much as possible but it is useful in legacy tests.
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
        MetaClientAnnotationTest.class,
        MemoryStore.class
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

  @Provides
  @Singleton
  MemoryStore provideMemoryStore() {
    return store;
  }

  @Provides
  @Singleton
  TsdbStore provideStore() {
    return store;
  }
}
