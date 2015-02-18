package net.opentsdb;

import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.DataPointsClientExecuteQueryTest;
import net.opentsdb.core.MetaClientAnnotationTest;
import net.opentsdb.core.MetaClientTSMetaTest;
import net.opentsdb.core.TsdbModule;
import net.opentsdb.search.TestTimeSeriesLookup;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.tools.TestDumpSeries;
import net.opentsdb.tools.TestFsck;
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
 * and it will expose this fact. However we want to test a general {@link
 * net.opentsdb.storage.TsdbStore} implementation and not the behavior of the
 * {@link net.opentsdb.storage.MemoryStore}, thus this module should be avoided
 * as much as possible but it is useful in legacy tests.
 *
 * @see net.opentsdb.TestModule
 */
@Module(includes = TsdbModule.class,
        overrides = true,
        injects = {
                DataPointsClientExecuteQueryTest.class,
                MetaClientAnnotationTest.class,
                MemoryStore.class,
                MetaClientTSMetaTest.class,
                TestTimeSeriesLookup.class,
                TestDumpSeries.class,
                TestFsck.class
        })
public class TestModuleMemoryStore {
  private final Config config;
  private final MemoryStore store;

  public TestModuleMemoryStore(final Config config) {
    this.config = checkNotNull(config);
    this.store = new MemoryStore();
  }

  public TestModuleMemoryStore() {
    this(new Config());
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
