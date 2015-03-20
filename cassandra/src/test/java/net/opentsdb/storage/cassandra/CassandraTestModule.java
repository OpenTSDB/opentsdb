package net.opentsdb.storage.cassandra;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.CoreModule;
import net.opentsdb.core.PluginsModule;
import net.opentsdb.storage.StoreDescriptor;
import net.opentsdb.storage.StoreModule;

import javax.inject.Singleton;

/**
 * This is the dagger module that should be used by all Cassandra tests. It
 * provides a live CassandraStore as its TsdbStore.
 *
 * @see net.opentsdb.core.TsdbModule
 * @see net.opentsdb.TestModule
 */
@Module(includes = {
            CoreModule.class,
            PluginsModule.class,
            StoreModule.class
        },
        injects = {
            TestCassandraStore.class
        })
class CassandraTestModule {
  @Provides
  Config provideConfig() {
    return ConfigFactory.load("cassandra");
  }

  @Provides @Singleton
  CassandraStoreDescriptor provideCassandraStoreDescriptor() {
    return new CassandraStoreDescriptor();
  }
}
