package net.opentsdb.storage.cassandra;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.TsdbModule;
import net.opentsdb.storage.StoreDescriptor;

import javax.inject.Singleton;

/**
 * This is the dagger module that should be used by all Cassandra tests. It
 * provides a live CassandraStore as its TsdbStore.
 *
 * @see net.opentsdb.core.TsdbModule
 * @see net.opentsdb.TestModule
 */
@Module(includes = TsdbModule.class,
        overrides = true,
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

  @Provides @Singleton
  StoreDescriptor provideStoreDescriptor() {
    return provideCassandraStoreDescriptor();
  }
}
