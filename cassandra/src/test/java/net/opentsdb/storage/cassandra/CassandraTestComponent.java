package net.opentsdb.storage.cassandra;

import net.opentsdb.core.CoreModule;
import net.opentsdb.plugins.PluginsModule;
import net.opentsdb.storage.StoreDescriptor;
import net.opentsdb.storage.StoreModule;

import com.typesafe.config.Config;
import dagger.Component;

import javax.inject.Singleton;

/**
 * This is the dagger module that should be used by all Cassandra tests. It provides a live
 * CassandraStore as its TsdbStore.
 *
 * @see net.opentsdb.core.TsdbModule
 * @see net.opentsdb.TestComponent
 */
@Component(
    modules = {
        CoreModule.class,
        PluginsModule.class,
        StoreModule.class,
        CassandraConfigModule.class
    })
@Singleton
interface CassandraTestComponent {
  Config config();

  StoreDescriptor storeDescriptor();

  void inject(CassandraTimeSeriesQueryTest cassandraTimeSeriesQueryTest);
}
