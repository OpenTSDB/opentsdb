package net.opentsdb.storage.cassandra;

import net.opentsdb.core.CoreModule;
import net.opentsdb.plugins.PluginsModule;
import net.opentsdb.storage.StoreDescriptor;
import net.opentsdb.storage.StoreModule;

import com.typesafe.config.Config;
import dagger.Component;

import javax.inject.Singleton;

/**
 * A dagger component that is configured to load a live cassandra store for use in the cassandra
 * test suites.
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
