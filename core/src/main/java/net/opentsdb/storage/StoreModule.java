package net.opentsdb.storage;

import com.codahale.metrics.MetricRegistry;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.InvalidConfigException;
import com.typesafe.config.Config;

import javax.inject.Singleton;
import java.util.ServiceLoader;

/**
 * Use this class to create a TsdbStore instance. Given a config file and an
 * iterable with store plugin this class will set up an instance of the
 * configured store.
 */
@Module(complete = false,
        library = true,
        injects = TsdbStore.class)
public class StoreModule {
  @Provides
  @Singleton
  TsdbStore provideStore(final StoreDescriptor storeDescriptor,
                         final Config config,
                         final MetricRegistry metrics) {
    return storeDescriptor.createStore(config, metrics);
  }

  /**
   * Get the {@link net.opentsdb.storage.StoreDescriptor} that the configuration
   * specifies. This method will create a new instance on each call.
   *
   * @return This method will return a ready to use {@link TsdbStore} object. No
   * guarantee is made that it will connect properly to the database but it will
   * be configured according to the config provided config.
   */
  @Provides
  @Singleton
  StoreDescriptor provideStoreDescriptor(final Config config,
                                         final Iterable<StoreDescriptor> storePlugins) {
    String adapter_type = config.getString("tsd.storage.adapter");

    for (final StoreDescriptor storeDescriptor : storePlugins) {
      String pluginName = storeDescriptor.getClass().getCanonicalName();

      if (pluginName.equals(adapter_type))
        return storeDescriptor;
    }

    throw new InvalidConfigException(config.getValue("tsd.storage.adapter"),
            "Found no storage adapter that matches '" + adapter_type + "'");
  }

  /**
   * Provides an iterable with all {@link net.opentsdb.storage.StoreDescriptor}s
   * that are registered as services and thus are found by the {@link
   * java.util.ServiceLoader}
   */
  @Provides
  Iterable<StoreDescriptor> provideStoreDescriptors() {
    return ServiceLoader.load(StoreDescriptor.class);
  }
}
