package net.opentsdb.storage;


import com.google.common.base.Strings;

import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.TsdbModule;
import net.opentsdb.stats.Metrics;
import net.opentsdb.utils.Config;

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
  TsdbStore provideStore(final StoreDescriptor storeDescriptor,
                         final Config config,
                         final Metrics metrics) {
    return storeDescriptor.createStore(config, metrics);
  }

  /**
   * Get the {@link net.opentsdb.storage.StoreDescriptor} that the configuration specifies. This method
   * will create a new instance on each call.
   *
   * @return This method will return a ready to use {@link TsdbStore} object.
   * No guarantee is made that it will connect properly to the database but
   * it will be configured according to the config class sent in when this
   * object was created.
   */
  @Provides
  StoreDescriptor provideStoreDescriptor(final Config config,
                                         final Iterable<StoreDescriptor> storePlugins) {
    String adapter_type = config.getString("tsd.storage.adapter");
    if (Strings.isNullOrEmpty(adapter_type)) {
      throw new IllegalArgumentException("The config could not find the" +
          " field 'tsd.storage.adapter', please make sure it was " +
          "configured correctly.");
    }

    for (final StoreDescriptor storeDescriptor : storePlugins) {
      String pluginName = storeDescriptor.getClass().getCanonicalName();

      if (pluginName.equals(adapter_type))
        return storeDescriptor;
    }

    throw new IllegalArgumentException("The config could not find a valid" +
        " value for the field 'tsd.storage.adapter', please make sure" +
        " it was configured correctly. It was '" + adapter_type + "'.");
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
