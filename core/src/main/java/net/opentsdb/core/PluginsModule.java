package net.opentsdb.core;

import com.typesafe.config.Config;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.search.DefaultSearchPlugin;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchPluginDescriptor;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.utils.PluginLoader;

import javax.inject.Singleton;

@Module(complete = false,
        library = true)
public class PluginsModule {
  @Provides
  @Singleton
  SearchPlugin provideSearchPlugin(final Config config,
                                   final TsdbStore store) {
    try {
      // load the search plugin if enabled
      if (config.getBoolean("tsd.search.enable")) {
        SearchPluginDescriptor descriptor = PluginLoader.loadSpecificPlugin(
            config.getString("tsd.search.plugin"),
            SearchPluginDescriptor.class);

        return descriptor.create(config);
      }

      return new DefaultSearchPlugin(store);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to instantiate the configured search plugin", e);
    }
  }

  @Provides
  @Singleton
  RTPublisher provideRealtimePublisher(final Config config) {
    try {
      // load the realtime publisher plugin if enabled
      if (config.getBoolean("tsd.rtpublisher.enable")) {
        RTPublisherDescriptor descriptor = PluginLoader.loadSpecificPlugin(
            config.getString("tsd.rtpublisher.plugin"),
            RTPublisherDescriptor.class);

        return descriptor.create(config);
      }

      return new DefaultRealtimePublisher();
    } catch (Exception e) {
      throw new IllegalStateException("Unable to instantiate the configured realtime publisher", e);
    }
  }
}
