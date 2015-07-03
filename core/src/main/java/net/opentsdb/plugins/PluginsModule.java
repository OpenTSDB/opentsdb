package net.opentsdb.plugins;

import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchPluginDescriptor;

import com.typesafe.config.Config;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module
public class PluginsModule {
  @Provides
  @Singleton
  SearchPlugin provideSearchPlugin(final Config config) {
    try {
      SearchPluginDescriptor descriptor = PluginLoader.loadSpecificPlugin(
          config.getString("tsd.search.plugin"),
          SearchPluginDescriptor.class);

      return descriptor.create(config);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to instantiate the configured search plugin", e);
    }
  }

  @Provides
  @Singleton
  RealTimePublisher provideRealtimePublisher(final Config config) {
    try {
      RealTimePublisherDescriptor descriptor = PluginLoader.loadSpecificPlugin(
          config.getString("tsd.rtpublisher.plugin"),
          RealTimePublisherDescriptor.class);

      return descriptor.create(config);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to instantiate the configured realtime publisher", e);
    }
  }
}
