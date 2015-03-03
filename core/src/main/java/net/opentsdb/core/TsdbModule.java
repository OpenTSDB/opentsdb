package net.opentsdb.core;

import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.EventBus;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.search.DefaultSearchPlugin;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchPluginDescriptor;
import net.opentsdb.storage.StoreModule;
import net.opentsdb.storage.TsdbStore;
import com.typesafe.config.Config;
import net.opentsdb.utils.PluginLoader;

/**
 * This is the main dagger module for the TSDB core library. It is not complete
 * however, it needs to be complemented with an extending module that provides a
 * {@link com.typesafe.config.Config}, a {@link net.opentsdb.storage.TsdbStore}
 * and a {@link com.codahale.metrics.MetricRegistry}.
 */
@Module(library = true,
        includes = {
                StoreModule.class
        },
        complete = false,
        injects = {
                TSDB.class,
                UniqueIdClient.class,
                TreeClient.class,
                MetaClient.class,
                DataPointsClient.class
        })
public class TsdbModule {
  @Provides
  EventBus provideEventBus() {
    return new EventBus();
  }

  @Provides
  MetricRegistry provideMetricRegistry() {
    return new MetricRegistry();
  }

  @Provides
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
