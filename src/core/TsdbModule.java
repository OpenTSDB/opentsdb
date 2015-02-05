package net.opentsdb.core;

import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.EventBus;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.search.DefaultSearchPlugin;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchPluginDescriptor;
import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.StoreDescriptor;
import net.opentsdb.storage.StoreSupplier;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.tsd.RTPublisherDescriptor;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;

import java.util.ServiceLoader;

@Module(library = true,
        injects = {
                TSDB.class,
                UniqueIdClient.class,
                TreeClient.class,
                MetaClient.class,
                DataPointsClient.class
        })
public class TsdbModule {
  @Provides
  MetricRegistry provideMetricRegistry() {
    return new MetricRegistry();
  }

  @Provides
  EventBus provideEventBus() {
    return new EventBus();
  }

  @Provides
  Metrics provideMetrics(final MetricRegistry registry) {
    return new Metrics(registry);
  }

  @Provides
  TsdbStore provideStore(final Config config,
                         final Metrics metrics) {
    StoreSupplier storeSupplier = new StoreSupplier(config,
            ServiceLoader.load(StoreDescriptor.class), metrics);
    return storeSupplier.get();
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
