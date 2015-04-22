package net.opentsdb.core;

import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(complete = false,
        library = true,
        injects = {
            Config.class,
            TSDB.class,
            UniqueIdClient.class,
            MetaClient.class,
            DataPointsClient.class
        })
public class CoreModule {
  @Provides
  @Singleton
  EventBus provideEventBus() {
    return new EventBus();
  }

  @Provides
  @Singleton
  MetricRegistry provideMetricRegistry() {
    return new MetricRegistry();
  }
}
