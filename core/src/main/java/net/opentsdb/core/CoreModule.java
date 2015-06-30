package net.opentsdb.core;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.google.common.eventbus.EventBus;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module
public class CoreModule {
  @Provides
  @Singleton
  EventBus provideEventBus() {
    return new EventBus();
  }

  @Provides
  @Singleton
  MetricRegistry provideMetricRegistry() {
    MetricRegistry registry = new MetricRegistry();
    registry.registerAll(new ClassLoadingGaugeSet());
    registry.registerAll(new GarbageCollectorMetricSet());
    registry.registerAll(new MemoryUsageGaugeSet());
    registry.registerAll(new ThreadStatesGaugeSet());
    registry.register("descriptor-usage", new FileDescriptorRatioGauge());
    return registry;
  }
}
