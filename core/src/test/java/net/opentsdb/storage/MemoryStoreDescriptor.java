package net.opentsdb.storage;

import com.codahale.metrics.MetricRegistry;
import com.google.auto.service.AutoService;
import com.typesafe.config.Config;

@AutoService(StoreDescriptor.class)
public class MemoryStoreDescriptor extends StoreDescriptor {
  @Override
  public TsdbStore createStore(final Config config, final MetricRegistry metrics) {
    return new MemoryStore();
  }
}
