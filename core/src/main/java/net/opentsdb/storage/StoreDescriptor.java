package net.opentsdb.storage;

import com.codahale.metrics.MetricRegistry;
import com.typesafe.config.Config;

public abstract class StoreDescriptor {
  public abstract TsdbStore createStore(Config config, MetricRegistry metrics);
}
