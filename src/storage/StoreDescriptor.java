package net.opentsdb.storage;

import net.opentsdb.stats.Metrics;
import com.typesafe.config.Config;

public abstract class StoreDescriptor {
  public abstract TsdbStore createStore(Config config, Metrics metrics);
}
