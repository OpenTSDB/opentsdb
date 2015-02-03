package net.opentsdb.storage;

import net.opentsdb.stats.Metrics;
import net.opentsdb.utils.Config;

public abstract class StoreDescriptor {
  public abstract TsdbStore createStore(Config config, Metrics metrics);
}
