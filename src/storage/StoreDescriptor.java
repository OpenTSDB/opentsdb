package net.opentsdb.storage;

import net.opentsdb.utils.Config;

public abstract class StoreDescriptor {
  public abstract TsdbStore createStore(Config config);
}
