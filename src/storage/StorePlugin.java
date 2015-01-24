package net.opentsdb.storage;

import net.opentsdb.utils.Config;

public abstract class StorePlugin {
  public abstract TsdbStore createStore(Config config);
}
