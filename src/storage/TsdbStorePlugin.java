package net.opentsdb.storage;

import net.opentsdb.utils.Config;

public abstract class TsdbStorePlugin {
  public abstract String identifier();

  public abstract TsdbStore createStore(Config config);
}
