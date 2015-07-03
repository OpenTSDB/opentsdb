package net.opentsdb.storage;

import com.typesafe.config.Config;

public abstract class TsdbStoreTest<K extends TsdbStore> {
  protected K store;
  protected Config config;
}
