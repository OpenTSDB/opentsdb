package net.opentsdb.storage;

import com.typesafe.config.Config;

public abstract class TestTsdbStore {
  protected TsdbStore store;
  protected Config config;
}
