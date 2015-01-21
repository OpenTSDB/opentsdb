package net.opentsdb.storage.hbase;

import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.TsdbStorePlugin;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;

public class HBaseStorePlugin extends TsdbStorePlugin {
  @Override
  public String identifier() {
    return "HBase";
  }

  @Override
  public TsdbStore createStore(final Config config) {
    return new HBaseStore(createHBaseClient(config), config);
  }

  /**
   * Use this method when you want to create a new HBaseStore object.
   *
   * @return Returns a HBaseStore object ready for use.
   */
  private HBaseClient createHBaseClient(final Config config) {
    return new HBaseClient(
                    config.getString("tsd.storage.hbase.zk_quorum"),
                    config.getString("tsd.storage.hbase.zk_basedir"));
  }
}
