package net.opentsdb.storage.hbase;

import com.google.auto.service.AutoService;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.StoreDescriptor;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;

@AutoService(StoreDescriptor.class)
public class HBaseStoreDescriptor extends StoreDescriptor {
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
