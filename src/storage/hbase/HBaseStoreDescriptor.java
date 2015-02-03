package net.opentsdb.storage.hbase;

import com.codahale.metrics.MetricRegistry;
import com.google.auto.service.AutoService;

import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.StoreDescriptor;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;

@AutoService(StoreDescriptor.class)
public class HBaseStoreDescriptor extends StoreDescriptor {
  @Override
  public TsdbStore createStore(final Config config, final Metrics metrics) {
    final HBaseClient client = createHBaseClient(config);
    final HBaseStore store = new HBaseStore(client, config);

    MetricRegistry registry = metrics.getRegistry();
    registry.registerAll(new HBaseClientStats(client));
    registry.registerAll(new CompactionQueue
            .CompactionQueueMetrics(store.getCompactionQueue()));

    return store;
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
