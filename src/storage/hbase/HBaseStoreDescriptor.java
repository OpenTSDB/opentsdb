package net.opentsdb.storage.hbase;

import com.codahale.metrics.MetricRegistry;
import com.google.auto.service.AutoService;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.StoreDescriptor;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;

import static net.opentsdb.stats.Metrics.name;
import static net.opentsdb.stats.Metrics.tag;

@AutoService(StoreDescriptor.class)
public class HBaseStoreDescriptor extends StoreDescriptor {
  @Override
  public TsdbStore createStore(final Config config, final Metrics metrics) {
    final HBaseClient client = createHBaseClient(config);
    checkNecessaryTablesExist(client, config);

    final HBaseStore store = new HBaseStore(client, config);

    MetricRegistry registry = metrics.getRegistry();
    registry.registerAll(new HBaseClientStats(client));
    registry.registerAll(new CompactionQueue
            .CompactionQueueMetrics(store.getCompactionQueue()));

    registerIdUsageGuages(client, registry, config);

    return store;
  }

  /**
   * Use this method when you want to create a new HBaseStore object.
   *
   * @return Returns a HBaseStore object ready for use.
   */
  private HBaseClient createHBaseClient(final Config config) {
    HBaseClient client = new HBaseClient(
            config.getString("tsd.storage.hbase.zk_quorum"),
            config.getString("tsd.storage.hbase.zk_basedir"));

    client.setFlushInterval(config.getShort("tsd.storage.flush_interval"));

    return client;
  }


  private void checkNecessaryTablesExist(final HBaseClient client,
                                         final Config config) {
    final boolean enable_tree_processing = config.enable_tree_processing();
    final boolean enable_realtime_ts = config.enable_realtime_ts();
    final boolean enable_realtime_uid = config.enable_realtime_uid();
    final boolean enable_tsuid_incrementing = config.enable_tsuid_incrementing();

    Deferred<Object> d = checkTableExists(client,
            config.getString("tsd.storage.hbase.data_table"));

    d = checkTableExists(client,
            config.getString("tsd.storage.hbase.uid_table"), d);

    if (enable_tree_processing) {
      d = checkTableExists(client,
              config.getString("tsd.storage.hbase.tree_table"), d);
    }
    if (enable_realtime_ts ||
        enable_realtime_uid ||
        enable_tsuid_incrementing) {
      d = checkTableExists(client,
              config.getString("tsd.storage.hbase.meta_table"), d);
    }

    try {
      d.joinUninterruptibly();
    } catch (Exception e) {
      throw new IllegalStateException("One or more tables are probably missing", e);
    }
  }

  private Deferred<Object> checkTableExists(final HBaseClient client,
                                            final String table) {
    return client.ensureTableExists(table.getBytes(HBaseConst.CHARSET));
  }

  private Deferred<Object> checkTableExists(final HBaseClient client,
                                            final String table,
                                            Deferred<Object> d) {
    return d.addCallbackDeferring(new Callback<Deferred<Object>, Object>() {
      @Override
      public Deferred<Object> call(final Object arg) throws Exception {
        return checkTableExists(client, table);
      }
    });
  }

  /**
   * Register ID usage guages for all {@link net.opentsdb.uid.UniqueIdType} on
   * the provided registry.
   *
   * @param client    The client to use for communication with HBase
   * @param registry  The registry to register the gauges on
   * @param config    A config instance used to looking up which table to look in.
   */
  private void registerIdUsageGuages(final HBaseClient client,
                                     final MetricRegistry registry,
                                     final Config config) {
    final byte[] table = config.getString("tsd.storage.hbase.uid_table")
            .getBytes(HBaseConst.CHARSET);

    registerIdUsageGauge(client, registry, table, UniqueIdType.METRIC);
    registerIdUsageGauge(client, registry, table, UniqueIdType.TAGK);
    registerIdUsageGauge(client, registry, table, UniqueIdType.TAGV);
  }

  /**
   * Register ID usage guages for a single {@link net.opentsdb.uid.UniqueIdType}
   * on the provided registry.
   *
   * @param client   The client to use for communication with HBase
   * @param registry The registry to register the gauges on
   * @param table    The table to look for ID usage information in
   * @param type     The type of IDs to register a gauge for
   */
  private void registerIdUsageGauge(final HBaseClient client,
                                    final MetricRegistry registry,
                                    final byte[] table,
                                    final UniqueIdType type) {
    Metrics.Tag typeTag = tag("kind", type.toValue());

    UsedIdsGauge usedIdsGauge = new UsedIdsGauge(type, client, table);
    registry.register(name("uid.ids-used", typeTag), usedIdsGauge);

    registry.register(name("uid.ids-available", typeTag),
            new AvailableIdsGauge(usedIdsGauge, type.width));
  }
}
