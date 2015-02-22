package net.opentsdb.storage.cassandra;

import com.datastax.driver.core.Cluster;
import com.google.auto.service.AutoService;
import com.google.common.net.HostAndPort;
import net.opentsdb.core.InvalidConfigException;
import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.StoreDescriptor;
import com.typesafe.config.Config;

import java.util.List;

@AutoService(StoreDescriptor.class)
public class CassandraStoreDescriptor extends StoreDescriptor {
  /**
   * Create a new cluster that is configured to use the list of addresses in the
   * config as seed nodes.
   *
   * @param config The config to get the list of addresses to from
   * @return A new {@link com.datastax.driver.core.Cluster} instance
   * @throws com.typesafe.config.ConfigException      if the config key was
   *                                                  missing or is malformed
   * @throws net.opentsdb.core.InvalidConfigException if one of the addresses
   *                                                  could not be parsed
   */
  private Cluster createCluster(final Config config) {
    try {
      return createCluster(config.getStringList("tsd.storage.cassandra.nodes"));
    } catch (IllegalArgumentException e) {
      throw new InvalidConfigException(config.getValue("tsd.storage.cassandra.nodes"),
              "One or more of the addresses in the cassandra config could not be parsed");
    }
  }

  /**
   * Create a new cluster that is configured to use the provided list of string
   * addresses as seed nodes.
   *
   * @param nodes A list of addresses to use as seed nodes
   * @return A new {@link com.datastax.driver.core.Cluster} instance
   * @throws java.lang.IllegalArgumentException If any of the addresses could
   *                                            not be parsed
   */
  private Cluster createCluster(final List<String> nodes) {
    final Cluster.Builder builder = Cluster.builder();

    for (final String node : nodes) {
      final HostAndPort host = HostAndPort.fromString(node)
              .withDefaultPort(CassandraConst.DEFAULT_CASSANDRA_PORT);

      builder.addContactPoint(host.getHostText())
              .withPort(host.getPort());
    }

    return builder.build();
  }

  @Override
  public CassandraStore createStore(final Config config,
                                    final Metrics metrics) {
    final Cluster cluster = createCluster(config);
    registerMetrics(cluster, metrics);
    return new CassandraStore(cluster);
  }

  /**
   * Register the metrics that are exposed on the provided {@link
   * com.datastax.driver.core.Cluster} with the provided {@link
   * net.opentsdb.stats.Metrics} instance.
   */
  private void registerMetrics(final Cluster cluster, final Metrics metrics) {
    metrics.getRegistry().registerAll(cluster.getMetrics().getRegistry());
  }
}
