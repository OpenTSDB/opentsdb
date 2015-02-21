package net.opentsdb.storage.cassandra;

import com.datastax.driver.core.Cluster;
import com.google.auto.service.AutoService;
import com.google.common.net.HostAndPort;
import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.StoreDescriptor;
import com.typesafe.config.Config;

import java.util.List;

/**
 * Use this to create a CassandraStore object. Will throw
 * IllegalArgumentException if there was an error in the config.
 */
@AutoService(StoreDescriptor.class)
public class CassandraStoreDescriptor extends StoreDescriptor {

  @Override
  public CassandraStore createStore(final Config config,
                                    final Metrics metrics) {
    Cluster.Builder builder = Cluster.builder();

    List<String> nodes = config.getStringList("tsd.storage.cassandra.nodes");

    for (String node : nodes) {
      try {
        HostAndPort host = HostAndPort.fromString(node)
                .withDefaultPort(CassandraConst.DEFAULT_CASSANDRA_PORT);

        builder.addContactPoint(host.getHostText())
                .withPort(host.getPort());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("There was an error in the" +
                " configuration file in the field 'tsd.storage" +
                ".cassandra.clusters'.", e);
      }
    }

    Cluster cluster = builder.build();

    registerMetrics(cluster, metrics);

    return new CassandraStore(cluster);
  }

  private void registerMetrics(final Cluster cluster, final Metrics metrics) {
    metrics.getRegistry().registerAll(cluster.getMetrics().getRegistry());
  }
}
