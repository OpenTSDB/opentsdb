package net.opentsdb.storage.hbase;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableMap;
import org.hbase.async.HBaseClient;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.opentsdb.stats.Metrics.name;
import static net.opentsdb.stats.Metrics.tag;

class HBaseClientStats implements MetricSet {
  private HBaseClient client;

  HBaseClientStats(HBaseClient client) {
    this.client = checkNotNull(client);
  }

  @Override
  public Map<String, Metric> getMetrics() {
    final ImmutableMap.Builder<String, Metric> metrics = ImmutableMap.builder();

    metrics.put(name("hbase.root_lookups"), new RootLookupsGauge());
    metrics.put(name("hbase.meta_lookups", tag("type", "uncontended")),
        new UncontendedMetaLookups());
    metrics.put(name("hbase.meta_lookups", tag("type", "contended")),
        new ContendedMetaLookups());
    metrics.put(name("hbase.rpcs", tag("type", "increment")),
        new AtomicIncrements());
    metrics.put(name("hbase.rpcs", tag("type", "delete")), new Deletes());
    metrics.put(name("hbase.rpcs", tag("type", "get")), new Gets());
    metrics.put(name("hbase.rpcs", tag("type", "put")), new Puts());
    metrics.put(name("hbase.rpcs", tag("type", "rowLock")), new RowLocks());
    metrics.put(name("hbase.rpcs", tag("type", "openScanner")),
        new ScannersOpened());
    metrics.put(name("hbase.rpcs", tag("type", "scan")), new Scans());

    metrics.put("hbase.rpcs.batched", new NumBatchedRpcSent());
    metrics.put("hbase.flushes", new Flushes());
    metrics.put("hbase.connections.created", new ConnectionsCreated());
    metrics.put("hbase.nsre", new NoSuchRegionExceptions());
    metrics.put("hbase.nsre.rpcs_delayed", new NumRpcDelayedDueToNSRE());

    return metrics.build();
  }

  private class RootLookupsGauge implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().rootLookups();
    }
  }

  private class UncontendedMetaLookups implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().uncontendedMetaLookups();
    }
  }

  private class ContendedMetaLookups implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().contendedMetaLookups();
    }
  }

  private class AtomicIncrements implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().atomicIncrements();
    }
  }

  private class Deletes implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().deletes();
    }
  }

  private class Gets implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().gets();
    }
  }

  private class Puts implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().puts();
    }
  }

  private class RowLocks implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().rowLocks();
    }
  }

  private class ScannersOpened implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().scannersOpened();
    }
  }

  private class Scans implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().scans();
    }
  }

  private class NumBatchedRpcSent implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().numBatchedRpcSent();
    }
  }

  private class Flushes implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().flushes();
    }
  }

  private class ConnectionsCreated implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().connectionsCreated();
    }
  }

  private class NoSuchRegionExceptions implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().noSuchRegionExceptions();
    }
  }

  private class NumRpcDelayedDueToNSRE implements Gauge<Long> {
    @Override
    public Long getValue() {
      return client.stats().numRpcDelayedDueToNSRE();
    }
  }
}
