package net.opentsdb.tsd;

import java.util.Map;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;

import static net.opentsdb.stats.Metrics.name;
import static net.opentsdb.stats.Metrics.tag;

class TsdStats {
  private final GraphHandlerStats graphHandlerStats;
  private final PutDataPointRpcStats putDataPointRpcStats;
  private final ConnectionManagerStats connectionManagerStats;

  private final Counter telnet_rpcs_received;
  private final Counter http_rpcs_received;
  private final Counter exceptions_caught;

  /**
   * Keep track of the latency of HTTP requests.
   */
  private final Timer httplatency;

  TsdStats(final MetricRegistry registry) {
    httplatency = registry.timer(name("http.latency", tag("type", "all")));

    telnet_rpcs_received = registry.counter(name("rpc.received", tag("type", "telnet")));
    http_rpcs_received = registry.counter(name("rpc.received", tag("type", "http")));
    exceptions_caught = registry.counter(name("rpc.exceptions"));

    putDataPointRpcStats = new PutDataPointRpcStats(registry);
    graphHandlerStats = new GraphHandlerStats(registry);
    connectionManagerStats = new ConnectionManagerStats(registry);
  }

  public Timer getHttplatency() {
    return httplatency;
  }

  public Counter getTelnet_rpcs_received() {
    return telnet_rpcs_received;
  }

  public Counter getHttp_rpcs_received() {
    return http_rpcs_received;
  }

  public Counter getExceptions_caught() {
    return exceptions_caught;
  }

  static class PutDataPointRpcStats {
    private final Counter requests;
    private final Counter hbase_errors;
    private final Counter invalid_values;
    private final Counter illegal_arguments;
    private final Counter unknown_metrics;

    private PutDataPointRpcStats(MetricRegistry registry) {
      requests = registry.counter(name("rpc.received", tag("type", "put")));
      hbase_errors = registry.counter(name("rpc.errors", tag("type", "hbase_errors")));
      invalid_values = registry.counter(name("rpc.errors", tag("type", "invalid_values")));
      illegal_arguments = registry.counter(name("rpc.errors", tag("type", "illegal_arguments")));
      unknown_metrics = registry.counter(name("rpc.errors", tag("type", "unknown_metrics")));
    }

    public Counter getRequests() {
      return requests;
    }

    public Counter getHbase_errors() {
      return hbase_errors;
    }

    public Counter getInvalid_values() {
      return invalid_values;
    }

    public Counter getIllegal_arguments() {
      return illegal_arguments;
    }

    public Counter getUnknown_metrics() {
      return unknown_metrics;
    }
  }

  static class GraphHandlerStats {
    /** Number of times we had to do all the work up to running Gnuplot. */
    private final Counter graphs_generated;
    /** Number of times a graph request was served from disk, no work needed. */
    private final Counter graphs_diskcache_hit;
    /** Keep track of the latency of graphing requests. */
    private final Timer graphlatency;
    /** Keep track of the latency (in ms) introduced by running Gnuplot. */
    private final Timer gnuplotlatency;

    private GraphHandlerStats(MetricRegistry registry) {
      graphs_generated = registry.counter(name("http.graph.requests", tag("cache","miss")));
      graphs_diskcache_hit = registry.counter(name("http.graph.requests", tag("cache", "disk")));
      graphlatency = registry.timer(name("http.latency", tag("type", "graph")));
      gnuplotlatency = registry.timer(name("http.latency", tag("type", "gnuplot")));
    }

    public Counter getGraphs_generated() {
      return graphs_generated;
    }

    public Counter getGraphs_diskcache_hit() {
      return graphs_diskcache_hit;
    }

    public Timer getGraphlatency() {
      return graphlatency;
    }

    public Timer getGnuplotlatency() {
      return gnuplotlatency;
    }
  }

  static class ConnectionManagerStats {
    private final Counter connections_established;
    private final Counter exceptions_unknown;
    private final Counter exceptions_closed;
    private final Counter exceptions_reset;
    private final Counter exceptions_timeout;

    private ConnectionManagerStats(MetricRegistry registry) {
      connections_established = registry.counter(name("connectionmgr.connections", tag("type", "total")));
      exceptions_closed = registry.counter(name("connectionmgr.exceptions", tag("type", "closed")));
      exceptions_reset = registry.counter(name("connectionmgr.exceptions", tag("type", "reset")));
      exceptions_timeout = registry.counter(name("connectionmgr.exceptions", tag("type", "timeout")));
      exceptions_unknown = registry.counter(name("connectionmgr.exceptions", tag("type", "unknown")));

      registry.register(name("connectionmgr.connections", tag("type", "open")),
              new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                  return ConnectionManager.channels.size();
                }
              });
    }

    public Counter getConnections_established() {
      return connections_established;
    }

    public Counter getExceptions_unknown() {
      return exceptions_unknown;
    }

    public Counter getExceptions_closed() {
      return exceptions_closed;
    }

    public Counter getExceptions_reset() {
      return exceptions_reset;
    }

    public Counter getExceptions_timeout() {
      return exceptions_timeout;
    }
  }

  public PutDataPointRpcStats getPutDataPointRpcStats() {
    return putDataPointRpcStats;
  }

  public GraphHandlerStats getGraphHandlerStats() {
    return graphHandlerStats;
  }

  public ConnectionManagerStats getConnectionManagerStats() {
    return connectionManagerStats;
  }
}
