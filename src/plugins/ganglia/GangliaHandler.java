// This file is part of OpenTSDB.
// Copyright (C) 2014 The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.plugins.ganglia;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TSDB;
import net.opentsdb.plugins.ganglia.GangliaMessage.ProtocolVersion;
import net.opentsdb.plugins.ganglia.GangliaMessage.Tag;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.Config;


/**
 * Imports Ganglia metrics to OpenTSDB.
 */
class GangliaHandler extends SimpleChannelUpstreamHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(GangliaHandler.class);
  private static final Counters GLOBAL_COUNTERS = new Counters();

  /** The TSDB to use. */
  private final TSDB tsdb;
  /** Prefix to Ganglia metric names. */
  private final String prefix;
  /** cluster tag for Ganglia metrics. */
  private final String cluster_tag;
  /** Keeps statistics. */
  private final Counters counters;

  /**
   * Imports Ganglia v30x and v31x metric values to OpenTSDB.
   * @param config An initialized configuration object
   * @param tsdb The TSDB to import the data point into.
   */
  public GangliaHandler(final GangliaConfig config, final TSDB tsdb) {
    this(config, tsdb, GLOBAL_COUNTERS);
  }

  /**
   * Imports Ganglia v30x and v31x metric values to OpenTSDB.
   * @param config An initialized configuration object
   * @param tsdb The TSDB to import the data point into.
   * @param counters Stores statistics
   */
  private GangliaHandler(final GangliaConfig config, final TSDB tsdb,
      final Counters counters) {
    this.tsdb = tsdb;
    this.counters = counters;
    String prefix_or_null = config.ganglia_name_prefix();
    String cluster_tag_or_null = config.ganglia_tags_cluster();
    this.prefix = prefix_or_null == null ? "" : prefix_or_null;
    this.cluster_tag = cluster_tag_or_null == null ? "" : cluster_tag_or_null;
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    GLOBAL_COUNTERS.collectStats(collector);
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx,
      final MessageEvent event) throws Exception {
    GangliaMessage msg = null;
    try {
      msg = (GangliaMessage)event.getMessage();
      InetSocketAddress sock_addr = (InetSocketAddress)event.getRemoteAddress();
      importGangliaMetric(msg, sock_addr.getAddress());
    } catch (Exception err) {
      if (msg != null) {
        keepStats(err, msg.toString());
      } else {
        keepStats(err, err.getMessage());
      }
    }
  }

  private void keepStats(final Exception exception, final String err_msg) {
    try {
      LOG.debug(err_msg, exception);
      counters.ganglia_errors.incrementAndGet();
      throw exception;
    } catch (NumberFormatException x) {
      LOG.debug("Ganglia Unable to parse value to a number: " + err_msg);
    } catch (IllegalArgumentException iae) {
      LOG.debug(iae.getMessage() + ": " + err_msg);
    } catch (NoSuchUniqueName nsu) {
      LOG.debug("Ganglia Unknown metric: " + err_msg);
    } catch (Exception err) {
      LOG.debug("Ganglia Unknown error: '" + err.getMessage() + "' " + err_msg);
    }
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx,
      final ExceptionEvent e) throws Exception {
      LOG.debug("Exception while processing Ganglia message", e.getCause());
      counters.channel_exceptions.incrementAndGet();
      // We don't close the channel because we can keep serving requests.
  }

  /**
   * Imports a single data point.
   * @param msg Ganglia v30x or v31x message
   * @param inet_addr {@link InetAddress} of the source host/device.
   * @throws NumberFormatException if any value is invalid.
   * @throws IllegalArgumentException if any other argument is invalid.
   * @throws NoSuchUniqueName if the metric isn't registered.
   */
  private void importGangliaMetric(final GangliaMessage msg,
      final InetAddress inet_addr) {
    ProtocolVersion protocol = msg.protocolVersion();
    counters.incrementRequest(protocol);
    if (msg.hasError()) {
      counters.incrementError();
    } else if (msg.isMetadata()) {
      // TODO: Process metadata. For now, we just count it.
      counters.incrementMetadata();
    } else {
      GangliaMessage.Metric metric = msg.getMetricIfNumeric();
      if (metric == null) {
        counters.incrementNonNumericValue(protocol);
      } else {
        importMetric(metric, msg.getTags(inet_addr));
        counters.incrementSuccess(protocol);
      }
    }
  }

  private Deferred<Object> importMetric(final GangliaMessage.Metric gmt,
      Collection<Tag> tags) {
    final String metric = prefixMetricNameIfPresent(gmt.name());
    final HashMap<String, String> all_tags = createTsdbTags(tags);
    if (LOG.isDebugEnabled()) {
      LOG.debug(gmt.toString() + " tags=" + tags);
    }
    final Deferred<Object> deferred;
    if (gmt.isInteger()) {
      deferred = tsdb.addPoint(metric, gmt.timestamp(), gmt.longValue(), all_tags);
    } else {
      // TODO: Support double precision values.
      // We do not support it yet to save storage space.
      deferred = tsdb.addPoint(metric, gmt.timestamp(), gmt.floatValue(), all_tags);
    }
    return deferred;
  }

  private String prefixMetricNameIfPresent(final String name) {
    if (!prefix.isEmpty()) {
      return prefix + name;
    }
    return name;
  }

  private HashMap<String, String> createTsdbTags(final Collection<Tag> tags) {
    final HashMap<String, String> all_tags = Maps.newHashMapWithExpectedSize(
        tags.size() + 1);
    for (Tag tag: tags) {
      all_tags.put(tag.key, tag.value);
    }
    addClusterNameIfPresent(all_tags);
    return all_tags;
  }

  private void addClusterNameIfPresent(final HashMap<String, String> tags) {
    if (!cluster_tag.isEmpty()) {
      tags.put("cluster", cluster_tag);
    }
  }

  /** Counters to keep statistics of Ganglia messages. */
  private static class Counters {

    private final AtomicLong v30x_requests = new AtomicLong();
    private final AtomicLong v30x_successes = new AtomicLong();
    private final AtomicLong v30x_non_numeric_values = new AtomicLong();
    private final AtomicLong v31x_requests = new AtomicLong();
    private final AtomicLong v31x_successes = new AtomicLong();
    private final AtomicLong v31x_metadata = new AtomicLong();
    private final AtomicLong v31x_non_numeric_values = new AtomicLong();
    private final AtomicLong ganglia_errors = new AtomicLong();
    private final AtomicLong channel_exceptions = new AtomicLong();

    void incrementRequest(ProtocolVersion protocol) {
      if (protocol == ProtocolVersion.V31x) {
        v31x_requests.incrementAndGet();
      } else {
        v30x_requests.incrementAndGet();
      }
    }

    void incrementError() {
      ganglia_errors.incrementAndGet();
    }

    void incrementMetadata() {
      v31x_metadata.incrementAndGet();
    }

    void incrementSuccess(ProtocolVersion protocol) {
      if (protocol == ProtocolVersion.V31x) {
        v31x_successes.incrementAndGet();
      } else {
        v30x_successes.incrementAndGet();
      }
    }

    void incrementNonNumericValue(ProtocolVersion protocol) {
      if (protocol == ProtocolVersion.V31x) {
        v31x_non_numeric_values.incrementAndGet();
      } else {
        v30x_non_numeric_values.incrementAndGet();
      }
    }

    void collectStats(final StatsCollector collector) {
      collector.record("ganglia.v30x", v30x_requests, "type=requests");
      collector.record("ganglia.v30x", v30x_successes, "type=successes");
      collector.record("ganglia.v30x", v30x_non_numeric_values,
          "type=non_numeric_values");
      collector.record("ganglia.msg", ganglia_errors, "type=errors");
      collector.record("ganglia.channel", channel_exceptions,
          "type=exceptions");
      collector.record("ganglia.v31x", v31x_requests, "type=requests");
      collector.record("ganglia.v31x", v31x_successes, "type=successes");
      collector.record("ganglia.v31x", v31x_metadata, "type=metadata");
      collector.record("ganglia.v31x", v31x_non_numeric_values,
          "type=non_numeric_values");
    }
  }

  /**
   * Creates a GangliaHandler object with the given {@link Counters}
   * just for unit tests
   */
  @VisibleForTesting
  static GangliaHandler ForTesting(final Config config, final TSDB tsdb,
      final Counters counters) {
    return new GangliaHandler(new GangliaConfig(config), tsdb, counters);
  }

  @VisibleForTesting
  /** Exposes {@link Counters} just for unit tests. */
  static final class CountersForTesting extends Counters {
  }
}
