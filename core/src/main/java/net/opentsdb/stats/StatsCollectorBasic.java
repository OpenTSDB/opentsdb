// This file is part of OpenTSDB.
// Copyright (C) 2010-2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.utils.Config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Receives various stats/metrics from the current process.
 * <p>
 * Instances of this class are passed around to other classes to collect
 * their stats/metrics and do something with them (presumably send them
 * to a client).
 * <p>
 * This class does not do any synchronization and is not thread-safe.
 * 
 * @since 1.0
 */
public abstract class StatsCollectorBasic {

  private static final Logger LOG =
    LoggerFactory.getLogger(StatsCollectorBasic.class);

  /** Tags to add to every stat emitted by the collector */
  private static Map<String, String> global_tags;
  
  /** Prefix to add to every metric name, for example `tsd'. */
  protected final String prefix;

  /** Extra tags to add to every data point emitted. */
  protected HashMap<String, String> extratags;

  /** Buffer used to build lines emitted. */
  private final StringBuilder buf = new StringBuilder();
  
  /**
   * Constructor.
   * @param prefix A prefix to add to every metric name, for example
   * `tsd'.
   */
  public StatsCollectorBasic(final String prefix) {
    this.prefix = prefix;
    if (global_tags != null && !global_tags.isEmpty()) {
      for (final Entry<String, String> entry : global_tags.entrySet()) {
        addExtraTag(entry.getKey(), entry.getValue());
      }
    }
  }
  
  /**
   * Method to override to actually emit a data point.
   * @param datapoint A data point in a format suitable for a text
   * import.
   * @throws IllegalStateException if the emitter has not been implemented
   */
  public void emit(String datapoint) {
    throw new IllegalStateException("Emitter has not been implemented");
  }

  /**
   * Records a data point.
   * @param name The name of the metric.
   * @param value The current value for that metric.
   */
  public final void record(final String name, final long value) {
    record(name, value, null);
  }

  /**
   * Records a data point.
   * @param name The name of the metric.
   * @param value The current value for that metric.
   */
  public final void record(final String name, final Number value) {
    record(name, value.longValue(), null);
  }

  /**
   * Records a data point.
   * @param name The name of the metric.
   * @param value The current value for that metric.
   * @param xtratag An extra tag ({@code name=value}) to add to those
   * data points (ignored if {@code null}).
   * @throws IllegalArgumentException if {@code xtratag != null} and it
   * doesn't follow the {@code name=value} format.
   */
  public final void record(final String name,
                           final Number value,
                           final String xtratag) {
    record(name, value.longValue(), xtratag);
  }

  /**
   * Records a number of data points from a {@link Histogram}.
   * @param name The name of the metric.
   * @param histo The histogram to collect data points from.
   * @param xtratag An extra tag ({@code name=value}) to add to those
   * data points (ignored if {@code null}).
   * @throws IllegalArgumentException if {@code xtratag != null} and it
   * doesn't follow the {@code name=value} format.
   */
  public final void record(final String name,
                           final Histogram histo,
                           final String xtratag) {
    record(name + "_50pct", histo.percentile(50), xtratag);
    record(name + "_75pct", histo.percentile(75), xtratag);
    record(name + "_90pct", histo.percentile(90), xtratag);
    record(name + "_95pct", histo.percentile(95), xtratag);
  }

  /**
   * Records a data point.
   * @param name The name of the metric.
   * @param value The current value for that metric.
   * @param xtratag An extra tag ({@code name=value}) to add to this
   * data point (ignored if {@code null}).
   * @throws IllegalArgumentException if {@code xtratag != null} and it
   * doesn't follow the {@code name=value} format.
   */
  public void record(final String name,
                           final long value,
                           final String xtratag) {
    buf.setLength(0);
    buf.append(prefix).append(".")
       .append(name)
       .append(' ')
       .append(System.currentTimeMillis() / 1000)
       .append(' ')
       .append(value);

    if (xtratag != null) {
      if (xtratag.indexOf('=') != xtratag.lastIndexOf('=')) {
        throw new IllegalArgumentException("invalid xtratag: " + xtratag
            + " (multiple '=' signs), name=" + name + ", value=" + value);
      } else if (xtratag.indexOf('=') < 0) {
        throw new IllegalArgumentException("invalid xtratag: " + xtratag
            + " (missing '=' signs), name=" + name + ", value=" + value);
      }
      buf.append(' ').append(xtratag);
    }

    if (extratags != null) {
      for (final Map.Entry<String, String> entry : extratags.entrySet()) {
        buf.append(' ').append(entry.getKey())
           .append('=').append(entry.getValue());
      }
    }
    buf.append('\n');
    emit(buf.toString());
  }

  /**
   * Adds a tag to all the subsequent data points recorded.
   * <p>
   * All subsequent calls to one of the {@code record} methods will
   * associate the tag given to this method with the data point.
   * <p>
   * This method can be called multiple times to associate multiple tags
   * with all the subsequent data points.
   * @param name The name of the tag.
   * @param value The value of the tag.
   * @throws IllegalArgumentException if the name or the value are empty
   * or otherwise invalid.
   * @see #clearExtraTag
   */
  public final void addExtraTag(final String name, final String value) {
    if (name.length() <= 0) {
      throw new IllegalArgumentException("empty tag name, value=" + value);
    } else if (value.length() <= 0) {
      throw new IllegalArgumentException("empty value, tag name=" + name);
    } else if (name.indexOf('=') != -1) {
      throw new IllegalArgumentException("tag name contains `=': " + name
                                         + " (value = " + value + ')');
    } else if (value.indexOf('=') != -1) {
      throw new IllegalArgumentException("tag value contains `=': " + value
                                         + " (name = " + name + ')');
    }
    if (extratags == null) {
      extratags = new HashMap<String, String>();
    }
    extratags.put(name, value);
  }

  /**
   * Adds a {@code host=hostname} tag.
   * <p>
   * This uses {@link InetAddress#getLocalHost} to find the hostname of the
   * current host.  If the hostname cannot be looked up, {@code (unknown)}
   * is used instead.
   */
  public final void addHostTag() {
    addHostTag(false);
  }

  /**
   * Adds a {@code host=hostname} or {@code fqdn=full.host.name} tag.
   * <p>
   * This uses {@link InetAddress#getLocalHost} to find the hostname of the
   * current host.  If the hostname cannot be looked up, {@code (unknown)}
   * is used instead.
   * @param canonical Whether or not we should try to get the FQDN of the host.
   * If set to true, the tag changes to "fqdn" instead of "host"
   */
  public final void addHostTag(final boolean canonical) {
    try {
      if (canonical) {
        addExtraTag("fqdn", InetAddress.getLocalHost().getCanonicalHostName());
      } else {
        addExtraTag("host", InetAddress.getLocalHost().getHostName());
      }
    } catch (UnknownHostException x) {
      LOG.error("WTF?  Can't find hostname for localhost!", x);
      addExtraTag("host", "(unknown)");
    }
  }

  /**
   * Clears a tag added using {@link #addExtraTag addExtraTag}.
   * @param name The name of the tag to remove from the set of extra
   * tags.
   * @throws IllegalStateException if there's no extra tag currently
   * recorded.
   * @throws IllegalArgumentException if the given name isn't in the
   * set of extra tags currently recorded.
   * @see #addExtraTag
   */
  public final void clearExtraTag(final String name) {
    if (extratags == null) {
      throw new IllegalStateException("no extra tags added");
    }
    if (extratags.get(name) == null) {
      throw new IllegalArgumentException("tag '" + name
          + "' not in" + extratags);
    }
    extratags.remove(name);
  }

  /**
   * Parses the configuration to determine if any extra tags should be included
   * with every stat emitted.
   * @param config The config object to parse
   * @throws IllegalArgumentException if the config is null. Other exceptions
   * may be thrown if the config values are unparseable.
   */
  public static final void setGlobalTags(final Config config) {
    if (config == null) {
      throw new IllegalArgumentException("Configuration cannot be null.");
    }
    
    if (config.getBoolean("tsd.core.stats_with_port")) {
      global_tags = new HashMap<String, String>(1);
      global_tags.put("port", config.getString("tsd.network.port"));
    }
  }
}
