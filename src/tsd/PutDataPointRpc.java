// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpMethod;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.NoSuchUniqueName;

/** Implements the "put" telnet-style command. */
final class PutDataPointRpc implements TelnetRpc, HttpRpc {

  private static final AtomicLong requests = new AtomicLong();
  private static final AtomicLong hbase_errors = new AtomicLong();
  private static final AtomicLong invalid_values = new AtomicLong();
  private static final AtomicLong illegal_arguments = new AtomicLong();
  private static final AtomicLong unknown_metrics = new AtomicLong();

  /**
   * Dirty rows for time series that are being written to.
   *
   * The key in the map is a string that uniquely identifies a time series.
   * Right now we use the time series name concatenated with the stringified
   * version of the map that stores all the tags, e.g. "foo{bar=a,quux=42}".
   */
  private ConcurrentHashMap<String, WritableDataPoints> dirty_rows
    = new ConcurrentHashMap<String, WritableDataPoints>();

  /**
   * Returns the dirty row for the given time series or creates a new one.
   * @param tsdb The TSDB in which the data point should be created.
   * @param metric The metric of the time series.
   * @param tags The tags of the time series.
   * @return the dirty row in which data points for the given time series
   * should be appended.
   */
  private WritableDataPoints getDirtyRow(final TSDB tsdb,
                                         final String metric,
                                         final HashMap<String, String> tags) {
    final String key = metric + tags;
    WritableDataPoints row = dirty_rows.get(key);
    if (row == null) {  // Try to create a new row.
      // TODO(tsuna): Properly evict old rows to save memory.
      if (dirty_rows.size() >= 20000) {
        dirty_rows.clear();  // free some RAM.
      }
      final WritableDataPoints new_row = tsdb.newDataPoints();
      new_row.setSeries(metric, tags);
      row = dirty_rows.putIfAbsent(key, new_row);
      if (row == null) {  // We've just inserted a new row.
        return new_row;   // So use that.
      }
    }
    return row;
  }
  
  public void execute(final TSDB tsdb, final HttpQuery query) {
    if (query.request().getMethod() != HttpMethod.POST) {
        query.badRequest("Must use HTTP method POST");
        return;
    }
    
    // build a String[] named cmd with the data from the contents split on
    // newlines...
    final String[] cmd = query.request().getContent().toString(Charset.forName("ISO-8859-1")).split("\n");

    requests.incrementAndGet();
    String errmsg = null;
    try {
      final class PutErrback implements Callback<Exception, Exception> {
        public Exception call(final Exception arg) {
          query.internalError(arg);
          hbase_errors.incrementAndGet();
          return arg;
        }
        public String toString() {
          return "report error to user";
        }
      }
      importDataPoint(tsdb, cmd).addErrback(new PutErrback());
    } catch (NumberFormatException x) {
      errmsg = "put: invalid value: " + x.getMessage() + '\n';
      invalid_values.incrementAndGet();
    } catch (IllegalArgumentException x) {
      errmsg = "put: illegal argument: " + x.getMessage() + '\n';
      illegal_arguments.incrementAndGet();
    } catch (NoSuchUniqueName x) {
      errmsg = "put: unknown metric: " + x.getMessage() + '\n';
      unknown_metrics.incrementAndGet();
    }
    if (errmsg != null) {
      query.badRequest(errmsg);
    } else {
      query.sendReply("Data points imported.");
    }
  }

  public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                  final String[] cmd) {
    requests.incrementAndGet();
    String errmsg = null;
    try {
      final class PutErrback implements Callback<Exception, Exception> {
        public Exception call(final Exception arg) {
          if (chan.isConnected()) {
            chan.write("put: HBase error: " + arg.getMessage() + '\n');
          }
          hbase_errors.incrementAndGet();
          return arg;
        }
        public String toString() {
          return "report error to channel";
        }
      }
      return importDataPoint(tsdb, cmd).addErrback(new PutErrback());
    } catch (NumberFormatException x) {
      errmsg = "put: invalid value: " + x.getMessage() + '\n';
      invalid_values.incrementAndGet();
    } catch (IllegalArgumentException x) {
      errmsg = "put: illegal argument: " + x.getMessage() + '\n';
      illegal_arguments.incrementAndGet();
    } catch (NoSuchUniqueName x) {
      errmsg = "put: unknown metric: " + x.getMessage() + '\n';
      unknown_metrics.incrementAndGet();
    }
    if (errmsg != null && chan.isConnected()) {
      chan.write(errmsg);
    }
    return Deferred.fromResult(null);
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("rpc.received", requests, "type=put");
    collector.record("rpc.errors", hbase_errors, "type=hbase_errors");
    collector.record("rpc.errors", invalid_values, "type=invalid_values");
    collector.record("rpc.errors", illegal_arguments, "type=illegal_arguments");
    collector.record("rpc.errors", unknown_metrics, "type=unknown_metrics");
  }

  /**
   * Imports a single data point.
   * @param tsdb The TSDB to import the data point into.
   * @param words The words describing the data point to import, in
   * the following format: {@code [metric, timestamp, value, ..tags..]}
   * @return A deferred object that indicates the completion of the request.
   * @throws NumberFormatException if the timestamp or value is invalid.
   * @throws IllegalArgumentException if any other argument is invalid.
   * @throws NoSuchUniqueName if the metric isn't registered.
   */
  private Deferred<Object> importDataPoint(final TSDB tsdb, final String[] words) {
    words[0] = null; // Ditch the "put".
    if (words.length < 5) {  // Need at least: metric timestamp value tag
      //               ^ 5 and not 4 because words[0] is "put".
      throw new IllegalArgumentException("not enough arguments"
                                         + " (need least 4, got " + (words.length - 1) + ')');
    }
    final String metric = words[1];
    if (metric.length() <= 0) {
      throw new IllegalArgumentException("empty metric name");
    }
    final long timestamp = Tags.parseLong(words[2]);
    if (timestamp <= 0) {
      throw new IllegalArgumentException("invalid timestamp: " + timestamp);
    }
    final String value = words[3];
    if (value.length() <= 0) {
      throw new IllegalArgumentException("empty value");
    }
    final HashMap<String, String> tags = new HashMap<String, String>();
    for (int i = 4; i < words.length; i++) {
      if (!words[i].isEmpty()) {
        Tags.parse(tags, words[i]);
      }
    }
    final WritableDataPoints dp = getDirtyRow(tsdb, metric, tags);
    if (value.indexOf('.') < 0) {  // integer value
      return dp.addPoint(timestamp, Tags.parseLong(value));
    } else {  // floating point value
      return dp.addPoint(timestamp, Float.parseFloat(value));
    }
  }
}
