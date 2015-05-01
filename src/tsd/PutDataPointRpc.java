// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.tsd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.NoSuchUniqueName;

/** Implements the "put" telnet-style command. */
final class PutDataPointRpc implements TelnetRpc, HttpRpc {
  private static final Logger LOG =
      LoggerFactory.getLogger(PutDataPointRpc.class);
  private static final AtomicLong requests = new AtomicLong();
  private static final AtomicLong hbase_errors = new AtomicLong();
  private static final AtomicLong invalid_values = new AtomicLong();
  private static final AtomicLong illegal_arguments = new AtomicLong();
  private static final AtomicLong unknown_metrics = new AtomicLong();
  private static final AtomicLong batch_timeouts = new AtomicLong();

  /** timers to watch the requests */
  private final static ScheduledExecutorService scheduler =
      Executors.newScheduledThreadPool(2);

  /** Implements a callback sink to count completion of storage calls */
  final class RequestCompletion implements Callback<Object, Object> {
    private final long total;
    private long success = 0;
    private long errors = 0;
    private final boolean show_summary;
    private final ArrayList<HashMap<String, Object>> details;
    private final HttpQuery query;
    private final AtomicBoolean request_complete;
    private ScheduledFuture<?> taskhandle;

    RequestCompletion(final int timeout_seconds,
                      final long total,
                      final boolean show_summary,
                      final ArrayList<HashMap<String, Object>> details,
                      final AtomicBoolean request_complete,
                      final HttpQuery query) {

      this.total = total;
      this.query = query;
      this.show_summary = show_summary;
      this.details = details;
      this.request_complete = request_complete;

      if (timeout_seconds > 0) {
        Runnable task  = new Runnable() {
            public void run() {
              timeout();
            }
        };
        taskhandle = scheduler.schedule(task, timeout_seconds, TimeUnit.SECONDS);
      } else  {
        // 0 or less - timeout directly
        timeout();
      }
    }

    public synchronized Object call(Object result) throws Exception {
      if (result != null &&  (result instanceof Exception)) {
        errors++;
      } else {
        success++;
      }
      if (success + errors >= total) {
        // all items called back, complete the request
        if (taskhandle != null) {
          taskhandle.cancel(false);
        }
        final HttpResponseStatus httpcode = (errors > 0) ?
            HttpResponseStatus.INTERNAL_SERVER_ERROR : HttpResponseStatus.OK;

        execute_complete(query, success, errors, total, show_summary, details,
            request_complete, httpcode);
      }
      return result;
    }

    private synchronized void timeout() {
      try {
        // not all items called back in time - complete it now as timeout.
        batch_timeouts.incrementAndGet();
        execute_complete(query, success, errors, total, show_summary, details,
            request_complete, HttpResponseStatus.REQUEST_TIMEOUT);
      }
      catch (Exception e) {
        LOG.error("Unexpected exception in timeout", e);
      }
    }
  }

  /** parse query args for waitflush argument */
  private int parseWaitflush(final HttpQuery query) throws IOException {
    String value = query.getQueryStringParam("waitflush");
    if (value != null && !value.isEmpty()) {
      try {
        final int waitflush = Integer.parseInt(value);
        if (waitflush < 0 || waitflush > 3600) {
          throw new BadRequestException("waitflush out of range");
        }
        return waitflush;
      } catch (NumberFormatException unused) {
        throw new BadRequestException("Unable to parse 'waitflush' as a number");
      }
    }
    return -1;
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
    if (errmsg != null) {
      LOG.debug(errmsg);
      if (chan.isConnected()) {
        chan.write(errmsg);
      }
    }
    return Deferred.fromResult(null);
  }

  /**
   * Handles HTTP RPC put requests
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query from the user
   * @throws IOException if there is an error parsing the query or formatting 
   * the output
   * @throws BadRequestException if the user supplied bad data
   * @since 2.0
   */
  public void execute(final TSDB tsdb, final HttpQuery query) 
    throws IOException {
    requests.incrementAndGet();
    
    // only accept POST
    if (query.method() != HttpMethod.POST) {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
          "Method not allowed", "The HTTP method [" + query.method().getName() +
          "] is not permitted for this endpoint");
    }

    long success = 0;
    final List<IncomingDataPoint> dps = query.serializer().parsePutV1();
    final long total = dps.size();
    if (total < 1) {
      throw new BadRequestException("No datapoints found in content");
    }

    final AtomicBoolean request_complete = new AtomicBoolean();
    final int waitflush = parseWaitflush(query);
    final boolean show_details = query.hasQueryStringParam("details");
    final boolean show_summary = query.hasQueryStringParam("summary");
    final ArrayList<HashMap<String, Object>> details =
        show_details ? new ArrayList<HashMap<String, Object>>() : null;

    Callback<Object, Object> request_callback = (waitflush < 0) ?
        Callback.PASSTHROUGH:
        new RequestCompletion(waitflush, total,  show_summary, details,
                              request_complete, query);

    for (IncomingDataPoint dp : dps) {
      try {
        if (dp.getMetric() == null || dp.getMetric().isEmpty()) {
          if (show_details) {
            details.add(this.getHttpDetails("Metric name was empty", dp));
          }
          LOG.warn("Metric name was empty: " + dp);
          continue;
        }
        if (dp.getTimestamp() <= 0) {
          if (show_details) {
            details.add(this.getHttpDetails("Invalid timestamp", dp));
          }
          LOG.warn("Invalid timestamp: " + dp);
          continue;
        }
        if (dp.getValue() == null || dp.getValue().isEmpty()) {
          if (show_details) {
            details.add(this.getHttpDetails("Empty value", dp));
          }
          LOG.warn("Empty value: " + dp);
          continue;
        }
        if (dp.getTags() == null || dp.getTags().size() < 1) {
          if (show_details) {
            details.add(this.getHttpDetails("Missing tags", dp));
          }
          LOG.warn("Missing tags: " + dp);
          continue;
        }

        // This will lookup metric and tags synchronous if they are not in
        // cache. If ID's are not found and we can't auto create them we'll get
        // a NoSuchUniqueName exception. The code in UniqueId will swallow
        // HBase exceptions - in case of HBase errors we should get nothing
        // here. If we create the missing ID, UniqueId will retry and eventually
        // throws a HBase exception which will bubble up to netty.
        if (Tags.looksLikeInteger(dp.getValue())) {
          tsdb.addPoint(dp.getMetric(), dp.getTimestamp(),
              Tags.parseLong(dp.getValue()), dp.getTags())
                  .addBoth(request_callback);
        } else {
          tsdb.addPoint(dp.getMetric(), dp.getTimestamp(),
              Float.parseFloat(dp.getValue()), dp.getTags())
                  .addBoth(request_callback);
        }
        success++;
      } catch (NumberFormatException x) {
        if (show_details) {
          details.add(this.getHttpDetails("Unable to parse value to a number",
                                          dp));
        }
        LOG.warn("Unable to parse value to a number: " + dp);
        invalid_values.incrementAndGet();
      } catch (IllegalArgumentException iae) {
        if (show_details) {
          details.add(this.getHttpDetails(iae.getMessage(), dp));
        }
        LOG.warn(iae.getMessage() + ": " + dp);
        illegal_arguments.incrementAndGet();
      } catch (NoSuchUniqueName nsu) {
        if (show_details) {
          details.add(this.getHttpDetails("Unknown metric", dp));
        }
        LOG.warn("Unknown metric: " + dp);
        unknown_metrics.incrementAndGet();
      }
    }

    final long failures = total - success;
    if (waitflush < 0 || failures > 0) {
      // failures at this point are bad arguments
      final HttpResponseStatus httpcode = (failures > 0) ?
              HttpResponseStatus.BAD_REQUEST : HttpResponseStatus.OK;

      execute_complete(query, success, failures, total, show_summary, details,
          request_complete,  httpcode);
    }
  }

  /**
   * Completes the HTTP RPC put requests b sending the response
   * @param query The HTTP query from the user
   * @throws IOException if there is an error parsing the query or formatting
   * the output
   * @throws BadRequestException if the user supplied bad data
   * @param success Successful written to hbase
   * @param failures items that encountered some error
   * @param total all items in the orignal request (might be > success+failures)
   * @param show_summary if true, send a short summary of errors/success the
   * batch
   * @param details includes a list of detailed errors to be sent in the
   * response
   * @since 2.0
   */
  void execute_complete(final HttpQuery query,
                        final long success,
                        final long failures,
                        final long total,
                        final boolean show_summary,
                        final ArrayList<HashMap<String, Object>> details,
                        final AtomicBoolean request_complete,
                        final HttpResponseStatus httpcode)
                        throws IOException {

    if (request_complete.getAndSet(true)) {
      // this request has already been completed, do nothing.
      // Because this CAS is atomic, we can make it past this barrier only once
      // which prevents multiple writes to the HttpQuery
      return;
    }

    if (!show_summary && details == null) {
      if (httpcode == HttpResponseStatus.OK) {
          query.sendReply(HttpResponseStatus.NO_CONTENT, "".getBytes());
      } else if (httpcode == HttpResponseStatus.REQUEST_TIMEOUT ||
          httpcode == HttpResponseStatus.INTERNAL_SERVER_ERROR ||
          httpcode == HttpResponseStatus.SERVICE_UNAVAILABLE) {
          query.sendStatusOnly(httpcode);
      }
      else {
          // for all other errors attach exception
          query.badRequest(
              "One or more data points had errors. " +
              "Please see the TSD logs or append \"details\" to the put request");
      }
    } else {
      final HashMap<String, Object> summary = new HashMap<String, Object>();
      summary.put("total", total);
      summary.put("success", success);
      summary.put("failed", failures);
      if (details != null) {
        summary.put("errors", details);
      }
      query.sendReply(httpcode, query.serializer().formatPutV1(summary));
    }
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
    collector.record("rpc.errors", batch_timeouts, "type=batch_timeouts");
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
    final long timestamp;
    if (words[2].contains(".")) {
      timestamp = Tags.parseLong(words[2].replace(".", "")); 
    } else {
      timestamp = Tags.parseLong(words[2]);
    }
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
    if (Tags.looksLikeInteger(value)) {
      return tsdb.addPoint(metric, timestamp, Tags.parseLong(value), tags);
    } else {  // floating point value
      return tsdb.addPoint(metric, timestamp, Float.parseFloat(value), tags);
    }
  }

  /**
   * Simple helper to format an error trying to save a data point
   * @param message The message to return to the user
   * @param dp The datapoint that caused the error
   * @return A hashmap with information
   * @since 2.0
   */
  final private HashMap<String, Object> getHttpDetails(final String message, 
      final IncomingDataPoint dp) {
    final HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("error", message);
    map.put("datapoint", dp);
    return map;
  }
}
