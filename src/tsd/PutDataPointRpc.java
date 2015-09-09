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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.NoSuchUniqueName;

/** Implements the "put" telnet-style command. */
final class PutDataPointRpc implements TelnetRpc, HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(PutDataPointRpc.class);
  private static final ArrayList<Boolean> EMPTY_DEFERREDS = 
      new ArrayList<Boolean>(0);
  private static final AtomicLong requests = new AtomicLong();
  private static final AtomicLong hbase_errors = new AtomicLong();
  private static final AtomicLong invalid_values = new AtomicLong();
  private static final AtomicLong illegal_arguments = new AtomicLong();
  private static final AtomicLong unknown_metrics = new AtomicLong();
  private static final AtomicLong writes_blocked = new AtomicLong();
  private static final AtomicLong writes_timedout = new AtomicLong();
  
  public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                  final String[] cmd) {
    requests.incrementAndGet();
    String errmsg = null;
    try {
      final class PutErrback implements Callback<Exception, Exception> {
        public Exception call(final Exception arg) {
          // we handle the storage exceptions here so as to avoid creating yet
          // another callback object on every data point.
          handleStorageException(tsdb, getDataPointFromString(cmd), arg);
          if (chan.isConnected()) {
            if (chan.isWritable()) {
              chan.write("put: HBase error: " + arg.getMessage() + '\n');
            } else {
              writes_blocked.incrementAndGet();
            }
          }
          hbase_errors.incrementAndGet();
          return null;
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
        if (chan.isWritable()) {
          chan.write(errmsg);
        } else {
          writes_blocked.incrementAndGet();
        }
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
    
    final List<IncomingDataPoint> dps = query.serializer().parsePutV1();
    if (dps.size() < 1) {
      throw new BadRequestException("No datapoints found in content");
    }
    
    final boolean show_details = query.hasQueryStringParam("details");
    final boolean show_summary = query.hasQueryStringParam("summary");
    final boolean synchronous = query.hasQueryStringParam("sync");
    final int sync_timeout = query.hasQueryStringParam("sync_timeout") ? 
        Integer.parseInt(query.getQueryStringParam("sync_timeout")) : 0;
    // this is used to coordinate timeouts
    final AtomicBoolean sending_response = new AtomicBoolean();
    sending_response.set(false);
        
    final ArrayList<HashMap<String, Object>> details = show_details
      ? new ArrayList<HashMap<String, Object>>() : null;
    int queued = 0;
    final List<Deferred<Boolean>> deferreds = synchronous ? 
        new ArrayList<Deferred<Boolean>>(dps.size()) : null;
    for (final IncomingDataPoint dp : dps) {

      /** Handles passing a data point to the storage exception handler if 
       * we were unable to store it for any reason */
      final class PutErrback implements Callback<Boolean, Exception> {
        public Boolean call(final Exception arg) {
          handleStorageException(tsdb, dp, arg);
          hbase_errors.incrementAndGet();
          
          if (show_details) {
            details.add(getHttpDetails("Storage exception: " 
                + arg.getMessage(), dp));
          }
          return false;
        }
        public String toString() {
          return "HTTP Put Exception CB";
        }
      }
      
      /** Simply marks the put as successful */
      final class SuccessCB implements Callback<Boolean, Object> {
        @Override
        public Boolean call(final Object obj) {
          return true;
        }
        public String toString() {
          return "HTTP Put success CB";
        }
      }
      
      try {
        if (dp.getMetric() == null || dp.getMetric().isEmpty()) {
          if (show_details) {
            details.add(this.getHttpDetails("Metric name was empty", dp));
          }
          LOG.warn("Metric name was empty: " + dp);
          illegal_arguments.incrementAndGet();
          continue;
        }
        if (dp.getTimestamp() <= 0) {
          if (show_details) {
            details.add(this.getHttpDetails("Invalid timestamp", dp));
          }
          LOG.warn("Invalid timestamp: " + dp);
          illegal_arguments.incrementAndGet();
          continue;
        }
        if (dp.getValue() == null || dp.getValue().isEmpty()) {
          if (show_details) {
            details.add(this.getHttpDetails("Empty value", dp));
          }
          LOG.warn("Empty value: " + dp);
          invalid_values.incrementAndGet();
          continue;
        }
        if (dp.getTags() == null || dp.getTags().size() < 1) {
          if (show_details) {
            details.add(this.getHttpDetails("Missing tags", dp));
          }
          LOG.warn("Missing tags: " + dp);
          illegal_arguments.incrementAndGet();
          continue;
        }
        final Deferred<Object> deferred;
        if (Tags.looksLikeInteger(dp.getValue())) {
          deferred = tsdb.addPoint(dp.getMetric(), dp.getTimestamp(), 
              Tags.parseLong(dp.getValue()), dp.getTags());
        } else {
          deferred = tsdb.addPoint(dp.getMetric(), dp.getTimestamp(), 
              Float.parseFloat(dp.getValue()), dp.getTags());
        }
        if (synchronous) {
          deferreds.add(deferred.addCallback(new SuccessCB()));
        }
        deferred.addErrback(new PutErrback());
        ++queued;
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
    
    /** A timer task that will respond to the user with the number of timeouts
     * for synchronous writes. */
    class PutTimeout implements TimerTask {
      final int queued;
      public PutTimeout(final int queued) {
        this.queued = queued;
      }
      @Override
      public void run(final Timeout timeout) throws Exception {
        if (sending_response.get()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Put data point call " + query + 
                " already responded successfully");
          }
          return;
        } else {
          sending_response.set(true);
        }
        
        // figure out how many writes are outstanding
        int good_writes = 0;
        int failed_writes = 0;
        int timeouts = 0;
        for (int i = 0; i < deferreds.size(); i++) {
          try {
            if (deferreds.get(i).join(1)) {
              ++good_writes;
            } else {
              ++failed_writes;
            }
          } catch (TimeoutException te) {
            if (show_details) {
              details.add(getHttpDetails("Write timedout", dps.get(i)));
            }
            ++timeouts;
          }
        }
        writes_timedout.addAndGet(timeouts);
        final int failures = dps.size() - queued;
        if (!show_summary && !show_details) {
          throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
              "The put call has timedout with " + good_writes 
                + " successful writes, " + failed_writes + " failed writes and "
                + timeouts + " timed out writes.", 
              "Please see the TSD logs or append \"details\" to the put request");
        } else {
          final HashMap<String, Object> summary = new HashMap<String, Object>();
          summary.put("success", good_writes);
          summary.put("failed", failures + failed_writes);
          summary.put("timeouts", timeouts);
          if (show_details) {
            summary.put("errors", details);
          }
          
          query.sendReply(HttpResponseStatus.BAD_REQUEST, 
              query.serializer().formatPutV1(summary));
        }
      }
    }
    
    // now after everything has been sent we can schedule a timeout if so
    // the caller asked for a synchronous write.
    final Timeout timeout = sync_timeout > 0 ? 
        tsdb.getTimer().newTimeout(new PutTimeout(queued), sync_timeout, 
            TimeUnit.MILLISECONDS) : null;
    
    /** Serializes the response to the client */
    class GroupCB implements Callback<Object, ArrayList<Boolean>> {
      final int queued;
      public GroupCB(final int queued) {
        this.queued = queued;
      }
      
      @Override
      public Object call(final ArrayList<Boolean> results) {
        if (sending_response.get()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Put data point call " + query + " was marked as timedout");
          }
          return null;
        } else {
          sending_response.set(true);
          if (timeout != null) {
            timeout.cancel();
          }
        }
        int good_writes = 0;
        int failed_writes = 0;
        for (final boolean result : results) {
          if (result) {
            ++good_writes;
          } else {
            ++failed_writes;
          }
        }
        
        final int failures = dps.size() - queued;
        if (!show_summary && !show_details) {
          if (failures + failed_writes > 0) {
            query.sendReply(HttpResponseStatus.BAD_REQUEST, 
                query.serializer().formatErrorV1(
                    new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                "One or more data points had errors", 
                "Please see the TSD logs or append \"details\" to the put request")));
          } else {
            query.sendReply(HttpResponseStatus.NO_CONTENT, "".getBytes());
          }
        } else {
          final HashMap<String, Object> summary = new HashMap<String, Object>();
          if (sync_timeout > 0) {
            summary.put("timeouts", 0);
          }
          summary.put("success", results.isEmpty() ? queued : good_writes);
          summary.put("failed", failures + failed_writes);
          if (show_details) {
            summary.put("errors", details);
          }
          
          if (failures > 0) {
            query.sendReply(HttpResponseStatus.BAD_REQUEST, 
                query.serializer().formatPutV1(summary));
          } else {
            query.sendReply(query.serializer().formatPutV1(summary));
          }
        }
        
        return null;
      }
      @Override
      public String toString() {
        return "put data point serialization callback";
      }
    }
    
    /** Catches any unexpected exceptions thrown in the callback chain */
    class ErrCB implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception e) throws Exception {
        if (sending_response.get()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Put data point call " + query + " was marked as timedout");
          }
          return null;
        } else {
          sending_response.set(true);
          if (timeout != null) {
            timeout.cancel();
          }
        }
        LOG.error("Unexpected exception", e);
        throw new RuntimeException("Unexpected exception", e);
      }
      @Override
      public String toString() {
        return "put data point error callback";
      }
    }
    
    if (synchronous) {
      Deferred.groupInOrder(deferreds).addCallback(new GroupCB(queued))
        .addErrback(new ErrCB());
    } else {
      new GroupCB(queued).call(EMPTY_DEFERREDS);
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
    collector.record("rpc.errors", writes_blocked, "type=socket_writes_blocked");
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
   * Converts the string array to an IncomingDataPoint. WARNING: This method
   * does not perform validation. It should only be used by the Telnet style
   * {@code execute} above within the error callback. At that point it means
   * the array parsed correctly as per {@code importDataPoint}.
   * @param words The array of strings representing a data point
   * @return An incoming data point object.
   */
  final private IncomingDataPoint getDataPointFromString(final String[] words) {
    final IncomingDataPoint dp = new IncomingDataPoint();
    dp.setMetric(words[1]);
    
    if (words[2].contains(".")) {
      dp.setTimestamp(Tags.parseLong(words[2].replace(".", ""))); 
    } else {
      dp.setTimestamp(Tags.parseLong(words[2]));
    }
    
    dp.setValue(words[3]);
    
    final HashMap<String, String> tags = new HashMap<String, String>();
    for (int i = 4; i < words.length; i++) {
      if (!words[i].isEmpty()) {
        Tags.parse(tags, words[i]);
      }
    }
    dp.setTags(tags);
    return dp;
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
  
  /**
   * Passes a data point off to the storage handler plugin if it has been
   * configured. 
   * @param tsdb The TSDB from which to grab the SEH plugin
   * @param dp The data point to process
   * @param e The exception that caused this
   */
  void handleStorageException(final TSDB tsdb, final IncomingDataPoint dp, 
      final Exception e) {
    final StorageExceptionHandler handler = tsdb.getStorageExceptionHandler();
    if (handler != null) {
      handler.handleError(dp, e);
    }
  }
}
