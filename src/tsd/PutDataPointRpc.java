// This file is part of OpenTSDB.
// Copyright (C) 2010-2015  The OpenTSDB Authors.
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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

import org.hbase.async.HBaseException;
import org.hbase.async.PleaseThrottleException;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Histogram;
import net.opentsdb.core.HistogramDataPoint;
import net.opentsdb.core.HistogramPojo;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.rollup.NoSuchRollupForIntervalException;
import net.opentsdb.rollup.RollUpDataPoint;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.Config;

/**
 * This class handles the parsing and writing of data points over any of the
 * implemented RPCs. Each put method should track the deferred of the 
 * {@code tsdb.addPoint()} method so that we know whether or not the write was
 * actually successful. Note that if HBase is backed up, then the arguments
 * will hang around until the deferred is called back. This could take many
 * seconds, during which time the heap will keep growing. 
 * <p>
 * Each execute method also checks the tsd's {@code StorageExceptionHandler} 
 * object on failure so that we can do something else with the data point such
 * as spool it to disk or send it back to an external queue. We do that in the
 * error callback here instead of in the TSDB {@code addPoint()} because we 
 * want to avoid adding yet another callback and chewing up more heap 
 * unnecessarily.  
 * <p>
 * Note that this class can be subclassed to handle different types of 
 * data points such as Rollups or Pre-Aggregates
 */
class PutDataPointRpc implements TelnetRpc, HttpRpc {
  protected static final Logger LOG = LoggerFactory.getLogger(PutDataPointRpc.class);
  protected static final ArrayList<Boolean> EMPTY_DEFERREDS = 
      new ArrayList<Boolean>(0);
  protected static final AtomicLong telnet_requests = new AtomicLong();
  protected static final AtomicLong http_requests = new AtomicLong();
  protected static final AtomicLong raw_dps = new AtomicLong();
  protected static final AtomicLong raw_histograms = new AtomicLong();
  protected static final AtomicLong rollup_dps = new AtomicLong();
  protected static final AtomicLong raw_stored = new AtomicLong();
  protected static final AtomicLong raw_histograms_stored = new AtomicLong();
  protected static final AtomicLong rollup_stored = new AtomicLong();
  protected static final AtomicLong hbase_errors = new AtomicLong();
  protected static final AtomicLong unknown_errors = new AtomicLong();
  protected static final AtomicLong invalid_values = new AtomicLong();
  protected static final AtomicLong illegal_arguments = new AtomicLong();
  protected static final AtomicLong unknown_metrics = new AtomicLong();
  protected static final AtomicLong inflight_exceeded = new AtomicLong();
  protected static final AtomicLong writes_blocked = new AtomicLong();
  protected static final AtomicLong writes_timedout = new AtomicLong();
  protected static final AtomicLong requests_timedout = new AtomicLong();
  
  /** Whether or not to send error messages back over telnet */
  private final boolean send_telnet_errors;
  
  /** The type of data point we're writing.
   * @since 2.4 */
  public enum DataPointType {
    PUT("put"),
    ROLLUP("rollup"),
    HISTOGRAM("histogram");
    
    private final String name;
    DataPointType(final String name) {
      this.name = name;
    }
    
    @Override
    public String toString() {
      return name;
    }
  }
  
  /**
   * Default Ctor
   * @param config The TSDB config to pull from
   */
  public PutDataPointRpc(final Config config) {
    send_telnet_errors = config.getBoolean("tsd.rpc.telnet.return_errors");
  }
  
  @Override
  public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                  final String[] cmd) {
    telnet_requests.incrementAndGet();
    final DataPointType type;
    final String command = cmd[0].toLowerCase();
    if (command.equals("put")) {
      type = DataPointType.PUT;
      raw_dps.incrementAndGet();
    } else if (command.equals("rollup")) {
      type = DataPointType.ROLLUP;
      rollup_dps.incrementAndGet();
    } else if (command.equals("histogram")) {
      type = DataPointType.HISTOGRAM;
      raw_histograms.incrementAndGet();
    } else {
      throw new IllegalArgumentException("Unrecognized command: " + cmd[0]);
    }

    String errmsg = null;
    try {
      
      /**
       * Error callback that handles passing a data point to the storage 
       * exception handler as well as responding to the client when HBase
       * is unable to write the data.
       */
      final class PutErrback implements Callback<Object, Exception> {
        @Override
        public Object call(final Exception arg) {
          String errmsg = null;
          if (arg instanceof PleaseThrottleException) {
            if (send_telnet_errors) {
              errmsg = type + ": Please throttle writes: " + arg.getMessage() + '\n';
            }
            inflight_exceeded.incrementAndGet();
          } else {
            if (send_telnet_errors) {
              errmsg = type + ": HBase error: " + arg.getMessage()+ '\n';
            }
            if (arg instanceof HBaseException) {
              hbase_errors.incrementAndGet();
            } else if (arg instanceof IllegalArgumentException) {
              illegal_arguments.incrementAndGet();
            } else {
              unknown_errors.incrementAndGet();
            }
          }
          
          // we handle the storage exceptions here so as to avoid creating yet
          // another callback object on every data point.
          handleStorageException(tsdb, getDataPointFromString(tsdb, cmd), arg);
          
          if (send_telnet_errors) {
            if (chan.isConnected()) {
              if (chan.isWritable()) {
                chan.write(errmsg);
              } else {
                writes_blocked.incrementAndGet();
              }
            }
          }
          
          return null;
        }
        public String toString() {
          return "report error to channel";
        }
      }
      
      /**
       * Simply called to increment the success counter and log hearbeats
       */
      final class SuccessCB implements Callback<Object, Object> {
        @Override
        public Object call(final Object obj) {
          if (type == DataPointType.PUT) {
            raw_stored.incrementAndGet();
          } else if (type == DataPointType.ROLLUP) {
            rollup_stored.incrementAndGet();
          } else if (type == DataPointType.HISTOGRAM) {
            raw_histograms_stored.incrementAndGet();
          }
          return true;
        }
      }
      
      // Rollups and histos override this method in their implementation so 
      // that it will route properly.
      return importDataPoint(tsdb, cmd)
          .addCallback(new SuccessCB())
          .addErrback(new PutErrback());
    } catch (NumberFormatException x) {
      errmsg = type + ": invalid value: " + x.getMessage() + '\n';
      invalid_values.incrementAndGet();
    } catch (NoSuchRollupForIntervalException x) {
      errmsg = type + ": No such rollup: " + x.getMessage() + '\n';
      illegal_arguments.incrementAndGet();
    } catch (IllegalArgumentException x) {
      errmsg = type + ": illegal argument: " + x.getMessage() + '\n';
      illegal_arguments.incrementAndGet();
    } catch (NoSuchUniqueName x) {
      errmsg = type + ": unknown metric: " + x.getMessage() + '\n';
      unknown_metrics.incrementAndGet();
    /*} catch (NoSuchUniqueNameInCache x) {
      errmsg = "put: waiting for cache: " + x.getMessage() + '\n';
      handleStorageException(tsdb, getDataPointFromString(cmd), x); */
    } catch (PleaseThrottleException x) {
      errmsg = type + ": Throttling exception: " + x.getMessage() + '\n';
      inflight_exceeded.incrementAndGet();
      handleStorageException(tsdb, getDataPointFromString(tsdb, cmd), x);
    } catch (TimeoutException tex) {
      errmsg = type + ": Request timed out: " + tex.getMessage() + '\n';
      handleStorageException(tsdb, getDataPointFromString(tsdb, cmd), tex);
    }
    catch (RuntimeException rex) {
      errmsg = type + ": Unexpected runtime exception: " + rex.getMessage() + '\n';
      throw rex;
    }
    
    if (errmsg != null && chan.isConnected()) {
      if (chan.isWritable()) {
        chan.write(errmsg);
      } else {
        writes_blocked.incrementAndGet();
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
  @Override
  public void execute(final TSDB tsdb, final HttpQuery query) 
    throws IOException {
    http_requests.incrementAndGet();
    
    // only accept POST
    if (query.method() != HttpMethod.POST) {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
          "Method not allowed", "The HTTP method [" + query.method().getName() +
          "] is not permitted for this endpoint");
    }
    final List<IncomingDataPoint> dps;
    try {
      dps = query.serializer()
          .parsePutV1(IncomingDataPoint.class, HttpJsonSerializer.TR_INCOMING);
    } catch (BadRequestException e) {
      illegal_arguments.incrementAndGet();
      throw e;
    }
    processDataPoint(tsdb, query, dps);
  }
  
  /**
   * Handles one or more incoming data point types for the HTTP endpoint
   * to put raw, rolled up or aggregated data points
   * @param <T> An {@link IncomingDataPoint} class.
   * @param tsdb The TSDB to which we belong
   * @param query The query to respond to
   * @param dps The de-serialized data points
   * @throws BadRequestException if the data is invalid in some way
   * @since 2.4
   */
  public <T extends IncomingDataPoint> void processDataPoint(final TSDB tsdb, 
      final HttpQuery query, final List<T> dps) {
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
    
    final List<Map<String, Object>> details = show_details
        ? new ArrayList<Map<String, Object>>() : null;
    int queued = 0;
    final List<Deferred<Boolean>> deferreds = synchronous ? 
        new ArrayList<Deferred<Boolean>>(dps.size()) : null;
    
    for (final IncomingDataPoint dp : dps) {
      final DataPointType type;
      if (dp instanceof RollUpDataPoint) {
        type = DataPointType.ROLLUP;
        rollup_dps.incrementAndGet();
      } else if (dp instanceof HistogramPojo) {
        type = DataPointType.HISTOGRAM;
        raw_histograms.incrementAndGet();
      } else {
        type = DataPointType.PUT;
        raw_dps.incrementAndGet();
      }

      /**
       * Error back callback to handle storage failures
       */
      final class PutErrback implements Callback<Boolean, Exception> {
        public Boolean call(final Exception arg) {
          if (arg instanceof PleaseThrottleException) {
            inflight_exceeded.incrementAndGet();
          } else {
            hbase_errors.incrementAndGet();
          }
          
          if (show_details) {
            details.add(getHttpDetails("Storage exception: " 
                + arg.getMessage(), dp));
          }
          
          // we handle the storage exceptions here so as to avoid creating yet
          // another callback object on every data point.
          handleStorageException(tsdb, dp, arg);
          return false;
        }
        public String toString() {
          return "HTTP Put exception";
        }
      }

      final class SuccessCB implements Callback<Boolean, Object> {
        @Override
        public Boolean call(final Object obj) {
          switch (type) {
          case PUT:
            raw_stored.incrementAndGet();
            break;
          case ROLLUP:
            rollup_stored.incrementAndGet();
            break;
          case HISTOGRAM:
            raw_histograms_stored.incrementAndGet();
            break;
          default:
            // don't care
          }
          return true;
        }
      }
      
      try {
        if (!dp.validate(details)) {
          illegal_arguments.incrementAndGet();
          continue;
        }
        // TODO - refactor the add calls someday or move some of this into the 
        // actual data point class.
        final Deferred<Boolean> deferred;
        if (type == DataPointType.HISTOGRAM) {
          final HistogramPojo pojo = (HistogramPojo) dp;
          // validation before storage of histograms by decoding then re-encoding.
          final Histogram hdp = tsdb.histogramManager().decode(
              pojo.getId(), pojo.getBytes(), false);
          deferred = tsdb.addHistogramPoint(
              pojo.getMetric(), 
              pojo.getTimestamp(), 
              tsdb.histogramManager().encode(pojo.getId(), hdp, true), 
              pojo.getTags())
                .addCallback(new SuccessCB())
                .addErrback(new PutErrback());
        } else {
          if (Tags.looksLikeInteger(dp.getValue())) {
            switch (type) {
            case ROLLUP:
            {
              final RollUpDataPoint rdp = (RollUpDataPoint)dp;
              deferred = tsdb.addAggregatePoint(rdp.getMetric(), 
                  rdp.getTimestamp(), 
                  Tags.parseLong(rdp.getValue()), 
                  dp.getTags(), 
                  rdp.getGroupByAggregator() != null, 
                  rdp.getInterval(), 
                  rdp.getAggregator(),
                  rdp.getGroupByAggregator())
                    .addCallback(new SuccessCB())
                    .addErrback(new PutErrback());
              break;
            }
            default:
              deferred = tsdb.addPoint(dp.getMetric(), dp.getTimestamp(),
                  Tags.parseLong(dp.getValue()), dp.getTags())
                  .addCallback(new SuccessCB())
                  .addErrback(new PutErrback());
            }
          } else {
            switch (type) {
            case ROLLUP:
            {
              final RollUpDataPoint rdp = (RollUpDataPoint)dp;
              deferred = tsdb.addAggregatePoint(rdp.getMetric(), 
                  rdp.getTimestamp(), 
                  (Tags.fitsInFloat(dp.getValue()) ? 
                      Float.parseFloat(dp.getValue()) :
                        Double.parseDouble(dp.getValue())), 
                    dp.getTags(), 
                  rdp.getGroupByAggregator() != null, 
                  rdp.getInterval(), 
                  rdp.getAggregator(),
                  rdp.getGroupByAggregator())
                    .addCallback(new SuccessCB())
                    .addErrback(new PutErrback());
              break;
            }
            default:
              deferred = tsdb.addPoint(dp.getMetric(), dp.getTimestamp(),
                  (Tags.fitsInFloat(dp.getValue()) ? 
                      Float.parseFloat(dp.getValue()) :
                        Double.parseDouble(dp.getValue())), 
                    dp.getTags())
                  .addCallback(new SuccessCB())
                  .addErrback(new PutErrback());
            }
          }
        }
        ++queued;
        if (synchronous) {
          deferreds.add(deferred);
        }
        
      } catch (NumberFormatException x) {
        if (show_details) {
          details.add(getHttpDetails("Unable to parse value to a number",
              dp));
        }
        LOG.warn("Unable to parse value to a number: " + dp);
        invalid_values.incrementAndGet();
      } catch (IllegalArgumentException iae) {
        if (show_details) {
          details.add(getHttpDetails(iae.getMessage(), dp));
        }
        LOG.warn(iae.getMessage() + ": " + dp);
        illegal_arguments.incrementAndGet();
      } catch (NoSuchUniqueName nsu) {
        if (show_details) {
          details.add(getHttpDetails("Unknown metric", dp));
        }
        LOG.warn("Unknown metric: " + dp);
        unknown_metrics.incrementAndGet();
      } catch (PleaseThrottleException x) {
        handleStorageException(tsdb, dp, x);
        if (show_details) {
          details.add(getHttpDetails("Please throttle", dp));
        }
        inflight_exceeded.incrementAndGet();
      } catch (TimeoutException tex) {
        handleStorageException(tsdb, dp, tex);
        if (show_details) {
          details.add(getHttpDetails("Timeout exception", dp));
        }
        requests_timedout.incrementAndGet();
      /*} catch (NoSuchUniqueNameInCache x) {
        handleStorageException(tsdb, dp, x);
        if (show_details) {
          details.add(getHttpDetails("Not cached yet", dp));
        } */
      } catch (RuntimeException e) {
        if (show_details) {
          details.add(getHttpDetails("Unexpected exception", dp));
        }
        LOG.warn("Unexpected exception: " + dp);
        unknown_errors.incrementAndGet();
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
        System.out.println("GOOD: " + good_writes + " Failures: " + failures + " FW " + failed_writes
            + " DPS: " + dps.size() + " Q " + queued);
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
            LOG.debug("ERROR point call " + query + " was marked as timedout", e);
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
      Deferred.groupInOrder(deferreds)
        .addCallback(new GroupCB(queued))
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
    collector.record("rpc.received", http_requests, "type=put");
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
  protected Deferred<Object> importDataPoint(final TSDB tsdb, 
                                             final String[] words) {
    words[0] = null; // Ditch the "put".
    if (words.length < 5) {  // Need at least: metric timestamp value tag
      //               ^ 5 and not 4 because words[0] is "put".
      throw new IllegalArgumentException("not enough arguments"
                                         + " (need least 4, got " 
                                         + (words.length - 1) + ')');
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
    } else if (Tags.fitsInFloat(value)) {  // floating point value
      return tsdb.addPoint(metric, timestamp, Float.parseFloat(value), tags);
    } else {
      return tsdb.addPoint(metric, timestamp, Double.parseDouble(value), tags);
    }
  }
  
  /**
   * Converts the string array to an IncomingDataPoint. WARNING: This method
   * does not perform validation. It should only be used by the Telnet style
   * {@code execute} above within the error callback. At that point it means
   * the array parsed correctly as per {@code importDataPoint}.
   * @param tsdb The TSDB for encoding/decoding.
   * @param words The array of strings representing a data point
   * @return An incoming data point object.
   */
  protected IncomingDataPoint getDataPointFromString(final TSDB tsdb, 
                                                     final String[] words) {
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
