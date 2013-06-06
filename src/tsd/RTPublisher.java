// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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

import java.util.Map;

import org.hbase.async.Bytes;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

/**
 * Real Time publisher plugin interface that is used to emit data from a TSD
 * as data comes in. Initially it supports publishing data points immediately
 * after they are queued for storage. In the future we may support publishing
 * meta data or other types of information as changes are made.
 * <p>
 * <b>Note:</b> Implementations must have a parameterless constructor. The 
 * {@link #initialize()} method will be called immediately after the plugin is
 * instantiated and before any other methods are called.
 * <p>
 * <b>Warning:</b> All processing should be performed asynchronously and return
 * a Deferred as quickly as possible.
 * @since 2.0
 */
public abstract class RTPublisher {

  /**
   * Called by TSDB to initialize the plugin
   * Implementations are responsible for setting up any IO they need as well
   * as starting any required background threads.
   * <b>Note:</b> Implementations should throw exceptions if they can't start
   * up properly. The TSD will then shutdown so the operator can fix the 
   * problem. Please use IllegalArgumentException for configuration issues.
   * @param tsdb The parent TSDB object
   * @throws IllegalArgumentException if required configuration parameters are 
   * missing
   * @throws Exception if something else goes wrong
   */
  public abstract void initialize(final TSDB tsdb);
  
  /**
   * Called to gracefully shutdown the plugin. Implementations should close 
   * any IO they have open
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> shutdown();
  
  /**
   * Should return the version of this plugin in the format:
   * MAJOR.MINOR.MAINT, e.g. 2.0.1. The MAJOR version should match the major
   * version of OpenTSDB the plugin is meant to work with.
   * @return A version string used to log the loaded version
   */
  public abstract String version();
  
  /**
   * Called by the TSD when a request for statistics collection has come in. The
   * implementation may provide one or more statistics. If no statistics are
   * available for the implementation, simply stub the method.
   * @param collector The collector used for emitting statistics
   */
  public abstract void collectStats(final StatsCollector collector);
  
  /**
   * Called by the TSD when a new, raw data point is published. Because this
   * is called after a data point is queued, the value has been converted to a
   * byte array so we need to convert it back to an integer or floating point 
   * value. Instead of requiring every implementation to perform the calculation
   * we perform it here and let the implementer deal with the integer or float.
   * @param metric The name of the metric associated with the data point
   * @param timestamp Timestamp as a Unix epoch in seconds or milliseconds
   * (depending on the TSD's configuration)
   * @param value The value as a byte array
   * @param tags Tagk/v pairs
   * @param tsuid Time series UID for the value
   * @param flags Indicates if the byte array is an integer or floating point
   * value
   * @return A deferred without special meaning to wait on if necessary. The 
   * value may be null but a Deferred must be returned.
   */
  public final Deferred<Object> sinkDataPoint(final String metric, 
      final long timestamp, final byte[] value, final Map<String, String> tags, 
      final byte[] tsuid, final short flags) {
    
    // One of two possible values from TSDB.addPoint(). Either it's an 8 byte
    // integer or a 4 byte float. Compare on the integer flag to avoid an or
    // calculation
    if (flags == 0x7) {
      return publishDataPoint(metric, timestamp, Bytes.getLong(value), tags, tsuid);
    } else {
      return publishDataPoint(metric, timestamp, 
          Float.intBitsToFloat(Bytes.getInt(value)), tags, tsuid);
    }
  }
  
  /**
   * Called any time a new data point is published
   * @param metric The name of the metric associated with the data point
   * @param timestamp Timestamp as a Unix epoch in seconds or milliseconds
   * (depending on the TSD's configuration)
   * @param value Value for the data point
   * @param tags Tagk/v pairs
   * @param tsuid Time series UID for the value
   * @return A deferred without special meaning to wait on if necessary. The 
   * value may be null but a Deferred must be returned.
   */
  public abstract Deferred<Object> publishDataPoint(final String metric, 
      final long timestamp, final long value, final Map<String, String> tags, 
      final byte[] tsuid);
  
  /**
   * Called any time a new data point is published
   * @param metric The name of the metric associated with the data point
   * @param timestamp Timestamp as a Unix epoch in seconds or milliseconds
   * (depending on the TSD's configuration)
   * @param value Value for the data point
   * @param tags Tagk/v pairs
   * @param tsuid Time series UID for the value
   * @return A deferred without special meaning to wait on if necessary. The 
   * value may be null but a Deferred must be returned.
   */
  public abstract Deferred<Object> publishDataPoint(final String metric, 
      final long timestamp, final double value, final Map<String, String> tags, 
      final byte[] tsuid);
}
