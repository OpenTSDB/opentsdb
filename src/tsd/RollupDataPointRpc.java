// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.rollup.RollUpDataPoint;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.Config;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * A class that handles overriding parsing calls when writing a rolled up data
 * point to storage.
 * @since 2.4
 */
class RollupDataPointRpc extends PutDataPointRpc 
  implements TelnetRpc, HttpRpc {
  
  private enum TelnetIndex {
    COMMAND,
    INTERVAL_AGG,
    METRIC,
    TIMESTAMP,
    VALUE,
    TAGS
  }
  
  /**
   * Default Ctor
   * @param config The TSDB config to pull from
   */
  public RollupDataPointRpc(final Config config) {
    super(config);
  }
  
  /**
   * Handles HTTP RPC put requests
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query from the user
   * @throws IOException if there is an error parsing the query or formatting
   * the output
   * @throws BadRequestException if the user supplied bad data
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

    final List<RollUpDataPoint> dps = query.serializer()
        .parsePutV1(RollUpDataPoint.class, HttpJsonSerializer.TR_ROLLUP);
    processDataPoint(tsdb, query, dps);
  }

  /**
   * Imports a single rolled up data point.
   * @param tsdb The TSDB to import the data point into.
   * @param words The words describing the data point to import, in
   * the following format: 
   * {@code rollup interval:[aggregator] metric timestamp value ..tags..}
   * @return A deferred object that indicates the completion of the request.
   * @throws NumberFormatException if the timestamp, value or count is invalid.
   * @throws IllegalArgumentException if any other argument is invalid.
   * @throws NoSuchUniqueName if the metric isn't registered.
   */
  @Override
  protected Deferred<Object> importDataPoint(final TSDB tsdb, 
      final String[] words) {
    words[TelnetIndex.COMMAND.ordinal()] = null; // Ditch the "rollup" string.
    if (words.length < TelnetIndex.TAGS.ordinal() + 1) {
      throw new IllegalArgumentException("not enough arguments"
              + " (need least 7, got " + (words.length - 1) + ')');
    }
    
    final String interval_agg = words[TelnetIndex.INTERVAL_AGG.ordinal()];
    if (interval_agg.isEmpty()) {
      throw new IllegalArgumentException("Missing interval or aggregator");
    }
    
    String interval = null;
    String temporal_agg = null;
    String spatial_agg = null;
    // if the interval_agg has a - in it, then it's an interval. If there's a :
    // then it is both. If no dash or colon then it's just a spatial agg.
    final String[] interval_parts = interval_agg.split(":");
    final int dash = interval_parts[0].indexOf("-");
    if (dash > -1) {
      interval = interval_parts[0].substring(0,dash);
      temporal_agg = interval_parts[0].substring(dash + 1);
      
    } else if (interval_parts.length == 1) {
      spatial_agg = interval_parts[0];
    } 
    if (interval_parts.length > 1) {
      spatial_agg = interval_parts[1];
    }

    final String metric = words[TelnetIndex.METRIC.ordinal()];
    if (metric.length() <= 0) {
      throw new IllegalArgumentException("empty metric name");
    }

    final long timestamp;
    if (words[TelnetIndex.TIMESTAMP.ordinal()].contains(".")) {
      timestamp = Tags.parseLong(words[TelnetIndex.TIMESTAMP.ordinal()]
          .replace(".", ""));
    } else {
      timestamp = Tags.parseLong(words[TelnetIndex.TIMESTAMP.ordinal()]);
    }
    if (timestamp <= 0) {
      throw new IllegalArgumentException("invalid timestamp: " + timestamp);
    }
    
    final String value = words[TelnetIndex.VALUE.ordinal()];
    if (value.length() <= 0) {
      throw new IllegalArgumentException("empty value");
    }
    
    final HashMap<String, String> tags = new HashMap<String, String>();
    for (int i = TelnetIndex.TAGS.ordinal(); i < words.length; i++) {
      if (!words[i].isEmpty()) {
        Tags.parse(tags, words[i]);
      }
    }
    
    if (spatial_agg != null && temporal_agg == null) {
      temporal_agg = spatial_agg;
    }
    
    if (Tags.looksLikeInteger(value)) {
      return tsdb.addAggregatePoint(metric, timestamp, Tags.parseLong(value),
          tags, spatial_agg != null ? true : false, interval, temporal_agg,
              spatial_agg);
    } else if (Tags.fitsInFloat(value)) {  // floating point value
      return tsdb.addAggregatePoint(metric, timestamp, Float.parseFloat(value),
          tags, spatial_agg != null ? true : false, interval, temporal_agg,
              spatial_agg);
    } else {
      return tsdb.addAggregatePoint(metric, timestamp, Double.parseDouble(value),
          tags, spatial_agg != null ? true : false, interval, temporal_agg,
              spatial_agg);
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
  @Override
  protected IncomingDataPoint getDataPointFromString(final TSDB tsdb, 
                                                     final String[] words) {
    final RollUpDataPoint dp = new RollUpDataPoint();
    
    final String interval_agg = words[TelnetIndex.INTERVAL_AGG.ordinal()];
    String interval = null;
    String temporal_agg = null;
    String spatial_agg = null;
    // if the interval_agg has a - in it, then it's an interval. If there's a :
    // then it is both. If no dash or colon then it's just a spatial agg.
    final String[] interval_parts = interval_agg.split(":");
    final int dash = interval_parts[0].indexOf("-");
    if (dash > -1) {
      interval = interval_parts[0].substring(0,dash);
      temporal_agg = interval_parts[0].substring(dash + 1);
      
    } else if (interval_parts.length == 1) {
      spatial_agg = interval_parts[0];
    } 
    if (interval_parts.length > 1) {
      spatial_agg = interval_parts[1];
    }
    dp.setInterval(interval);
    dp.setAggregator(temporal_agg);
    dp.setGroupByAggregator(spatial_agg);
    
    dp.setMetric(words[TelnetIndex.METRIC.ordinal()]);
    
    if (words[TelnetIndex.TIMESTAMP.ordinal()].contains(".")) {
      dp.setTimestamp(Tags.parseLong(words[TelnetIndex.TIMESTAMP.ordinal()]
          .replace(".", ""))); 
    } else {
      dp.setTimestamp(Tags.parseLong(words[TelnetIndex.TIMESTAMP.ordinal()]));
    }
    
    dp.setValue(words[TelnetIndex.VALUE.ordinal()]);
    
    final HashMap<String, String> tags = new HashMap<String, String>();
    for (int i = TelnetIndex.TAGS.ordinal(); i < words.length; i++) {
      if (!words[i].isEmpty()) {
        Tags.parse(tags, words[i]);
      }
    }
    dp.setTags(tags);
    return dp;
  }
}
