// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Histogram;
import net.opentsdb.core.HistogramPojo;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.SimpleHistogram;
import net.opentsdb.core.SimpleHistogramDecoder;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.utils.Config;

/**
 * The class responsible for writing histograms from Telnet calls or HTTP 
 * requests.
 * 
 * @since 2.4
 */
public class HistogramDataPointRpc extends PutDataPointRpc 
  implements TelnetRpc, HttpRpc {
  
  /** Type ref for the histo pojo. */
  private static final TypeReference<ArrayList<HistogramPojo>> TYPE_REF =
      new TypeReference<ArrayList<HistogramPojo>>() {};
      
  /** Whether or not histograms are enabled. */
  private final boolean enabled;

  /**
   * Default ctor. Checks the "tsd.core.histograms.config" value to see if 
   * histograms are enabled. If they are not, then exceptions are returned. 
   * @param config A non-null config to pull data from.
   */
  public HistogramDataPointRpc(final Config config) {
    super(config);
    // drats, since we can't look at the manager we'll look at the settings.
    final String histo_config = config.getString("tsd.core.histograms.config");
    if (Strings.isNullOrEmpty(histo_config)) {
      enabled = false;
    } else {
      enabled = true;
    }
  }

  @Override
  public void execute(final TSDB tsdb, final HttpQuery query) throws IOException {
    http_requests.incrementAndGet();
    
    if (!enabled) {
      throw new BadRequestException(HttpResponseStatus.SERVICE_UNAVAILABLE,
          "Histogram storage has not been enabled. Check the "
          + "'tsd.core.histograms.config' configuration.");
    }
    
    // only accept POST
    if (query.method() != HttpMethod.POST) {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED,
          "Method not allowed", "The HTTP method [" + query.method().getName() +
          "] is not permitted for this endpoint");
    }
    
    final List<HistogramPojo> dps = query.serializer()
        .parsePutV1(HistogramPojo.class, TYPE_REF);
    processDataPoint(tsdb, query, dps);
  }
  
  @Override
  protected Deferred<Object> importDataPoint(final TSDB tsdb, 
                                             final String[] words) {
    if (!enabled) {
      throw new IllegalArgumentException(
          "Histogram storage has not been enabled. Check the "
          + "'tsd.core.histograms.config' configuration.");
    }
    
    words[0] = null; // Ditch the "histogram".
    if (words.length < 5) {  // Need at least: metric timestamp value tag
      //               ^ 5 and not 4 because words[0] is "histogram".
      throw new IllegalArgumentException("not enough arguments"
                                         + " (need least 5, got " 
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
    
    boolean has_id = false;
    int id = 0;
    try {
      id = Integer.parseInt(words[3]);
      has_id = true;
    } catch (NumberFormatException e) { }
    final String value;
    if (has_id) {
      value = words[4];
    } else {
      // it's a simple Id
      id = tsdb.histogramManager().getCodec(SimpleHistogramDecoder.class);
      value = words[3];
    }
    if (value.length() <= 0) {
      throw new IllegalArgumentException("empty histogram value");
    }
    final HashMap<String, String> tags = new HashMap<String, String>();
    for (int i = has_id ? 5 : 4; i < words.length; i++) {
      if (!words[i].isEmpty()) {
        Tags.parse(tags, words[i]);
      }
    }
    
    // validation and prepend the ID.
    try {
      final Histogram dp;
      if (has_id) {
        dp = tsdb.histogramManager().decode(id, 
          HistogramPojo.base64StringToBytes(value), false);
      } else {
        dp = parseTelnet(tsdb, value);
      }
      return tsdb.addHistogramPoint(metric, timestamp, 
          tsdb.histogramManager().encode(id, dp, true), tags);
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }
  
  @Override
  protected IncomingDataPoint getDataPointFromString(final TSDB tsdb, 
                                                     final String[] words) {
    final long timestamp;
    if (words[2].contains(".")) {
      timestamp = Tags.parseLong(words[2].replace(".", "")); 
    } else {
      timestamp = Tags.parseLong(words[2]);
    }
    if (timestamp <= 0) {
      throw new IllegalArgumentException("invalid timestamp: " + timestamp);
    }
    
    boolean has_id = false;
    int id = 0;
    try {
      id = Integer.parseInt(words[3]);
      has_id = true;
    } catch (NumberFormatException e) { }
    final String value;
    if (has_id) {
      value = words[4];
    } else {
      // it's a simple Id
      id = tsdb.histogramManager().getCodec(SimpleHistogramDecoder.class);
      value = words[3];
    }

    final HistogramPojo dp = new HistogramPojo();
    dp.setMetric(words[1]);
    dp.setTimestamp(timestamp);
    dp.setId(id);
    if (has_id) {
      dp.setValue(value);
    } else {
      dp.setValue(HistogramPojo.bytesToBase64String(
          parseTelnet(tsdb, value).histogram(false)));
    }
    final HashMap<String, String> tags = new HashMap<String, String>();
    for (int i = has_id ? 5 : 4; i < words.length; i++) {
      if (!words[i].isEmpty()) {
        Tags.parse(tags, words[i]);
      }
    }
    dp.setTags(tags);
    return dp;
  }

  SimpleHistogram parseTelnet(final TSDB tsdb, final String encoded) {
    final SimpleHistogram shdp = new SimpleHistogram(tsdb.histogramManager()
        .getCodec(SimpleHistogramDecoder.class));
    
    final String[] buckets = Tags.splitString(encoded, ':');
    if (buckets.length < 1) {
      throw new IllegalArgumentException("Must have at least one bucket in the "
          + "histogram.");
    }
    
    for (final String bucket : buckets) {
      final String[] kv = Tags.splitString(bucket, '=');
      if (kv.length != 2) {
        throw new IllegalArgumentException("Improperly formatted bucket: " 
            + bucket);
      }
      
      kv[0] = kv[0].toLowerCase();
      if (kv[0].equals("u")) {
        shdp.setUnderflow(Long.parseLong(kv[1]));
      } else if (kv[0].equals("o")) {
        shdp.setOverflow(Long.parseLong(kv[1]));
      } else {
        final String[] bounds = Tags.splitString(kv[0], ',');
        if (bounds.length != 2) {
          throw new IllegalArgumentException("Improperly formatted bounds: " 
              + bucket);
        }
        shdp.addBucket(Float.parseFloat(bounds[0]), Float.parseFloat(bounds[1]), 
            Long.parseLong(kv[1]));
      }
    }
    return shdp;
  }
  
  @VisibleForTesting
  boolean enabled() {
    return enabled;
  }
}
