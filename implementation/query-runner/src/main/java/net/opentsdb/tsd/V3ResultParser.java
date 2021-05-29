// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.tsd;

import java.io.File;
import java.io.IOException;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Files;

import net.opentsdb.common.Const;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * Parses the TSDB v3 format.
 * 
 * @since 3.0
 */
public class V3ResultParser implements ResultParser {
  private static Logger LOG = LoggerFactory.getLogger(V3ResultParser.class);
  
  private static final String RESULT_COUNT = "query.parser.results";
  private static final String TIMESERIES_COUNT = "query.parser.timeseries";
  private static final String VALUES_COUNT = "query.parser.values";
  private static final String NANS_COUNT = "query.parser.nans";
  private static final String TIMESTAMP_LAG = "query.parser.value.lag";
  private static final String LATEST_DP = "query.parser.value.latest";
  
  @Override
  public void parse(final String path, final QueryConfig config, final String[] tags) {
    final File file = new File(path);
    try {
      final String json = Files.toString(file, Const.UTF8_CHARSET);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Raw results for " + config.id + " " + Arrays.toString(tags) 
          + "\n" + json);
      }
      final JsonNode root = JSON.getMapper().readTree(json);
      
      long last_ts = Long.MIN_VALUE;
      int result_count = 0;
      int time_series_count = 0;
      int nans = 0;
      int values = 0;
      
      final JsonNode results = root.get("results");
      if (results == null || results.isNull()) {
        LOG.debug("No data for " + config.id+ " " + Arrays.toString(tags));
        return;
      }
      
      for (final JsonNode r : results) {
        JsonNode temp = r.get("timeSpecification");
        int ts_start = 0;
        TemporalAmount interval = null;
        boolean had_data = false;
        if (temp != null && !temp.isNull()) {
          ts_start = temp.get("start").asInt();
          interval = DateTime.parseDuration2(temp.get("interval").asText());
        }
        
        temp = r.get("data");
        if (temp == null || temp.isNull()) {
          continue;
        }
        
        long series_last_ts = -1;
        double last_dp = Double.NaN;
        for (final JsonNode series : temp) {
          temp = series.get("NumericType");
          if (temp.isArray()) {
            // TODO - handle ms in the future.
            TimeStamp ts = new SecondTimeStamp(ts_start);
            // array
            int index = 0;
            for (final JsonNode value : temp) {
              if (Double.isNaN(value.asDouble())) {
                nans++;
              } else {
                values++;
                index++;
                had_data = true;
                if (config.post_latest_dps) {
                  // TODO - ints vs floats
                  series_last_ts = ts.epoch();
                  last_dp = value.asDouble();
                }
              }
              ts.add(interval);
            }
            
            if (index == 0) {
              // nothing
              continue;
            }
            
            if (ts.epoch() > last_ts) {
              last_ts = ts.epoch();
            }
            
            if (config.post_latest_dps && !Double.isNaN(last_dp)) {
              final String[] series_tags = ResultParser.parseIdToLatestDpTags(series, tags);
              // TODO - figure out a way to set the timestamp. 
              TsdbQueryRunner.TSDB.getStatsCollector().setGauge(
                  LATEST_DP, last_dp, series_tags);
              if (LOG.isTraceEnabled()) {
                LOG.trace("Reporting last DP for " + config.id + " for " 
                    + Arrays.toString(series_tags) + " @"
                    + series_last_ts + " => " + last_dp);
              }
            }
          } else {
            // just timestamp and numbers
            final Iterator<Entry<String, JsonNode>> it = temp.fields();
            while (it.hasNext()) {
              final Entry<String, JsonNode> value = it.next();
              int ts = Integer.parseInt(value.getKey());
              if (ts > last_ts) {
                last_ts = ts;
              }
              
              if (Double.isNaN(value.getValue().asDouble())) {
                nans++;
              } else {
                values++;
                had_data = true;
                if (config.post_latest_dps) {
                  // TODO - ints vs floats
                  series_last_ts = ts;
                  last_dp = value.getValue().asDouble();
                  final String[] series_tags = ResultParser.parseIdToLatestDpTags(series, tags);
                  // TODO - figure out a way to set the timestamp. 
                  TsdbQueryRunner.TSDB.getStatsCollector().setGauge(
                      LATEST_DP, last_dp, series_tags);
                  if (LOG.isTraceEnabled()) {
                    LOG.trace("Reporting last DP for " + config.id + " for " 
                        + Arrays.toString(series_tags) + " @"
                        + series_last_ts + " => " + last_dp);
                  }
                }
              }
            }
          }
          
          time_series_count++;
        }
        
        if (had_data) {
          result_count++;
        }
      }
      
      // send out some metrics now.
      TsdbQueryRunner.TSDB.getStatsCollector().setGauge(
          RESULT_COUNT, result_count, tags);
      TsdbQueryRunner.TSDB.getStatsCollector().setGauge(
          TIMESERIES_COUNT, time_series_count, tags);
      TsdbQueryRunner.TSDB.getStatsCollector().setGauge(
          VALUES_COUNT, values, tags);
      TsdbQueryRunner.TSDB.getStatsCollector().setGauge(
          NANS_COUNT, nans, tags);
      final int delta;
      if (values > 0) {
        delta = (int) ((DateTime.currentTimeMillis() / 1000) - last_ts);
          TsdbQueryRunner.TSDB.getStatsCollector().setGauge(
              TIMESTAMP_LAG, delta, tags);
      } else {
        delta = 0;
      }
      LOG.debug("Successfully parsed [" + config.id + " " + Arrays.toString(tags) 
          + "] Results: " + result_count 
          + " Time series: " + time_series_count + " Values: " + values 
          + " Nans: " + nans + " Lag: " + delta + "(s)");
    } catch (IOException e) {
      LOG.error("Failed to open file: " + path, e);
    }
  }

}
