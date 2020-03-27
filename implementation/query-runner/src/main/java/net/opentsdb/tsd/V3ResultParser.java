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
  
  @Override
  public void parse(final String path, final QueryConfig config, final String[] tags) {
    final File file = new File(path);
    try {
      final String json = Files.toString(file, Const.UTF8_CHARSET);
      final JsonNode root = JSON.getMapper().readTree(json);
      
      long last_ts = Long.MIN_VALUE;
      int result_count = 0;
      int time_series_count = 0;
      int nans = 0;
      int values = 0;
      
      final JsonNode results = root.get("results");
      if (results == null || results.isNull()) {
        LOG.debug("No data for " + config.id);
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
        for (final JsonNode series : temp) {
          temp = series.get("NumericType");
          if (temp.isArray()) {
            // array
            int index = 0;
            for (final JsonNode value : temp) {
              if (Double.isNaN(value.asDouble())) {
                nans++;
              } else {
                values++;
                index++;
                had_data = true;
              }
            }
            
            if (index == 0) {
              // nothing
              continue;
            }
            
            TimeStamp ts = new SecondTimeStamp(ts_start);
            for (int i = 0; i < index; i++) {
              ts.add(interval);
            }
            if (ts.epoch() > last_ts) {
              last_ts = ts.epoch();
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
      LOG.debug("Successfully parsed [" + config.id + "] Results: " + result_count 
          + " Time series: " + time_series_count + " Values: " + values 
          + " Nans: " + nans + " Lag: " + delta + "(s)");
    } catch (IOException e) {
      LOG.error("Failed to open file: " + path, e);
    }
  }

}
