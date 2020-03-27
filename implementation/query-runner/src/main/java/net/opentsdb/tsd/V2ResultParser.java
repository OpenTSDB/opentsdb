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
import java.util.Iterator;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Files;

import net.opentsdb.common.Const;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * Parses the TSDB v2 format.
 * 
 * @since 3.0
 */
public class V2ResultParser implements ResultParser {
  private static Logger LOG = LoggerFactory.getLogger(V2ResultParser.class);
  
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
      int time_series_count = 0;
      int nans = 0;
      int values = 0;
      
      for (final JsonNode series : root) {
        JsonNode temp = series.get("dps");
        if (temp == null || temp.isNull()) {
          continue;
        }
        
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
          }
        }
        
        time_series_count++;
      }
      
      // send out some metrics now.
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
      LOG.debug("Successfully parsed [" + config.id + "] Time series: " 
          + time_series_count + " Values: " + values + " Nans: " + nans 
          + " Lag: " + delta + "(s)");
    } catch (IOException e) {
      LOG.error("Failed to open file: " + path, e);
    }
  }

}
