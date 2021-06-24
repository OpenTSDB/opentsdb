// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import com.stumbleupon.async.Deferred;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.auth.AuthState;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.LowLevelMetricData.HashedLowLevelMetricData;
import net.opentsdb.data.LowLevelMetricData.ValueFormat;
import net.opentsdb.data.LowLevelTimeSeriesData;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Simple little consumer to periodically record the parsed data for testing
 * converters.
 */
public class DummyKafkaConsumer extends BaseTSDBPlugin implements
        TimeSeriesDataConsumer,
        TimeSeriesDataConsumerFactory,
        TimerTask {
  private static final Logger LOG = LoggerFactory.getLogger(DummyKafkaConsumer.class);

  @Override
  public void run(Timeout timeout) throws Exception {
    long events = 0;
    long good = 0;
    long bad = 0;
    for (Stats stats : TL.values()) {
      events += stats.events;
      good += stats.goodTS;
      bad += stats.badTS;
    }
    LOG.info("Kafka Consumer: Events {} Good TS {}  Bad TS {}  TS per Event {}", events, good, bad,
            (good / events));
    tsdb.getMaintenanceTimer().newTimeout(this, 60, TimeUnit.SECONDS);
  }

  static class Stats {
    volatile long events;
    volatile long goodTS;
    volatile long badTS;
  }

  static Map<Long, Stats> TL = new ConcurrentHashMap<>();

  @Override
  public String type() {
    return "DUMMY";
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    this.tsdb = tsdb;
    tsdb.getMaintenanceTimer().newTimeout(this, 60, TimeUnit.SECONDS);
    return Deferred.fromResult(null);
  }

  @Override
  public void write(AuthState state, TimeSeriesDatum datum, WriteCallback callback) {

  }

  @Override
  public void write(AuthState state, TimeSeriesSharedTagsAndTimeData data, WriteCallback callback) {

  }

  @Override
  public void write(AuthState state, LowLevelTimeSeriesData data, WriteCallback callback) {
    Stats stats = get();
    if (data instanceof HashedLowLevelMetricData) {
      HashedLowLevelMetricData hashed = (HashedLowLevelMetricData) data;
      stats.events++;
      while (hashed.advance()) {
        if (stats.goodTS++ % 10_000 == 0) {
          StringBuilder buf = new StringBuilder();
          buf.append("metric=")
             .append(new String(hashed.metricBuffer(), hashed.metricStart(), hashed.metricLength(), StandardCharsets.UTF_8))
          .append(" tags=");
          int i = 0;
          while (hashed.advanceTagPair()) {
            if (i++ > 0) {
              buf.append("  ");
            }
            buf.append(new String(hashed.tagsBuffer(), hashed.tagKeyStart(), hashed.tagKeyLength()))
                    .append(" => ")
                    .append(new String(hashed.tagsBuffer(), hashed.tagValueStart(), hashed.tagValueLength()));
          }
          buf.append(", timestamp=")
                  .append(hashed.timestamp().epoch())
                  .append(", value=");
          if (hashed.valueFormat() == ValueFormat.DOUBLE) {
            buf.append(hashed.doubleValue());
          } else {
            buf.append(hashed.longValue());
          }
          buf.append(", hash=")
                  .append(hashed.timeSeriesHash());
          LOG.info(buf.toString());
        }
      }
    } else {
      stats.badTS++;
    }
  }

  @Override
  public TimeSeriesDataConsumer consumer() {
    return this;
  }

  Stats get() {
    Thread t = Thread.currentThread();
    Stats stats = TL.get(t.getId());
    if (stats == null) {
      stats = new Stats();
      TL.put(t.getId(), stats);
    }
    return stats;
  }
}
