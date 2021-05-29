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
package net.opentsdb.stats;

import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import io.ultrabrew.metrics.MetricRegistry;
import io.ultrabrew.metrics.Timer;
import io.ultrabrew.metrics.data.DistributionBucket;
import io.ultrabrew.metrics.reporters.influxdb.InfluxDBReporter;
import io.ultrabrew.metrics.reporters.opentsdb.OpenTSDBReporter;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.DateTime;

/**
 * A stats reporter for OpenTSDB V3 that uses the memory-efficient Ultrabrew 
 * library to report metrics to Influx or OpenTSDB. Other reporters can be
 * added later on.
 * 
 * @since 3.0
 */
public class UltrabrewMetrics extends BaseTSDBPlugin implements StatsCollector {
  private static final Logger LOG = LoggerFactory.getLogger(UltrabrewMetrics.class);
  
  public static final String TYPE = "UltrabrewMetrics";
  public static final String MAX_CARDINALITY_KEY = "tsd.stats.cardinality.max";
  public static final String HISTOS_KEY = "tsd.stats.histogram.metrics";
  public static final String HISTOS_DIST_KEY = "tsd.stats.histogram.distribution";
  public static final String TAGS_KEY = "tsd.stats.tags.global";
  
  public static final String TSD_HOST = "tsd.stats.reporter.opentsdb.host";
  public static final String TSD_ENDPOINT = "tsd.stats.reporter.opentsdb.endpoint";
  public static final String TSD_BATCH_SIZE = "tsd.stats.reporter.opentsdb.batchSize";
  public static final String TSD_REQUENCY = "tsd.stats.reporter.opentsdb.frequency";
  
  public static final String INFLUX_URI = "tsd.stats.reporter.influx.uri";
  public static final String INFLUX_DB = "tsd.stats.reporter.influx.database";
  public static final String INFLUX_ENDPOINT = "tsd.stats.reporter.influx.endpoint";
  public static final String INFLUX_FREQUENCY = "tsd.stats.reporter.influx.frequency";
  
  public static final TypeReference<List<String>> LIST_TYPE = 
      new TypeReference<List<String>>() { };
  public static final DistributionBucket DISTRIBUTION = 
      new DistributionBucket(new long[] {
          0, 10, 50, 100, 250, 500, 750, 1000, 2000, 3000, 5000, 10000, 30000, 60000});
  
  private int max_cardinality;
  private InfluxDBReporter influx_reporter;
  private OpenTSDBReporter opentsdb_reporter;
  private MetricRegistry metric_registry;
  private String[] static_tags;
  
  @Override
  public void incrementCounter(final String metric, final String... tags) {
    try {
      metric_registry.counter(metric, max_cardinality).inc(concatenateTags(tags));
    } catch (Throwable t) {
      LOG.error("Failed to write metric", t);
    }
  }

  @Override
  public void incrementCounter(final String metric, final long amount, final String... tags) {
    try {
      metric_registry.counter(metric, max_cardinality).inc(amount, concatenateTags(tags));
    } catch (Throwable t) {
      LOG.error("Failed to write metric", t);
    }
  }

  @Override
  public void setGauge(final String metric, final long amount, final String... tags) {
    try {
      metric_registry.gauge(metric, max_cardinality).set(amount, concatenateTags(tags));
    } catch (Throwable t) {
      LOG.error("Failed to write metric", t);
    }
  }

  @Override
  public void setGauge(String metric, double amount, String... tags) {
    try {
      metric_registry.gaugeDouble(metric, max_cardinality).set(amount, concatenateTags(tags));
    } catch (Throwable t) {
      LOG.error("Failed to write metric", t);
    }
  }

  @Override
  public StatsTimer startTimer(final String metric, final ChronoUnit units) {
    return new UltrabrewTimerWrapper(metric, units);
  }
  
  @Override
  public void addTime(final String metric, 
                      long duration, 
                      final ChronoUnit units,
                      final String... tags) {
    switch (units) {
    case NANOS:
      break;
    case MICROS:
      duration = duration * 1000;
      break;
    case MILLIS:
      duration = duration * 1000 * 1000;
      break;
    case SECONDS:
      duration = duration * 1000 * 1000 * 1000;
      break;
    default:
      throw new IllegalArgumentException("Units must be between NANOs and SECONDS");
    }
    try {
      metric_registry.timer(metric).update(duration, concatenateTags(tags));
    } catch (Throwable t) {
      LOG.error("Failed to write metric", t);
    }
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    
    LOG.info("Initializing Ultrabrew metrics...");
    metric_registry = new MetricRegistry();
    registerConfigs();
    max_cardinality = tsdb.getConfig().getInt(MAX_CARDINALITY_KEY);
    List<String> histos = tsdb.getConfig().getTyped(HISTOS_KEY, LIST_TYPE);
    String dist_config = tsdb.getConfig().getString(HISTOS_DIST_KEY);
    final DistributionBucket distribution;
    if (Strings.isNullOrEmpty(dist_config)) {
      distribution = DISTRIBUTION;
    } else {
      String[] splits = dist_config.split(",");
      long[] buckets = new long[splits.length];
      for (int i = 0; i < splits.length; i++) {
        buckets[i] = Long.parseLong(splits[i]);
      }
      distribution = new DistributionBucket(buckets);
    }
    
    if (!Strings.isNullOrEmpty(tsdb.getConfig().getString(TSD_HOST))) {
      OpenTSDBReporter.Builder reporter_builder =
          OpenTSDBReporter.builder()
              .withBaseUri(URI.create(tsdb.getConfig().getString(TSD_HOST)))
              .withBatchSize(tsdb.getConfig().getInt(TSD_BATCH_SIZE))
              .withWindowSize(tsdb.getConfig().getInt(TSD_REQUENCY));
      if (!Strings.isNullOrEmpty(tsdb.getConfig().getString(TSD_ENDPOINT))) {
        reporter_builder.withApiEndpoint(tsdb.getConfig().getString(TSD_ENDPOINT));
      }
      if (histos != null && !histos.isEmpty()) {
        for (final String metric : histos) {
          reporter_builder.addHistogram(metric, distribution);
        }
      }
      opentsdb_reporter = reporter_builder.build();
      metric_registry.addReporter(opentsdb_reporter);
      LOG.info("Created OpenTSDB reporter for " + tsdb.getConfig().getString(TSD_HOST));
    }
    
    if (!Strings.isNullOrEmpty(tsdb.getConfig().getString(INFLUX_URI))) {
      String endpoint = tsdb.getConfig().getString(INFLUX_ENDPOINT);
      InfluxDBReporter.Builder reporter_builder = InfluxDBReporter.builder()
          .withBaseUri(URI.create(tsdb.getConfig().getString(INFLUX_URI)))
          .withEndpoint(endpoint)
          .withDatabase(tsdb.getConfig().getString(INFLUX_DB))
          .withWindowSize(tsdb.getConfig().getInt(INFLUX_FREQUENCY));
      if (histos != null && !histos.isEmpty()) {
        for (final String metric : histos) {
          reporter_builder.addHistogram(metric, distribution);
        }
      }
      influx_reporter = reporter_builder.build();
      metric_registry.addReporter(influx_reporter);
      LOG.info("Created Influx DB reporter for: " + tsdb.getConfig().getString(INFLUX_URI));
    }
    
    if (!Strings.isNullOrEmpty(tsdb.getConfig().getString(TAGS_KEY))) {
      final String raw = tsdb.getConfig().getString(TAGS_KEY);
      final String[] array = raw.split(",");
      if (array.length < 2 && !(array.length % 2 == 0)) {
        return Deferred.fromError(new IllegalArgumentException("Must have an "
            + "even number of tag keys and values: " + raw));
      }
      for (int i = 0; i < array.length; i++) {
        array[i] = array[i].trim();
      }
      static_tags = array;
      LOG.info("Reporting static tags: " + Arrays.toString(static_tags));
    }
    
    LOG.info("Successfully initialized Ultrabrew metrics.");
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    try {
      if (opentsdb_reporter != null) {
        opentsdb_reporter.close();
      }
    } catch (Exception e) {
      LOG.error("Failed to shutdown opentsdb reporter", e);
    }
    try {
      if (influx_reporter != null) {
        influx_reporter.close();
      }
    } catch (Exception e) {
      LOG.error("Failed to shutdown influx reporter", e);
    }
    
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  void registerConfigs() {
    if (!tsdb.getConfig().hasProperty(TAGS_KEY)) {
      tsdb.getConfig().register(TAGS_KEY, null, false, "An optional comma "
          + "separated list of tags to apply to every measurement. Treated as"
          + " key value pairs, so the count must be even.");
    }
    if (!tsdb.getConfig().hasProperty(MAX_CARDINALITY_KEY)) {
      tsdb.getConfig().register(MAX_CARDINALITY_KEY, 4096, false, "The maximum "
          + "cardinality of all metrics reported from this JVM.");
    }
    if (!tsdb.getConfig().hasProperty(HISTOS_KEY)) {
      tsdb.getConfig().register(
          ConfigurationEntrySchema.newBuilder()
          .setKey(HISTOS_KEY)
          .setType(LIST_TYPE)
          .setDescription("A list of metrics for which we want to track histograms.")
          .isNullable()
          .setSource(getClass().getName())
          .build());
    }
    if (!tsdb.getConfig().hasProperty(HISTOS_DIST_KEY)) {
      tsdb.getConfig().register(HISTOS_DIST_KEY, null, false, 
          "A comma separated list of bucket boundaries.");
    }
    
    if (!tsdb.getConfig().hasProperty(TSD_HOST)) {
      tsdb.getConfig().register(TSD_HOST, null, false, 
          "An OpenTSDB host to report stats to.");
    }
    if (!tsdb.getConfig().hasProperty(TSD_ENDPOINT)) {
      tsdb.getConfig().register(TSD_ENDPOINT, null, false, 
          "An OpenTSDB endpoint to use when reporting stats.");
    }
    if (!tsdb.getConfig().hasProperty(TSD_BATCH_SIZE)) {
      tsdb.getConfig().register(TSD_BATCH_SIZE, 64, false, 
          "The number of stats to batch in one request.");
    }
    if (!tsdb.getConfig().hasProperty(TSD_REQUENCY)) {
      tsdb.getConfig().register(TSD_REQUENCY, 60, false, 
          "How often, in seconds, to flush measurements to OpenTSDB.");
    }
    
    if (!tsdb.getConfig().hasProperty(INFLUX_URI)) {
      tsdb.getConfig().register(INFLUX_URI, null, false, 
          "An InfluxDB URI to flush stats to.");
    }
    if (!tsdb.getConfig().hasProperty(INFLUX_DB)) {
      tsdb.getConfig().register(INFLUX_DB, null, false, 
          "The database to report influx stats under.");
    }
    if (!tsdb.getConfig().hasProperty(INFLUX_ENDPOINT)) {
      tsdb.getConfig().register(INFLUX_ENDPOINT, null, false, 
          "The endpoint to report to. Must include the DB as well.");
    }
    if (!tsdb.getConfig().hasProperty(INFLUX_FREQUENCY)) {
      tsdb.getConfig().register(INFLUX_FREQUENCY, 60, false, 
          "How often, in seconds, to flush measurements to InfluxDB.");
    }
  }
  
  public class UltrabrewTimerWrapper implements StatsTimer {

    Timer timer;
    final long start_time;
    final ChronoUnit units;
    
    UltrabrewTimerWrapper(final String metric, final ChronoUnit units) {
      this.units = units;
      try {
        timer = metric_registry.timer(metric, max_cardinality);
      } catch (Throwable t) {
        LOG.error("Failed to write metric", t);
        timer = null;
      }
      switch (units) {
      case NANOS:
      case MICROS:
        start_time = DateTime.nanoTime();
        break;
      case MILLIS:
      case SECONDS:
        start_time = DateTime.currentTimeMillis();
        break;
      default:
        throw new IllegalArgumentException("Units must be in seconds or smaller.");
      }
    }
    
    @Override
    public void stop(final String... tags) {
      if (timer != null) {
        try {
          switch (units) {
          case NANOS:
            timer.update(DateTime.nanoTime() - start_time, tags);
            break;
          case MILLIS:
            timer.update(DateTime.currentTimeMillis() - start_time, tags);
            break;
          case MICROS:
            timer.update((DateTime.nanoTime() - start_time) / 1000, tags);
            break;
          case SECONDS:
            timer.update((DateTime.currentTimeMillis() - start_time) / 1000, tags);
            break;
          default:
            // can't be here
          }
        } catch (Throwable t) {
          LOG.error("Failed to write metric", t);
        }
      }
    }

    @Override
    public long startTime() {
      switch (units) {
      case NANOS:
      case MILLIS:
        return start_time;
      case MICROS:
      case SECONDS:
        return start_time / 1000;
      default:
        // can't be here
        return 0;
      }
      
    }
    
    @Override
    public ChronoUnit units() {
      return units;
    }
    
  }
  
  /**
   * Concatenates the incoming tags with the static tags if both are set or 
   * either if only one or non is set.
   * TODO - Not memory efficient to create these garbage arrays each time.
   * @param tags The tags to contact with. If null or empty just returns the static
   * tags.
   * @return The proper tag set to report.
   */
  String[] concatenateTags(final String... tags) {
    if (static_tags == null) {
      return tags;
    }
    if (tags == null || tags.length < 1) {
      return static_tags;
    }
    final String[] final_tags = new String[tags.length + static_tags.length];
    System.arraycopy(static_tags, 0, final_tags, 0, static_tags.length);
    System.arraycopy(tags, 0, final_tags, static_tags.length, tags.length);
    return final_tags;
  }
  
  /**
   * Helper for others to get the registry.
   * @return The registry.
   */
  public MetricRegistry getRegistry() {
    return metric_registry;
  }
}
