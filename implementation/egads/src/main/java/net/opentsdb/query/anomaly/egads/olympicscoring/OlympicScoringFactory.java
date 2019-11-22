// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.anomaly.egads.olympicscoring;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.anomaly.AnomalyConfig.ExecutionMode;
import net.opentsdb.query.anomaly.AnomalyPredictionState;
import net.opentsdb.query.anomaly.PredictionCache;
import net.opentsdb.query.anomaly.AnomalyPredictionState.State;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.downsample.DownsampleFactory;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;
import net.opentsdb.query.anomaly.egads.olympicscoring.OlympicScoringConfig.Builder;
import net.opentsdb.query.plan.QueryPlanner;

/**
 * Class that returns OlympicScoringNodes.
 * <b>NOTE:</b> If a query comes in with the PREDICT or EVALUATE mode set, the
 * {@link SetupGraph} method will check the cache for the state of the prediction.
 *
 * @since 3.0
 */
public class OlympicScoringFactory extends BaseQueryNodeFactory<
    OlympicScoringConfig, OlympicScoringNode> {
  private static final Logger LOG = LoggerFactory.getLogger(
      OlympicScoringFactory.class);
  
  public static final String TYPE = "OlympicScoring";
  public static final int TYPE_HASH = Const.HASH_FUNCTION().newHasher()
      .putString(TYPE, Const.UTF8_CHARSET).hash().asInt();

  private TSDB tsdb;
  private volatile PredictionCache cache;
  private String host_name;
  
  @Override
  public OlympicScoringNode newNode(final QueryPipelineContext context,
                                    final OlympicScoringConfig config) {
    return new OlympicScoringNode(this, context, config);
  }
  
  @Override
  public OlympicScoringConfig parseConfig(final ObjectMapper mapper, 
                                          final TSDB tsdb,
                                          final JsonNode node) {
    Builder builder = new Builder();
    Builder.parseConfig(mapper, tsdb, node, builder);
    
    JsonNode n = node.get("baselineQuery");
    if (n != null && !n.isNull()) {
      builder.setBaselineQuery(SemanticQuery.parse(tsdb, n).build());
    }
     
    n = node.get("baselinePeriod");
    if (n != null && !n.isNull()) {
      builder.setBaselinePeriod(n.asText());
    }
    
    n = node.get("baselineNumPeriods");
    if (n != null && !n.isNull()) {
      builder.setBaselineNumPeriods(n.asInt());
    }
    
    n = node.get("baselineAggregator");
    if (n != null && !n.isNull()) {
      builder.setBaselineAggregator(n.asText());
    }
    
    n = node.get("excludeMax");
    if (n != null && !n.isNull()) {
      builder.setExcludeMax(n.asInt());
    }
    
    n = node.get("excludeMin");
    if (n != null && !n.isNull()) {
      builder.setExcludeMin(n.asInt());
    }
    
    n = node.get("upperThresholdBad");
    if (n != null && !n.isNull()) {
      builder.setUpperThresholdBad(n.asDouble());
    }
    
    n = node.get("upperThresholdWarn");
    if (n != null && !n.isNull()) {
      builder.setUpperThresholdWarn(n.asDouble());
    }
    
    n = node.get("upperIsScalar");
    if (n != null && !n.isNull()) {
      builder.setUpperIsScalar(n.asBoolean());
    }
    
    n = node.get("lowerThresholdBad");
    if (n != null && !n.isNull()) {
      builder.setLowerThresholdBad(n.asDouble());
    }
    
    n = node.get("lowerThresholdWarn");
    if (n != null && !n.isNull()) {
      builder.setLowerThresholdWarn(n.asDouble());
    }
    
    n = node.get("lowerIsScalar");
    if (n != null && !n.isNull()) {
      builder.setLowerIsScalar(n.asBoolean());
    }
    
    return builder.build();
  }

  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final OlympicScoringConfig config, 
                         final QueryPlanner planner) {
    // UGG!!! initialization order issue.
    if (cache == null) {
      synchronized (this) {
        if (cache == null) {
          cache = tsdb.getRegistry().getDefaultPlugin(
              PredictionCache.class);
          LOG.info("EGADS Cache: " + cache);
        }
      }
    }
    if (config.getMode() == null) {
      throw new RuntimeException("MODE can't be null!!");
    }
    if (cache == null || config.getMode() == ExecutionMode.CONFIG) {
      return;
    }
    
    // we check the state here as we want to avoid querying current data while 
    // the prediction is being generated.
    final int prediction_start = predictionStartTime(context.query(), config, planner);
    final byte[] cache_key = generateCacheKey(context.query(), prediction_start);
    AnomalyPredictionState state = cache.getState(cache_key);
    if (state == null) {
      return;
    }
    
    if (state.state == State.RUNNING) {
      throw new QueryExecutionException("Prediction is still being generated "
          + "for prediction start [" + prediction_start + "] and key " 
          + Arrays.toString(cache_key) + " State: " 
          + JSON.serializeToString(state), 423);
    }
    
    if (state.state == State.ERROR) {
      throw new QueryExecutionException("Prediction failed generation and can "
          + "be retried later for prediction start [" + prediction_start + "] and key " 
          + Arrays.toString(cache_key) + " State: " 
          + JSON.serializeToString(state), 424);
    }
    
    // otherwise we're good so pass it on.
  }
  
  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    cache = tsdb.getRegistry().getDefaultPlugin(PredictionCache.class);
    LOG.info("EGADS Cache: " + cache);
    try {
      host_name = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Failed to get the hostname", e);
    } 
    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }

  byte[] generateCacheKey(final TimeSeriesQuery query, final int prediction_start) {
    // TODO - include: jitter timestamp, full query hash, model ID
    // hash of model ID, query hash, prediction timestamp w jitter
    byte[] key = new byte[4 + 8 + 4];
    Bytes.setInt(key, OlympicScoringFactory.TYPE_HASH, 0);
    Bytes.setLong(key, query.buildHashCode().asLong(), 4);
    Bytes.setInt(key, prediction_start, 12); 
    return key;
  }

  int predictionStartTime(final TimeSeriesQuery query, 
                          final OlympicScoringConfig config, 
                          final QueryPlanner planner) {
    final long baseline_span = DateTime.parseDuration(config.getBaselinePeriod()) / 1000;
    final ChronoUnit model_units; 
    if (baseline_span < 86400) {
      model_units = ChronoUnit.HOURS;
    } else {
      model_units = ChronoUnit.DAYS;
    }
    
    DownsampleConfig ds = null;
    for (final QueryNodeConfig node : config.getBaselineQuery().getExecutionGraph()) {
      if (node instanceof DownsampleConfig) {
        ds = (DownsampleConfig) node;
        break;
      }
    }
    if (ds == null) {
      throw new IllegalStateException("Downsample can't be null.");
    }
    
    final long query_time_span = query.endTime().msEpoch() - 
        query.startTime().msEpoch();
    final String ds_interval;
    if (ds.getInterval().equalsIgnoreCase("AUTO")) {
      final QueryNodeFactory dsf = tsdb.getRegistry()
          .getQueryNodeFactory(DownsampleFactory.TYPE);
      if (dsf == null) {
        //LOG.error("Unable to find a factory for the downsampler.");
      }
      if (((DownsampleFactory) dsf).intervals() == null) {
        //LOG.error("No auto intervals for the downsampler.");
      }
      ds_interval = DownsampleFactory.getAutoInterval(query_time_span, 
          ((DownsampleFactory) dsf).intervals(), null);
    } else {
      ds_interval = ds.getInterval();
    }
    
    int jitter = jitter(query, model_units);
    TemporalAmount jitter_duration = Duration.ofSeconds(jitter);
    
    final TimeStamp start = query.startTime().getCopy();
    final ChronoUnit duration = model_units;
    start.snapToPreviousInterval(1, duration);
    start.add(jitter_duration);
    // now snap to ds interval
    start.snapToPreviousInterval(DateTime.getDurationInterval(ds_interval), 
        DateTime.unitsToChronoUnit(DateTime.getDurationUnits(ds_interval)));
    if (start.compare(Op.GT, query.startTime())) {
      start.subtract(model_units == ChronoUnit.HOURS ? 
          Duration.ofHours(1) : Duration.ofDays(1));
    }
    return (int) start.epoch();
  }
  
  int jitter(final TimeSeriesQuery query, final ChronoUnit model_units) {
    if (model_units == ChronoUnit.DAYS) {
      // 1 hour jitter on 1m
      return (int) Math.abs(query.buildHashCode().asLong() % 59) * 60;
    } else {
      // 5m jitter on 15s
      return (int) Math.abs(query.buildHashCode().asLong() % 20) * 15;
    }
  }
  
  // TEMP!
  void setCache(PredictionCache cache) {
    this.cache = cache;
  }
  
  PredictionCache cache() {
    return cache;
  }

  String hostName() {
    return host_name;
  }
}
