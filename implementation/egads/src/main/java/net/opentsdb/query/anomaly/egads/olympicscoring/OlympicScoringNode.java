// This file is part of OpenTSDB.
// Copyright (C) 2019-2021  The OpenTSDB Authors.
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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.QuerySinkCallback;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.query.anomaly.AnomalyPredictionState;
import net.opentsdb.query.anomaly.AnomalyPredictionState.State;
import net.opentsdb.query.anomaly.BaseAnomalyNode;
import net.opentsdb.query.anomaly.AnomalyConfig.ExecutionMode;
import net.opentsdb.query.anomaly.AnomalyPredictionResult;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * A node that runs the OlympicScoring algorithm from EGADs. This node will
 * check the cache and on miss, trigger the baseline prediction execution on
 * node initialization. Once the prediction and current results (sent to 
 * onNext()) are in, then the current values are evaluated against the prediction
 * and results sent upstream.
 * 
 * This class is pretty messy, particularly in the EVAL mode.
 * When eval is enabled, it's possible to get a query time range for 2 hours but
 * the prediction boundaries are set on a 1 hour bases. Thus we have multiple
 * 'prediction_index' values, in this case we'd have 2 indices. 1 for the earlier
 * hour and 1 for the latter. 
 * 
 * TODO - Check for null or empty currents and baselines.
 * TODO - Set proper state on baseline failures.
 * TODO - See that we stay open long enough to finish building
 * in predict mode, just return.
 *
 * @since 3.0
 */
public class OlympicScoringNode extends BaseAnomalyNode {
  private static final Logger LOG = LoggerFactory.getLogger(
      OlympicScoringNode.class);
  
  protected volatile BaselineQuery[][] baseline_queries;
  protected final TemporalAmount baseline_period;
  protected Properties properties;
  protected final TLongObjectMap<OlympicScoringBaseline[]> join;
  protected final QueryResultId data_source;
  
  public OlympicScoringNode(final QueryNodeFactory factory,
                            final QueryPipelineContext context,
                            final OlympicScoringConfig config) {
    super(factory, context, config);
    baseline_period = DateTime.parseDuration2(config.getBaselinePeriod());
    data_source = (QueryResultId) config.resultIds().get(0);
    join = new TLongObjectHashMap<OlympicScoringBaseline[]>();
  }
  
  void runBaseline(final int prediction_idx) {
    final OlympicScoringConfig config = (OlympicScoringConfig) this.config;
    TypeToken<? extends TimeSeriesId> id_type = null;
    properties = new Properties();
    properties.setProperty("TS_MODEL", "OlympicModel2");
    final int interval_count = DateTime.getDurationInterval(ds_interval);
    properties.setProperty("INTERVAL", Integer.toString(interval_count));
    final ChronoUnit ds_units = DateTime.unitsToChronoUnit(
        DateTime.getDurationUnits(ds_interval));
    properties.setProperty("INTERVAL_UNITS", ds_units.toString());
    if (config.getMode() == ExecutionMode.CONFIG) {
      properties.setProperty("WINDOW_SIZE", Long.toString(
          context.query().endTime().epoch() - context.query().startTime().epoch()));
      properties.setProperty("WINDOW_SIZE_UNITS", "SECONDS");
    } else {
      properties.setProperty("WINDOW_SIZE", "1");
      properties.setProperty("WINDOW_SIZE_UNITS", model_units.toString());
    }
    properties.setProperty("WINDOW_DISTANCE", Integer.toString(
        DateTime.getDurationInterval(config.getBaselinePeriod())));
    properties.setProperty("WINDOW_DISTANCE_UNITS", 
        DateTime.unitsToChronoUnit(
            DateTime.getDurationUnits(config.getBaselinePeriod())).toString());
    properties.setProperty("HISTORICAL_WINDOWS", Integer.toString(
        config.getBaselineNumPeriods()));
    properties.setProperty("WINDOW_AGGREGATOR", 
        config.getBaselineAggregator().toUpperCase());
    if (prediction_idx > 0) {
      properties.setProperty("MODEL_START", Long.toString(prediction_starts[prediction_idx]));
    } else {
      properties.setProperty("MODEL_START", Long.toString(prediction_starts[prediction_idx]));
    }
    properties.setProperty("ENABLE_WEIGHTING", "TRUE");
    properties.setProperty("AGGREGATOR",
        config.getBaselineAggregator().toUpperCase());
    properties.setProperty("NUM_TO_DROP_LOWEST", 
        Integer.toString(config.getExcludeMin()));
    properties.setProperty("NUM_TO_DROP_HIGHEST", 
        Integer.toString(config.getExcludeMax()));
    properties.setProperty("PERIOD", 
        Long.toString(prediction_interval * prediction_intervals));
    
    if (context.query().isTraceEnabled()) {
      context.queryContext().logTrace("EGADS Properties: " + properties.toString());
    }
    LOG.debug("[EGADS] Running baseline with properties: " + properties.toString() 
      + "  For index: " + prediction_idx);
     // TODO - parallelize on each time series.
    final List<TimeSeries> computed = Lists.newArrayList();
    TLongObjectIterator<OlympicScoringBaseline[]> it = join.iterator();
    while (it.hasNext()) {
      it.advance();
      OlympicScoringBaseline[] baselines = it.value();
      if (baselines == null) {
        LOG.warn("[EGADS] No baselines for: " + this);
        continue;
      }
      if (baselines[prediction_idx] == null) {
        LOG.warn("[EGADS] No baseline at prediction index [" + prediction_idx + "] for " + this);
        continue;
      }
      
      TimeSeries ts = baselines[prediction_idx].predict(properties, prediction_starts[prediction_idx]);
      if (ts != null) {
        LOG.debug("[EGADS] Baseline series: " + ts.id() + "  For index: " + prediction_idx);
        computed.add(ts);
      } else {
        LOG.warn("[EGADS] No time series returned from prediction call at index [" 
            + prediction_idx + "] for " + this);
      }
    }
    TimeStamp start = new SecondTimeStamp(prediction_starts[prediction_idx]);
    TimeStamp end = start.getCopy();
    end.add(modelDuration() == ChronoUnit.HOURS ? Duration.ofHours(1) : Duration.ofDays(1));

    if (cache != null && config.getMode() != ExecutionMode.CONFIG) {
      // need's a clone as we may modify the list when we add thresholds, etc.
      writeCache(new AnomalyPredictionResult(this, 
                                             data_source, 
                                             start, 
                                             end, 
                                             ds_interval,
                                             Lists.newArrayList(computed), 
                                             id_type),
                 prediction_idx);
    }
    predictions[prediction_idx] = new AnomalyPredictionResult(this, 
                                           data_source, 
                                           start, 
                                           end,
                                           ds_interval,
                                           computed, 
                                           id_type);
    countdown();
  }
  
  class BaselineQuery implements QuerySink {
    final int prediction_idx;
    final int period_idx;
    QueryContext sub_context;
    
    BaselineQuery(final int prediction_idx, final int period_idx) {
      this.prediction_idx = prediction_idx;
      this.period_idx = period_idx;
    }
    
    @Override
    public void onComplete() {
      if (failed.get()) {
        return;
      }
      
      if (period_idx + 1 < ((OlympicScoringConfig) config).getBaselineNumPeriods() && 
          baseline_queries[prediction_idx][period_idx + 1] != null) {
        // fire next
        baseline_queries[prediction_idx][period_idx + 1].sub_context.initialize(null)
          .addCallback(new SubQueryCB( 
              baseline_queries[prediction_idx][period_idx + 1].sub_context))
          .addErrback(new ErrorCB());
        context.tsdb().getStatsCollector().incrementCounter(
            "amomaly.EGADS.query.prediction.baseline.query.count", 
            "model", OlympicScoringFactory.TYPE,
            "mode", config.getMode().toString());
      } else {
        runBaseline(prediction_idx);
      }
      context.tsdb().getStatsCollector().incrementCounter(
          "amomaly.EGADS.query.prediction.baseline.query.success", 
          "model", OlympicScoringFactory.TYPE,
          "mode", config.getMode().toString());
    }

    @Override
    public void onNext(final QueryResult next) {
      updateState(State.RUNNING, prediction_idx, null);
      // TODO filter, for now assume one result
      try {
      for (final TimeSeries series : next.timeSeries()) {
        final long hash = series.id().buildHashCode();
        synchronized (join) {
          OlympicScoringBaseline[] baselines = join.get(hash);
          if (baselines == null) {
            baselines = new OlympicScoringBaseline[predictions.length];
            join.put(hash, baselines);
          }
          if (baselines[prediction_idx] == null) {
            baselines[prediction_idx] = new OlympicScoringBaseline(OlympicScoringNode.this, series.id());
          }
          baselines[prediction_idx].append(series, next);
        }
      }
      
      next.close();
      } catch (Throwable t) {
        LOG.error("WTF?", t);
      }
    }

    @Override
    public void onNext(final PartialTimeSeries next, 
                       final QuerySinkCallback callback) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void onError(final Throwable t) {
      if (failed.compareAndSet(false, true)) {
        LOG.error("OOOPS on sub query: " + period_idx + " " + t.getMessage());
        if (t instanceof Exception) {
          handleError((Exception) t, prediction_idx, true);
        } else {
          handleError(new RuntimeException(t), prediction_idx, true);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failure in baseline query after initial failure", t);
        }
      }
      context.tsdb().getStatsCollector().incrementCounter(
          "amomaly.EGADS.query.prediction.baseline.query.errors", 
          "model", OlympicScoringFactory.TYPE,
          "mode", config.getMode().toString());
    }
    
  }
    
  class SubQueryCB implements Callback<Void, Void> {
    final QueryContext context;
    
    SubQueryCB(final QueryContext context) {
      this.context = context;
    }
    
    @Override
    public Void call(final Void arg) throws Exception {
      context.fetchNext(null);
      return null;
    }
    
  }
  
  class ErrorCB implements Callback<Void, Exception> {

    @Override
    public Void call(final Exception e) throws Exception {
      if (failed.compareAndSet(false, true)) {
        onError(e);
      } else {
        LOG.warn("Failure launching baseline query after initial failure", e);
      }
      context.tsdb().getStatsCollector().incrementCounter(
          "amomaly.EGADS.query.prediction.baseline.query.errors", 
          "model", OlympicScoringFactory.TYPE,
          "mode", config.getMode().toString());
      return null;
    }
    
  }

  @Override
  protected void fetchTrainingData(final int prediction_index) {
    // see we should actually start by checking the state cache.
    if (!startPrediction(prediction_index)) {
      return;
    }
    
    if (baseline_queries == null) {
      synchronized (this) {
        if (baseline_queries == null) {
          baseline_queries = new BaselineQuery[predictions.length][];
        }
      }
    }
    
    final OlympicScoringConfig config = (OlympicScoringConfig) this.config;
    baseline_queries[prediction_index] = new BaselineQuery[config.getBaselineNumPeriods()];
    final TimeStamp start = new SecondTimeStamp(prediction_starts[prediction_index]);
    final TemporalAmount period = DateTime.parseDuration2(config.getBaselinePeriod());
    
    // advance to the oldest time first
    final TimeStamp end = context.query().endTime().getCopy();
    for (int i = 0; i < config.getBaselineNumPeriods(); i++) {
      start.subtract(period);
      end.subtract(period);
    }
    if (config.getMode() != ExecutionMode.CONFIG) {
      end.update(start);
      end.add(Duration.of(1, model_units));
    }
    
    // build the queries. If we have a funky query that is back-to-back, fire one
    // instead of multiple
    boolean consecutive = true;
    final long start_epoch = start.epoch(); 
    long last_epoch = end.epoch();
    for (int i = 0; i < config.getBaselineNumPeriods(); i++) {
      final BaselineQuery query = new BaselineQuery(prediction_index, i);
      baseline_queries[prediction_index][i] = query;
      query.sub_context = buildQuery((int) start.epoch(), 
                                     (int) end.epoch(), 
                                     context.queryContext(), 
                                     query);
      if (context.query().isTraceEnabled()) {
        context.queryContext().logTrace("Baseline query at [" + i + "] " + 
            JSON.serializeToString(query.sub_context.query()));
      }
      start.add(baseline_period);
      end.add(baseline_period);
      if (start.epoch() - last_epoch > 0) {
        consecutive = false;
      }
      last_epoch = end.epoch();
    }

    if (consecutive && (end.epoch() - start_epoch <= 86400)) {
      LOG.info("Switching to single query mode!");
      for (int i = 1; i < config.getBaselineNumPeriods(); i++) {
        baseline_queries[prediction_index][i] = null;
      }
      baseline_queries[prediction_index][0].sub_context = buildQuery(
          (int) start_epoch, 
          (int) end.epoch(), 
          context.queryContext(), 
          baseline_queries[prediction_index][0]);
      if (context.query().isTraceEnabled()) {
        context.queryContext().logTrace("Rebuilding consecutive baseline query at [0] " + 
        JSON.serializeToString(baseline_queries[prediction_index][0].sub_context.query()));
      }
    }
    
    baseline_queries[prediction_index][0].sub_context.initialize(null)
      .addCallback(new SubQueryCB(baseline_queries[prediction_index][0].sub_context))
      .addErrback(new ErrorCB());
    context.tsdb().getStatsCollector().incrementCounter(
        "amomaly.EGADS.query.prediction.baseline.query.count", 
        "model", OlympicScoringFactory.TYPE,
        "mode", config.getMode().toString());
    
    if (config.getMode() == ExecutionMode.PREDICT) {
      // return here.
      final AnomalyPredictionState state = cache.getState(cache_keys[prediction_index]);
      QueryExecutionException e = new QueryExecutionException("Successfully "
          + "started prediction start [" + prediction_starts[prediction_index] + "] and key " 
          + Arrays.toString(cache_keys[prediction_index]) + " State: " 
          + JSON.serializeToString(state), 423);
      sendUpstream(e);
    }
  }
  
  QueryContext buildQuery(final int start, 
                          final int end, 
                          final QueryContext context, 
                          final QuerySink sink) {
    final SemanticQuery.Builder builder = SemanticQuery.newBuilder()
        // TODO - PADDING compute the padding
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(start - 300))
        .setEnd(Integer.toString(end));
    // TODO - figure out why the config.getBaselineQuery() is missing the query 
    // filters. Hmm.
    if (context.query().getFilters() != null) {
      builder.setFilters(context.query().getFilters());
    }
    
    for (final QueryNodeConfig config : ((OlympicScoringConfig) config).getBaselineQuery().getExecutionGraph()) {
      if (config instanceof DownsampleConfig) {
        builder.addExecutionGraphNode(((DownsampleConfig.Builder)
            config.toBuilder())
            .setInterval(ds_interval)
            .setSources(config.getSources())
            .build());
      } else {
        builder.addExecutionGraphNode(config);
      }
    }
    
    LOG.info("  BASELINE Q: " + JSON.serializeToString(builder.build()));
    return SemanticQueryContext.newBuilder()
        .setTSDB(context.tsdb())
        .setLocalSinks((List<QuerySink>) Lists.newArrayList(sink))
        .setQuery(builder.build())
        .setStats(context.stats())
        .setAuthState(context.authState())
        .setHeaders(context.headers())
        .build();
  }
  
  ChronoUnit modelDuration() {
    return model_units;
  }
  
  long predictionIntervals() {
    return prediction_intervals;
  }
  
  long predictionInterval() {
    return prediction_interval;
  }
  
}
