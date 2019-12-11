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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.QuerySinkCallback;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.query.anomaly.AnomalyPredictionState;
import net.opentsdb.query.anomaly.AnomalyPredictionState.State;
import net.opentsdb.query.anomaly.PredictionCache;
import net.opentsdb.query.anomaly.egads.EgadsPredictionResult;
import net.opentsdb.query.anomaly.egads.EgadsPredictionTimeSeries;
import net.opentsdb.query.anomaly.egads.EgadsResult;
import net.opentsdb.query.anomaly.egads.EgadsThresholdEvaluator;
import net.opentsdb.query.anomaly.egads.EgadsThresholdTimeSeries;
import net.opentsdb.query.anomaly.AnomalyConfig.ExecutionMode;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.downsample.DownsampleFactory;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * A node that runs the OlympicScoring algorithm from EGADs. This node will
 * check the cache and on miss, trigger the baseline prediction execution on
 * node initialization. Once the prediction and current results (sent to 
 * onNext()) are in, then the current values are evaluated against the prediction
 * and results sent upstream.
 * 
 * TODO - Check for null or empty currents and baselines.
 * TODO - SEt proper state on baseline failures.
 * TODO - See that we stay open long enough to finish building
 * in predict mode, just return.
 *
 * @since 3.0
 */
public class OlympicScoringNode extends AbstractQueryNode {
  private static final Logger LOG = LoggerFactory.getLogger(
      OlympicScoringNode.class);
  
  protected final OlympicScoringConfig config;
  protected final PredictionCache cache;
  protected final AtomicInteger latch;
  protected final int jitter;
  protected final AtomicBoolean failed;
  protected final AtomicBoolean building_prediction;
  protected final AtomicBoolean cache_error;
  protected final boolean cache_hits[];
  protected volatile BaselineQuery[][] baseline_queries;
  protected final TemporalAmount baseline_period;
  protected Properties properties;
  protected final long[] prediction_starts;
  protected volatile QueryResult[] predictions;
  protected volatile QueryResult current;
  protected final TLongObjectMap<OlympicScoringBaseline> join = 
      new TLongObjectHashMap<OlympicScoringBaseline>();
  protected final ChronoUnit model_units;
  protected final byte[][] cache_keys;
  protected final long prediction_intervals;
  protected final long prediction_interval;
  protected final int threshold_dps;
  protected String ds_interval;
  protected volatile String data_source;
  
  public OlympicScoringNode(final QueryNodeFactory factory,
                            final QueryPipelineContext context,
                            final OlympicScoringConfig config) {
    super(factory, context);
    this.config = config;
    latch = new AtomicInteger(2);
    failed = new AtomicBoolean();
    cache_error = new AtomicBoolean();
    building_prediction = new AtomicBoolean();
    baseline_period = DateTime.parseDuration2(config.getBaselinePeriod());
    
    // TODO - find the proper ds in graph in order
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
    final long query_time_span = context.query().endTime().msEpoch() - 
        context.query().startTime().msEpoch();
    if (ds.getInterval().equalsIgnoreCase("AUTO")) {
      final QueryNodeFactory dsf = context.tsdb().getRegistry()
          .getQueryNodeFactory(DownsampleFactory.TYPE);
      if (dsf == null) {
        LOG.error("Unable to find a factory for the downsampler.");
      }
      if (((DownsampleFactory) dsf).intervals() == null) {
        LOG.error("No auto intervals for the downsampler.");
      }
      ds_interval = DownsampleFactory.getAutoInterval(query_time_span, 
          ((DownsampleFactory) factory).intervals(), null);
    } else {
      ds_interval = ds.getInterval();
    }
    cache = ((OlympicScoringFactory) factory).cache();
    prediction_interval = DateTime.parseDuration(ds_interval) / 1000;
    
    // set timings
    switch (config.getMode()) {
    case CONFIG:
      jitter = 0;
      model_units = null;
      prediction_starts = new long[] { context.query().startTime().epoch() };
      prediction_intervals = query_time_span / (prediction_interval * 1000);
      threshold_dps = (int) prediction_intervals;
      cache_keys = null;
      cache_hits = new boolean[0];
      predictions = new QueryResult[1];
      break;
    case EVALUATE:
    case PREDICT:
      // TODO - for now, we need the query timespan to be 1h or 1day at the most as
      // we'll build the model off the start time of the query.
      long baseline_span = DateTime.parseDuration(config.getBaselinePeriod()) / 1000;
      if (baseline_span < 86400) {
        model_units = ChronoUnit.HOURS;
      } else {
        model_units = ChronoUnit.DAYS;
      }
      
      jitter = ((OlympicScoringFactory) factory).jitter(context.query(), model_units);
      TemporalAmount jitter_duration = Duration.ofSeconds(jitter);
      
      final TimeStamp start = context.query().startTime().getCopy();
      final ChronoUnit duration = modelDuration();
      start.snapToPreviousInterval(1, duration);
      start.add(jitter_duration);
      // now snap to ds interval
      start.snapToPreviousInterval(DateTime.getDurationInterval(ds_interval), 
          DateTime.unitsToChronoUnit(DateTime.getDurationUnits(ds_interval)));
      if (start.compare(Op.GT, context.query().startTime())) {
        start.subtract(model_units == ChronoUnit.HOURS ? 
            Duration.ofHours(1) : Duration.ofDays(1));
      }
      
      prediction_intervals = (model_units == 
          ChronoUnit.HOURS ? 3600 : 86400) * 1000 / (prediction_interval * 1000);
      
      // we need to see how many predictions we need for this eval as the query time
      // range may span the boundary.
      int num_predictions = 0;
      long ts = start.epoch();
      while (ts < context.query().endTime().epoch()) {
        num_predictions++;
        ts += (model_units == ChronoUnit.HOURS ? 3600 : 86400);
      }
      prediction_starts = new long[num_predictions];
      predictions = new QueryResult[num_predictions];
      if (num_predictions > 1) {
        latch.set(num_predictions + 1);
      }
      cache_keys = new byte[num_predictions][];
      ts = start.epoch();
      int i = 0;
      while (ts < context.query().endTime().epoch()) {
        prediction_starts[i] = ts;
        cache_keys[i++] = ((OlympicScoringFactory) factory)
          .generateCacheKey(context.query(), (int) ts);
        ts += (model_units == ChronoUnit.HOURS ? 3600 : 86400);
      }
      threshold_dps = (int) (prediction_intervals * predictions.length);
      cache_hits = new boolean[num_predictions];
      
      if (context.query().isTraceEnabled()) {
        context.queryContext().logTrace("EGADs evaluation jitter: " + jitter);
        context.queryContext().logTrace("EGADs Model duration: 1 " + model_units);
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("EGADs evaluation jitter: " + jitter);
        LOG.trace("EGADs Model duration: 1 " + model_units);
      }
      break;
    default:
      throw new IllegalStateException("Unhandled config mode: " + config.getMode());
    }
  }
  
  @Override
  public Deferred<Void> initialize(final Span span) {
    final class InitCB implements Callback<Void, Void> {
      @Override
      public Void call(final Void arg) throws Exception {
        // trigger the cache lookup.
        if (cache != null && config.getMode() != ExecutionMode.CONFIG) {
          for (int i = 0; i < predictions.length; i++) {
            cache.fetch(pipelineContext(), cache_keys[i], null)
              .addCallback(new CacheCB(i))
              .addErrback(new CacheErrCB(i));
          }
        } else {
          for (int i = 0; i < predictions.length; i++) {
            fetchBaselineData(i);
          }
        }
        return null;
      }
    }
    return super.initialize(span).addCallback(new InitCB());
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {
    // TODO fire out out what to do here. For now, we'll let the baseline keep
    // going.
  }

  @Override
  public void onNext(final QueryResult next) {
    if (data_source == null || !data_source.equals(next.dataSource())) {
      synchronized (this) {
        if (data_source == null || !data_source.equals(next.dataSource())) {
          data_source = next.dataSource();
        }
      }
    }
    current = next;
    countdown();
  }
  
  void run() {
    // Got baseline and current data, yay!
    TLongObjectMap<TimeSeries> map = new TLongObjectHashMap<TimeSeries>();
    
    for (final TimeSeries series : current.timeSeries()) {
      final long hash = series.id().buildHashCode();
      map.put(hash, series);
    }
    
    final EgadsResult result = new EgadsResult(
        this, current, config.getSerializeObserved());
    if (predictions.length > 1) {
      // join first then eval
      final TLongObjectMap<TimeSeries[]> series_arrays = 
          new TLongObjectHashMap<TimeSeries[]>();
      for (int i = 0; i < predictions.length; i++) {
        final int series_limit = predictions[i].timeSeries().size();
        for (int x = 0; x < series_limit; x++) {
          final TimeSeries series = predictions[i].timeSeries().get(x);
          final long hash = series.id().buildHashCode();
          TimeSeries[] array = series_arrays.get(hash);
          if (array == null) {
            array = new TimeSeries[predictions.length];
            series_arrays.put(hash, array);
          }
          array[i] = series;
        }
      }
      
      TLongObjectIterator<TimeSeries[]> iterator = series_arrays.iterator();
      while (iterator.hasNext()) {
        iterator.advance();
        TimeSeries series = null;
        for (int i = 0; i < predictions.length; i++) {
          if (iterator.value()[i] == null) {
            continue;
          }
          
          series = iterator.value()[i];
          break;
        }
        
        if (series == null) {
          LOG.warn("Whoops, null series in predictions?? Shouldn't happen!");
          continue;
        }
        
        final long hash = series.id().buildHashCode();
        TimeSeries cur = map.remove(hash);
        evaluate(cur, iterator.value(), result);
      }
    } else {
      final int series_limit = predictions[0].timeSeries().size();
      for (int i = 0; i < series_limit; i++) {
        final TimeSeries series = predictions[0].timeSeries().get(i);
        final long hash = series.id().buildHashCode();
        TimeSeries cur = map.remove(hash);
        evaluate(cur, series, result);
      }
    }
    map = null; // release to GC
    sendUpstream(result);
  }
  
  void evaluate(final TimeSeries cur, final TimeSeries prediction, final EgadsResult result) {
    evaluate(cur, new TimeSeries[] { prediction }, result);
  }
  
  void evaluate(final TimeSeries cur, final TimeSeries[] preds, final EgadsResult result) {
    if (cur != null) {
      final EgadsThresholdEvaluator eval = new EgadsThresholdEvaluator(
          config,
          threshold_dps,
          cur,
          current,
          preds,
          predictions);
      eval.evaluate();
      
      final EgadsPredictionTimeSeries pred_ts = new EgadsPredictionTimeSeries(
          preds, predictions, "prediction", OlympicScoringFactory.TYPE);
      if (eval.alerts() != null && !eval.alerts().isEmpty()) {
        pred_ts.addAlerts(eval.alerts());
      }
      
      result.addPredictionsAndThresholds(pred_ts, predictions);
      
      if (config.getSerializeDeltas()) {
        final TimeSeries ts = new EgadsThresholdTimeSeries(
            cur.id(), 
            "delta", 
            prediction_starts[0], 
            eval.deltas(), 
            eval.index(),
            OlympicScoringFactory.TYPE);
        result.addPredictionsAndThresholds(ts, predictions);
      }
      
      if (config.getSerializeThresholds()) {
        if (config.getUpperThresholdBad() != 0) {
          final TimeSeries ts = new EgadsThresholdTimeSeries(
              cur.id(), 
              "upperBad", 
              prediction_starts[0], 
              eval.upperBadThresholds(), 
              eval.index(),
              OlympicScoringFactory.TYPE);
          result.addPredictionsAndThresholds(ts, predictions);
        }
        if (config.getUpperThresholdWarn() != 0) {
          final TimeSeries ts = new EgadsThresholdTimeSeries(
              cur.id(), 
              "upperWarn", 
              prediction_starts[0], 
              eval.upperWarnThresholds(), 
              eval.index(),
              OlympicScoringFactory.TYPE);
          result.addPredictionsAndThresholds(ts, predictions);
        }
        if (config.getLowerThresholdBad() != 0) {
          final TimeSeries ts = new EgadsThresholdTimeSeries(
              cur.id(), 
              "lowerBad", 
              prediction_starts[0], 
              eval.lowerBadThresholds(), 
              eval.index(),
              OlympicScoringFactory.TYPE);
          result.addPredictionsAndThresholds(ts, predictions);
        }
        if (config.getLowerThresholdWarn() != 0) {
          final TimeSeries ts = new EgadsThresholdTimeSeries(
              cur.id(), 
              "lowerWarn", 
              prediction_starts[0], 
              eval.lowerWarnThresholds(), 
              eval.index(),
              OlympicScoringFactory.TYPE);
          result.addPredictionsAndThresholds(ts, predictions);
        }
      }
    }
  }
  
  void runBaseline(final int prediction_idx) {
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
     // TODO - parallelize
    final List<TimeSeries> computed = Lists.newArrayList();
    TLongObjectIterator<OlympicScoringBaseline> it = join.iterator();
    while (it.hasNext()) {
      it.advance();
      TimeSeries ts = it.value().predict(properties, prediction_starts[prediction_idx]);
      if (ts != null) {
        computed.add(ts);
      }
    }
    TimeStamp start = new SecondTimeStamp(prediction_starts[prediction_idx]);
    TimeStamp end = start.getCopy();
    end.add(modelDuration() == ChronoUnit.HOURS ? Duration.ofHours(1) : Duration.ofDays(1));

    if (cache != null && config.getMode() != ExecutionMode.CONFIG) {
      // need's a clone as we may modify the list when we add thresholds, etc.
      writeCache(new EgadsPredictionResult(this, 
                                          data_source, 
                                          start, 
                                          end, 
                                          Lists.newArrayList(computed), 
                                          id_type),
                 prediction_idx);
    }
    predictions[prediction_idx] = new EgadsPredictionResult(this, 
                                           data_source, 
                                           start, 
                                           end, 
                                           computed, 
                                           id_type);
    countdown();
  }
  
  class CacheCB implements Callback<Void, QueryResult> {
    final int index;
    CacheCB(final int index) {
      this.index = index;
    }

    @Override
    public Void call(final QueryResult result) throws Exception {
      // TODO
      // If result == null ? fire baseline, else store and use to match.
      if (result != null) {
        context.queryContext().logDebug("Prediction cache hit for query.");
        predictions[index] = result;
        cache_hits[index] = true;
        countdown();
      } else {
        context.queryContext().logDebug("Prediction cache miss for query.");
        fetchBaselineData(index);
      }
      
      return null;
    }
    
  }
  
  class CacheErrCB implements Callback<Void, Exception> {
    final int index;
    CacheErrCB(final int index) {
      this.index = index;
    }
    
    @Override
    public Void call(final Exception e) throws Exception {
      LOG.warn("Cache exception", e);
      fetchBaselineData(index);
      return null;
    }
    
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
      
      if (period_idx + 1 < baseline_queries.length) {
        // fire next
        baseline_queries[prediction_idx][period_idx + 1].sub_context.initialize(null)
          .addCallback(new SubQueryCB( 
              baseline_queries[prediction_idx][period_idx + 1].sub_context))
          .addErrback(new ErrorCB());
      } else {
        runBaseline(prediction_idx);
      }
    }

    @Override
    public void onNext(final QueryResult next) {
      updateState(State.RUNNING, prediction_idx, null);
      // TODO filter, for now assume one result
      
      if (data_source == null) {
        synchronized (OlympicScoringNode.this) {
          if (data_source == null) {
            data_source = next.dataSource();
          }
        }
      }
      
      LOG.info("BASELINE [" + period_idx + "] got " + next.timeSeries().size() + " results!");
      for (final TimeSeries series : next.timeSeries()) {
        final long hash = series.id().buildHashCode();
        OlympicScoringBaseline baseline = join.get(hash);
        if (baseline == null) {
          baseline = new OlympicScoringBaseline(OlympicScoringNode.this, series.id());
          join.put(hash, baseline);
        }
        baseline.append(series, next);
      }
      
      next.close();
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
      return null;
    }
    
  }

  void fetchBaselineData(final int prediction_index) {
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
    
    // fire!
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
    }

    baseline_queries[prediction_index][0].sub_context.initialize(null)
      .addCallback(new SubQueryCB(baseline_queries[prediction_index][0].sub_context))
      .addErrback(new ErrorCB());
    
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
  
  void countdown() {
    int ct = latch.decrementAndGet();
    if (ct == 0) {
      run();
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
    
    for (final QueryNodeConfig config : config.getBaselineQuery().getExecutionGraph()) {
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
  
//  long predictionStart() {
//    return prediction_start;
//  }
  
  long predictionIntervals() {
    return prediction_intervals;
  }
  
  long predictionInterval() {
    return prediction_interval;
  }
  
  void writeCache(final QueryResult result, final int prediction_index) {
    if (cache_hits[prediction_index] || cache == null) {
      return;
    }
    
    context.tsdb().getQueryThreadPool().submit(new Runnable() {
      public void run() {
        LOG.info("********** WRITING CACHE!!  Finally in the runnable.");
        class CacheErrorCB implements Callback<Object, Exception> {
          @Override
          public Object call(final Exception e) throws Exception {
            LOG.warn("Failed to cache EGADs prediction", e);
            clearState(prediction_index);
            return null;
          }
        }
        
        class SuccessCB implements Callback<Object, Void> {
          @Override
          public Object call(final Void ignored) throws Exception {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Successfully cached EGADs prediction at " 
                  + Arrays.toString(cache_keys[prediction_index]));
            }
            
            updateState(State.COMPLETE, prediction_index, null);
            return null;
          }
        }
        
        final long expiration;
        if (model_units == ChronoUnit.HOURS) {
          expiration = 3600 * 2 * 1000;
        } else {
          expiration = 86400 * 2 * 1000;
        }
        
        cache.cache(cache_keys[prediction_index], expiration, result, null)
          .addCallback(new SuccessCB())
          .addErrback(new CacheErrorCB());
      }
    });
  }

  void handleError(final Exception e, 
                   final int prediction_index, 
                   final boolean update_state) {
    if (!failed.compareAndSet(false, true)) {
      sendUpstream(e);
      if (update_state) {
        updateState(State.ERROR, prediction_index, e);
      }
    } else {
      LOG.warn("Exception after failure", e);
    }
  }
  
  boolean startPrediction(final int prediction_index) {
    if (cache == null || config.getMode() == ExecutionMode.CONFIG) {
      return true;
    }
    
    AnomalyPredictionState state = cache.getState(cache_keys[prediction_index]);
    if (state == null) {
      state = new AnomalyPredictionState();
      state.host = ((OlympicScoringFactory) factory).hostName();
      state.hash = context.query().buildHashCode().asLong();
      state.startTime = state.lastUpdateTime = DateTime.currentTimeMillis() / 1000;
      state.state = State.RUNNING;
      
      cache.setState(cache_keys[prediction_index], state, 300_000); // TODO config
      
      AnomalyPredictionState present = cache.getState(cache_keys[prediction_index]);
      if (!state.equals(present)) {
        if (present == null) {
          handleError(new QueryExecutionException("Failed to set the prediction state."
              + "Retry later for prediction start [" + prediction_starts[prediction_index] + "] and key " 
              + Arrays.toString(cache_keys[prediction_index]) + " State: " 
              + JSON.serializeToString(state), 424),
              prediction_index,
              false);
        } else if (present.state == State.COMPLETE) {
          // TODO - handle infinite loops here, we need to inc a volatile and
          // fail if we hit too many retries.
          for (int i = 0; i < predictions.length; i++) {
            cache.fetch(pipelineContext(), cache_keys[prediction_index], null)
              .addCallback(new CacheCB(i))
              .addErrback(new CacheErrCB(i));
          }
          
        } else if (present.state == State.RUNNING) {
          handleError(new QueryExecutionException("Lost a race building "
              + "prediction for prediction start [" + prediction_starts[prediction_index] + "] and key " 
              + Arrays.toString(cache_keys[prediction_index]) + " State: " 
              + JSON.serializeToString(present), 423),
              prediction_index,
              false);
        } else {
          handleError(new QueryExecutionException("Unexpected exception or error state."
              + "Retry later for prediction start [" + prediction_starts[prediction_index] + "] and key " 
              + Arrays.toString(cache_keys[prediction_index]) + " State: " 
              + JSON.serializeToString(state), 424),
              prediction_index,
              false);
        }
        return false;
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Successfully wrote prediction state.");
        }
        return true;
      }
    } else if (state.state == State.COMPLETE) {
      // TODO - handle infinite loops here, we need to inc a volatile and
      // fail if we hit too many retries.
      for (int i = 0; i < predictions.length; i++) {
        cache.fetch(pipelineContext(), cache_keys[prediction_index], null)
          .addCallback(new CacheCB(i))
          .addErrback(new CacheErrCB(i));
      }
      
    } else if (state.state == State.RUNNING) {
      handleError(new QueryExecutionException("Lost a race building "
          + "prediction for prediction start [" + prediction_starts[prediction_index] + "] and key " 
          + Arrays.toString(cache_keys[prediction_index]) + " State: " 
          + JSON.serializeToString(state), 423),
          prediction_index,
          false);
    } else {
      handleError(new QueryExecutionException("Unexpected exception or error state."
          + "Retry later for prediction start [" + prediction_starts[prediction_index] + "] and key " 
          + Arrays.toString(cache_keys[prediction_index]) + " State: " 
          + JSON.serializeToString(state), 424),
          prediction_index,
          false);
    }
    return false;
  }

  void updateState(final State new_state, 
                   final int prediction_index, 
                   final Exception e) {
    if (cache == null || config.getMode() == ExecutionMode.CONFIG) {
      return;
    }
    AnomalyPredictionState state = cache.getState(cache_keys[prediction_index]);
    if (state == null) {
      LOG.error("No state found. Maybe we need to stop?");
      return;
    } else if (state.state == State.COMPLETE && 
        (DateTime.currentTimeMillis() / 1000) - state.lastUpdateTime < 120) {
      LOG.warn("Already complete?!?!");
      return;
    }
    
    state.state = new_state;
    state.lastUpdateTime = DateTime.currentTimeMillis() / 1000;
    state.exception = e == null ? "" : e.getMessage();
    cache.setState(cache_keys[prediction_index], state, 300_000); // TODO config
    
    AnomalyPredictionState cas = cache.getState(cache_keys[prediction_index]);
    int cnt = 0;
    while (!cas.equals(state) && cnt++ < 5) { 
      LOG.error("Whoops? Failed cas?: " + cas);
      try {
        Thread.sleep(50);
      } catch (InterruptedException e1) {
        LOG.error("Unexpected interruption", e1);
        return;
      }
      cache.setState(cache_keys[prediction_index], state, 300_000); // TODO config
      cas = cache.getState(cache_keys[prediction_index]);
    }
    if (cas.equals(state)) {
      LOG.info("Set EGADs state to " + new_state + " for " 
          + Arrays.toString(cache_keys[prediction_index]));
    } else {
      LOG.warn("Failed to update the state!!!!");
    }
  }
  
  void clearState(final int prediction_index) {
    if (cache == null || config.getMode() == ExecutionMode.CONFIG) {
      return;
    }
    cache.delete(cache_keys[prediction_index]);
  }
}
