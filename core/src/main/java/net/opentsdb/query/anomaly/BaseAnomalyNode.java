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
package net.opentsdb.query.anomaly;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.types.numeric.aggregators.DefaultArrayAggregatorConfig;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorConfig;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorFactory;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeries;
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
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.QuerySinkCallback;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.query.anomaly.AnomalyConfig.ExecutionMode;
import net.opentsdb.query.anomaly.AnomalyPredictionState.State;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.downsample.DownsampleFactory;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

public class BaseAnomalyNode extends AbstractQueryNode {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAnomalyNode.class);

  protected TimeSeriesDataSourceConfig dataSourceConfig;
  protected final BaseAnomalyConfig config;
  protected PredictionCache cache;
  protected AtomicInteger latch;
  protected AtomicInteger prediction_attempts;
  protected int jitter;
  protected AtomicBoolean failed;
  protected AtomicBoolean building_prediction;
  protected AtomicBoolean cache_error;
  protected boolean cache_hits[];
  protected long[] prediction_starts;
  protected ChronoUnit model_units;
  protected byte[][] cache_keys;
  protected long prediction_intervals;
  protected long prediction_interval;
  protected int threshold_dps;
  protected long training_span;
  protected QueryResultId data_source;
  protected String ds_interval;
  protected volatile QueryResult[] predictions;
  protected volatile QueryResult current;
  protected volatile NumericArrayAggregatorConfig aggregatorConfig;
  private volatile NumericArrayAggregatorFactory aggregatorFactory;
  protected TrainingQuery training_query;
  protected volatile QueryResult training_data;

  public BaseAnomalyNode(final QueryNodeFactory factory, 
                         final QueryPipelineContext context,
                         final BaseAnomalyConfig config) {
    super(factory, context);
    this.config = config;
  }

  @Override
  public Deferred<Void> initialize(final Span span) {
    LOG.info("*********** INITIALIZING");
    latch = new AtomicInteger(2);
    prediction_attempts = new AtomicInteger();
    failed = new AtomicBoolean();
    cache_error = new AtomicBoolean();
    building_prediction = new AtomicBoolean();

    aggregatorFactory = context.tsdb().getRegistry().getPlugin(
            NumericArrayAggregatorFactory.class, "MAX");

    // TODO - find the proper ds in graph in order
    DownsampleConfig ds = null;
    for (final QueryNodeConfig node : context.query().getExecutionGraph()) {
      if (node instanceof DownsampleConfig) {
        ds = (DownsampleConfig) node;
        break;
      }
    }
    if (ds == null) {
      throw new IllegalStateException("Downsample can't be null for anomaly " +
              "processing at this time.");
    }

    dataSourceConfig = context.commonSourceConfig(this);
    final long query_time_span = dataSourceConfig.endTimestamp().msEpoch() -
            dataSourceConfig.startTimestamp().msEpoch();
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
          ((DownsampleFactory) dsf).intervals(), null);
    } else {
      ds_interval = ds.getInterval();
    }
    
    if (!Strings.isNullOrEmpty(config.getTrainingInterval())) {
      training_span = DateTime.parseDuration(config.getTrainingInterval()) / 1000;
    } else {
      training_span = (query_time_span / 1000) * 3;
    }
    cache = ((BaseAnomalyFactory) factory).cache();
    prediction_interval = DateTime.parseDuration(ds_interval) / 1000;
    
    switch (config.getMode()) {
    case CONFIG:
      jitter = 0;
      model_units = null;
      prediction_starts = new long[] { dataSourceConfig.startTimestamp().epoch() };
      prediction_intervals = (query_time_span / 1000) / prediction_interval;
      threshold_dps = (int) prediction_intervals;
      cache_keys = null;
      cache_hits = new boolean[0];
      predictions = new QueryResult[1];
      break;
    case EVALUATE:
    case PREDICT:
      // TODO - for now, we need the query timespan to be 1h or 1day at the most as
      // we'll build the model off the start time of the query.
      if (training_span < 86400) {
        model_units = ChronoUnit.HOURS;
      } else {
        model_units = ChronoUnit.DAYS;
      }
      jitter = ((BaseAnomalyFactory) factory).jitter(context.query(), model_units);
      TemporalAmount jitter_duration = Duration.ofSeconds(jitter);
      
      final TimeStamp start = dataSourceConfig.startTimestamp().getCopy();
      final ChronoUnit duration = model_units;
      start.snapToPreviousInterval(1, duration);
      start.add(jitter_duration);
      // now snap to ds interval
      start.snapToPreviousInterval(DateTime.getDurationInterval(ds_interval), 
          DateTime.unitsToChronoUnit(DateTime.getDurationUnits(ds_interval)));
      if (start.compare(Op.GT, dataSourceConfig.startTimestamp())) {
        start.subtract(model_units == ChronoUnit.HOURS ? 
            Duration.ofHours(1) : Duration.ofDays(1));
      }
      
      prediction_intervals = (model_units == 
          ChronoUnit.HOURS ? 3600 : 86400) * 1000 / (prediction_interval * 1000);
      
      // we need to see how many predictions we need for this eval as the query time
      // range may span the boundary.
      int num_predictions = 0;
      long ts = start.epoch();
      while (ts < dataSourceConfig.endTimestamp().epoch()) {
        num_predictions++;
        ts += (model_units == ChronoUnit.HOURS ? 3600 : 86400);
      }
      prediction_starts = new long[num_predictions];
      predictions = new QueryResult[num_predictions];
      if (num_predictions > 1) {
        latch.set(num_predictions + 1);
      }

      prediction_attempts.set(num_predictions);
      cache_keys = new byte[num_predictions][];
      ts = start.epoch();
      int i = 0;
      while (ts < dataSourceConfig.endTimestamp().epoch()) {
        prediction_starts[i] = ts;
        cache_keys[i++] = ((BaseAnomalyFactory) factory)
          .generateCacheKey(context.query(), (int) ts);
        ts += (model_units == ChronoUnit.HOURS ? 3600 : 86400);
      }
      threshold_dps = (int) (prediction_intervals * predictions.length);
      cache_hits = new boolean[num_predictions];
      
      if (context.query().isTraceEnabled()) {
        context.queryContext().logTrace("Prophet evaluation jitter: " + jitter);
        context.queryContext().logTrace("Prophet Model duration: 1 " + model_units);
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Prophet evaluation jitter: " + jitter + "s");
        LOG.trace("Prophet Model duration: 1 " + model_units);
      }
      break;
    default:
      throw new IllegalStateException("Unhandled config mode: " + config.getMode());
    }
    data_source = (QueryResultId) config.resultIds().get(0);
    context.tsdb().getStatsCollector().incrementCounter("amomaly.query.count", 
        "model", factory.id(),
        "mode", config.getMode().toString());
    
    if (model_units == ChronoUnit.HOURS) {
      config.setModelInterval('h');
    } else {
      config.setModelInterval('d');
    }
    
    final class InitCB implements Callback<Void, Void> {
      @Override
      public Void call(final Void arg) throws Exception {
        LOG.info("********** INIT CB");
        // trigger the cache lookup.
        if (cache != null && config.getMode() != ExecutionMode.CONFIG) {
          for (int i = 0; i < predictions.length; i++) {
            cache.fetch(pipelineContext(), cache_keys[i], null)
              .addCallback(new CacheCB(i))
              .addErrback(new CacheErrCB(i));
          }
          LOG.info("******* Sending out to cache...");
        } else {
          LOG.info("****** Fetching training data!");
          for (int i = 0; i < predictions.length; i++) {
            fetchTrainingData(i);
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
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onNext(final QueryResult next) {
    synchronized (this) {
      current = next;
      // TODO - we better have a time spec here or something is very wrong.
      long intervals = next.timeSpecification().interval().get(ChronoUnit.SECONDS);
      intervals = (next.timeSpecification().end().epoch() -
      next.timeSpecification().start().epoch()) / intervals;
      aggregatorConfig = DefaultArrayAggregatorConfig.newBuilder()
              .setArraySize((int) intervals)
              .build();
    }
    countdown();
  }

  public class CacheCB implements Callback<Void, QueryResult> {
    final int index;
    public CacheCB(final int index) {
      this.index = index;
    }

    @Override
    public Void call(final QueryResult result) throws Exception {
      // TODO
      // If result == null ? fire training, else store and use to match.
      if (result != null) {
        context.queryContext().logDebug("Prediction cache hit for query.");
        predictions[index] = result;
        cache_hits[index] = true;
        context.tsdb().getStatsCollector().incrementCounter(
            "amomaly.query.prediction.cache.hit", 
            "model", factory.id(),
            "mode", config.getMode().toString());
        LOG.debug("Cache hit for index: " + index);
        countdown();
      } else {
        context.queryContext().logDebug("Prediction cache miss for query.");
        context.tsdb().getStatsCollector().incrementCounter(
            "amomaly.query.prediction.cache.miss", 
            "model", factory.id(),
            "mode", config.getMode().toString());
        LOG.debug("Cache miss for index: " + index + ". Fetching baseline.");
        fetchTrainingData(index);
      }
      
      return null;
    }

  }
  
  public class CacheErrCB implements Callback<Void, Exception> {
    final int index;
    public CacheErrCB(final int index) {
      this.index = index;
    }
    
    @Override
    public Void call(final Exception e) throws Exception {
      LOG.warn("Cache exception", e);
      context.tsdb().getStatsCollector().incrementCounter(
          "amomaly.query.prediction.cache.errors", 
          "model", factory.id(),
          "mode", config.getMode().toString());
      fetchTrainingData(index);
      return null;
    }
    
  }
  
  class TrainingQuery implements QuerySink {
    final int prediction_idx;
    QueryContext sub_context;
    
    TrainingQuery(final int prediction_idx) {
      this.prediction_idx = prediction_idx;
    }
    
    @Override
    public void onComplete() {
      LOG.info("***** TRAINING DATA DONE and IN!");
      if (failed.get()) {
        return;
      }
      
      context.tsdb().getStatsCollector().incrementCounter(
          "amomaly.query.prediction.baseline.query.success", 
          "model", factory.id(),
          "mode", config.getMode().toString());
    }

    @Override
    public void onNext(final QueryResult next) {
      for (int i = 0; i < predictions.length; i++) {
        updateState(State.RUNNING, i, null);
      }
      // TODO filter, for now assume one result
      training_data = next;
      //ProphetResult result = new ProphetResult(Prophet.this, training_data);
//      result.predict().addCallback(new Callback<Void, Void>() {
      predictAndSet(next, prediction_idx).addCallback(new Callback<Void, Void>() {

        @Override
        public Void call(Void arg) throws Exception {
          LOG.info("******* TRAINING COMPLETE!");
          // TODO - where should this go? For now we dump in 0? Race!
          countdown();
//          switch (config.getMode()) {
//          case CONFIG:
//          case EVALUATE:
//            run();
//            break;
//          case PREDICT:
//            sendUpstream(result);
//            break;
//          }
          next.close();
          return null;
        }
        
      });
      LOG.info("****** TRAINING data came in!");
    }

    @Override
    public void onNext(final PartialTimeSeries next, 
                       final QuerySinkCallback callback) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void onError(final Throwable t) {
      if (failed.compareAndSet(false, true)) {
        LOG.error("OOOPS on training query: " + t.getMessage());
        for (int i = 0; i < predictions.length; i++) {
          if (t instanceof Exception) {
            handleError((Exception) t, i, true);
          } else {
            handleError(new RuntimeException(t), i, true);
          }
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failure in baseline query after initial failure", t);
        }
      }
      context.tsdb().getStatsCollector().incrementCounter(
          "amomaly.query.prediction.baseline.query.errors", 
          "model", factory.id(),
          "mode", config.getMode().toString());
    }
    
  }
    
  class TrainingQueryCB implements Callback<Void, Void> {

    @Override
    public Void call(final Void arg) throws Exception {
      training_query.sub_context.fetchNext(null);
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
          "amomaly.query.prediction.baseline.query.errors", 
          "model", factory.id(),
          "mode", config.getMode().toString());
      return null;
    }
    
  }

  protected void fetchTrainingData(final int prediction_index) {
    // see we should actually start by checking the state cache.
    try {
      if (!startPrediction(prediction_index)) {
        LOG.debug("****** Told not to start prediction for: " + prediction_index);
        return;
      }
      
      synchronized (this) {
        if (training_query != null) {
          // another thread is starting the training query.
          return;
        }
        training_query = new TrainingQuery(prediction_index);
      }
      
      SemanticQuery.Builder builder = ((SemanticQuery) context.query()).toBuilder()
          .setMode(QueryMode.SINGLE)
          .setStart(Long.toString(prediction_starts[0] - training_span))
          .setEnd(Long.toString(prediction_starts[prediction_starts.length - 1]));
      Iterator<QueryNodeConfig> it = builder.executionGraph().iterator();
      while (it.hasNext()) {
        QueryNodeConfig config = it.next();
        if (config instanceof BaseAnomalyConfig) {
          LOG.debug("Removed : " + config);
          it.remove();
        }
      }
      
      // TODO - figure out why the config.getBaselineQuery() is missing the query 
      // filters. Hmm.
      if (context.query().getFilters() != null) {
        builder.setFilters(context.query().getFilters());
      }
      
//      for (final QueryNodeConfig config : context.query().getExecutionGraph()) {
//        if (config instanceof DownsampleConfig) {
//          builder.addExecutionGraphNode(((DownsampleConfig.Builder)
//              config.toBuilder())
//              .setInterval(ds_interval)
//              .setSources(config.getSources())
//              .build());
//        } else {
//          builder.addExecutionGraphNode(config);
//        }
//      }
      
      training_query.sub_context = SemanticQueryContext.newBuilder()
          .setTSDB(context.tsdb())
          .setLocalSinks((List<QuerySink>) Lists.newArrayList((QuerySink) training_query))
          .setQuery(builder.build())
          .setStats(context.queryContext().stats())
          .setAuthState(context.queryContext().authState())
          .setHeaders(context.queryContext().headers())
          .build();
      
      training_query.sub_context.initialize(null)
          .addCallback(new TrainingQueryCB())
          .addErrback(new ErrorCB());
      context.tsdb().getStatsCollector().incrementCounter(
          "amomaly.query.prediction.baseline.query.count", 
          "model", factory.id(),
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
    } catch (Exception e) {
      handleError(e, prediction_index, false);
    }
  }
  
  protected void countdown() {
    int ct = latch.decrementAndGet();
    LOG.info("********** LATCH: " + ct);
    if (ct == 0) {
      run();
    }
  }
  
  void run() {
    try {
      if (current == null) {
        LOG.error("Current data is null!");
        sendUpstream(new QueryExecutionException("No current data.", 500, 0));
        return;
      }

      // Got baseline and current data, yay!
      TLongObjectMap<TimeSeries> map = new TLongObjectHashMap<TimeSeries>();
      for (final TimeSeries series : current.timeSeries()) {
        final long hash = series.id().buildHashCode();
        map.put(hash, series);
      }
      
      final AnomalyQueryResult result = new AnomalyQueryResult(
          this, current, config.getSerializeObserved());
      if (predictions.length > 1) {
        // join first then eval
        final TLongObjectMap<TimeSeries[]> series_arrays = 
            new TLongObjectHashMap<TimeSeries[]>();
        for (int i = 0; i < predictions.length; i++) {
          if (predictions[i] == null ||
              predictions[i].timeSeries() == null || 
              predictions[i].timeSeries().isEmpty()) {
            LOG.warn("Null or empty set of series at: " + i + "  " + predictions[i]);
            continue;
          }
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
          if (cur == null) {
            LOG.warn("No current data for hash: " + hash);
            continue;
          }
          evaluate(cur, iterator.value(), result);
          cur.close();
        }
      } else {
        final int series_limit = predictions[0].timeSeries().size();
        for (int i = 0; i < series_limit; i++) {
          final TimeSeries series = predictions[0].timeSeries().get(i);
          final long hash = series.id().buildHashCode();
          TimeSeries cur = map.remove(hash);
          if (cur == null) {
            LOG.warn("No current data for hash: " + hash);
            continue;
          }
          evaluate(cur, series, result);
          cur.close();
        }
      }
      map = null; // release to GC
      sendUpstream(result);
    } catch (Throwable t) {
      LOG.error("Failed to process egads query.", t);
      sendUpstream(t);
    }
  }
  
  void evaluate(final TimeSeries cur, 
                final TimeSeries prediction, 
                final AnomalyQueryResult result) {
    evaluate(cur, new TimeSeries[] { prediction }, result);
  }
  
  void evaluate(final TimeSeries cur, 
                final TimeSeries[] preds, 
                final AnomalyQueryResult result) {
    if (cur != null) {
      final AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(
          config,
          threshold_dps,
          cur,
          current,
          preds,
          predictions);
      eval.evaluate();
      
      final AnomalyPredictionTimeSeries pred_ts = new AnomalyPredictionTimeSeries(
          preds, this, result, "prediction");
      if (eval.alerts() != null && !eval.alerts().isEmpty()) {
        pred_ts.addAlerts(eval.alerts());
      }
      result.addPredictionsAndThresholds(pred_ts, predictions);
      
      if (config.getSerializeDeltas()) {
        final TimeSeries ts = new AnomalyThresholdTimeSeries(
            cur.id(), 
            "delta", 
            prediction_starts[0], 
            eval.deltas(), 
            eval.index(),
            factory.id());
        result.addPredictionsAndThresholds(ts, predictions);
      }
      
      if (config.getSerializeThresholds()) {
        if (config.getUpperThresholdBad() != 0) {
          final TimeSeries ts = new AnomalyThresholdTimeSeries(
              cur.id(), 
              "upperBad", 
              prediction_starts[0], 
              eval.upperBadThresholds(), 
              eval.index(),
              factory.id());
          result.addPredictionsAndThresholds(ts, predictions);
        }
        if (config.getUpperThresholdWarn() != 0) {
          final TimeSeries ts = new AnomalyThresholdTimeSeries(
              cur.id(), 
              "upperWarn", 
              prediction_starts[0], 
              eval.upperWarnThresholds(), 
              eval.index(),
              factory.id());
          result.addPredictionsAndThresholds(ts, predictions);
        }
        if (config.getLowerThresholdBad() != 0) {
          final TimeSeries ts = new AnomalyThresholdTimeSeries(
              cur.id(), 
              "lowerBad", 
              prediction_starts[0], 
              eval.lowerBadThresholds(), 
              eval.index(),
              factory.id());
          result.addPredictionsAndThresholds(ts, predictions);
        }
        if (config.getLowerThresholdWarn() != 0) {
          final TimeSeries ts = new AnomalyThresholdTimeSeries(
              cur.id(), 
              "lowerWarn", 
              prediction_starts[0], 
              eval.lowerWarnThresholds(), 
              eval.index(),
              factory.id());
          result.addPredictionsAndThresholds(ts, predictions);
        }
      }
    }
  }

  public NumericArrayAggregator newAggregator() {
    return (NumericArrayAggregator) aggregatorFactory.newAggregator(aggregatorConfig);
  }

  protected String modelId() {
    return factory().id();
  }

  protected void writeCache(final QueryResult result, final int prediction_index) {
    if (cache_hits[prediction_index] || cache == null) {
      return;
    }
    
    context.tsdb().getQueryThreadPool().submit(new Runnable() {
      public void run() {
        try {
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
            result.close();
            return null;
          }
        }
        
        final long expiration;
        if (model_units == ChronoUnit.HOURS) {
          long end = prediction_starts[prediction_index] + 3600;
          if (DateTime.currentTimeMillis() / 1000 < end) {
            expiration = (60 * 5) * 1000;
          } else {
            expiration = 3600 * 2 * 1000;
          }
        } else {
          expiration = 86400 * 2 * 1000;
        }
        
        cache.cache(cache_keys[prediction_index], expiration, result, null)
          .addCallback(new SuccessCB())
          .addErrback(new CacheErrorCB());
        context.tsdb().getStatsCollector().incrementCounter(
            "amomaly.query.prediction.cache.write", 
            "model", factory.type(),
            "mode", config.getMode().toString());
        } catch (Throwable t) {
          LOG.error("Failed to cache prediction", t);
        }
      }
    });
  }
  
  protected void handleError(final Exception e,
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
  
  protected boolean startPrediction(final int prediction_index) {
    if (cache == null || config.getMode() == ExecutionMode.CONFIG) {
      return true;
    }
    
    AnomalyPredictionState state = cache.getState(cache_keys[prediction_index]);
    if (state == null) {
      state = new AnomalyPredictionState();
      state.host = ((BaseAnomalyFactory) factory).hostName();
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
          return true;
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
      return true;
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

  protected void updateState(final State new_state,
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

  public Deferred<Void> predictAndSet(final QueryResult result, 
                                      final int prediction_index) {
    return Deferred.fromError(new UnsupportedOperationException(
        "This method must be implemented."));
  }

  public NumericArrayAggregatorFactory getAggregatorFactory() {
    return aggregatorFactory;
  }
}
