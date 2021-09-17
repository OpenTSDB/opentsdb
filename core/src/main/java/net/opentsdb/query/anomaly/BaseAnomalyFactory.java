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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.anomaly.AnomalyConfig.ExecutionMode;
import net.opentsdb.query.anomaly.AnomalyPredictionState.State;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.ProcessorFactory;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.downsample.DownsampleFactory;
import net.opentsdb.utils.BigSmallLinkedBlockingQueue;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * A base factory for anomaly nodes that has launches a thread pool for running
 * jobs without affecting other threads.
 * 
 * TODO - make this a shared pool or use existing pools. Having too many threads
 * is an issue.
 * 
 * @param <C> The type of config.
 * @param <N> The node.
 * 
 * @since 3.0
 */
public abstract class BaseAnomalyFactory<C extends BaseAnomalyConfig, N extends AbstractQueryNode>
    extends BaseTSDBPlugin implements ProcessorFactory<C, N> {
  protected final Logger LOG = LoggerFactory.getLogger(getClass());
  
  public static final String CONFIG_PREFIX = "anomaly.";
  
  public static final String QUEUE_THRESHOLD_KEY = "queue.threshold";
  public static final int DEFAULT_QUEUE_THRESHOLD = 86400;
  
  public static final String THREAD_COUNT_KEY = "thread.count";
  public static final int DEFAULT_THREAD_COUNT = 8;

  /** The map of iterator factories keyed on type. */
  protected final Map<TypeToken<? extends TimeSeriesDataType>, 
      QueryIteratorFactory> iterator_factories;
  
  protected String type;
  protected int type_hash;
  protected Predicate<AnomalyPredictionJob> bigJobPredicate;
  protected BigSmallLinkedBlockingQueue<AnomalyPredictionJob> queue;
  protected int threadCount;
  protected Thread[] threads;
  protected volatile PredictionCache cache;
  protected String host_name;
  protected String threshold_key;
  
  /**
   * Default ctor.
   */
  public BaseAnomalyFactory() {
    iterator_factories = Maps.newHashMapWithExpectedSize(3);
  }
  
  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return iterator_factories.keySet();
  }

  @Override
  public <T extends TimeSeriesDataType> void registerIteratorFactory(
      final TypeToken<? extends TimeSeriesDataType> type,
      final QueryIteratorFactory<N, T> factory) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (factory == null) {
      throw new IllegalArgumentException("Factory cannot be null.");
    }
    if (iterator_factories.containsKey(type)) {
      LOG.warn("Replacing existing iterator factory: " + 
          iterator_factories.get(type) + " with factory: " + factory);
    }
    iterator_factories.put(type, factory);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registering iteratator factory: " + factory 
          + " with type: " + type);
    }
  }

  @Override
  public <T extends TimeSeriesDataType>  TypedTimeSeriesIterator newTypedIterator(
      final TypeToken<T> type,
      final N node,
      final QueryResult result,
      final Collection<TimeSeries> sources) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (sources == null || sources.isEmpty()) {
      throw new IllegalArgumentException("Sources cannot be null or empty.");
    }
    
    final QueryIteratorFactory factory = iterator_factories.get(type);
    if (factory == null) {
      return null;
    }
    return factory.newIterator(node, result, sources, type);
  }

  @Override
  public <T extends TimeSeriesDataType> TypedTimeSeriesIterator newTypedIterator(
      final TypeToken<T> type,
      final N node,
      final QueryResult result,
      final Map<String, TimeSeries> sources) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (sources == null || sources.isEmpty()) {
      throw new IllegalArgumentException("Sources cannot be null or empty.");
    }
    
    final QueryIteratorFactory factory = iterator_factories.get(type);
    if (factory == null) {
      return null;
    }
    return factory.newIterator(node, result, sources, type);
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    if (Strings.isNullOrEmpty(type)) {
      throw new IllegalStateException("Implementation must set the `type` field.");
    }
    this.tsdb = tsdb;
    this.id = Strings.isNullOrEmpty(id) ? type : id;
    type_hash = Const.HASH_FUNCTION().newHasher()
        .putString(type, Const.UTF8_CHARSET)
        .hash()
        .asInt();
    
    registerConfigs();
    threshold_key = getConfigKey(QUEUE_THRESHOLD_KEY);
    bigJobPredicate =
        prediction -> prediction.jobThreshold() > 
        tsdb.getConfig().getInt(threshold_key);
    queue = new BigSmallLinkedBlockingQueue<>(tsdb, "anomalyQueue", bigJobPredicate);
    threadCount = tsdb.getConfig().getInt(getConfigKey(THREAD_COUNT_KEY));
    threads = new Thread[threadCount];
    LOG.info(
        "Initialized Anomly factory {} with job queue settings {}: {} {}: {}",
        id,
        threshold_key,
        tsdb.getConfig().getInt(threshold_key),
        getConfigKey(THREAD_COUNT_KEY),
        threadCount);
    for (int i = 0; i < threads.length; i++) {
      Thread thread = new Thread(() -> {
        while (true) {
          try {
            queue.take().run();
          } catch (InterruptedException ignored) {
            LOG.error("Prophet thread interrupted", ignored);
          }catch (Throwable throwable) {
            LOG.error("Error running Prophet job", throwable);
          }
        }
      }, id + " Job Thread " + (i + 1));
      thread.start();
      threads[i] = thread;
    }
    
    try {
      host_name = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Failed to get the hostname", e);
    } 
    
    return Deferred.fromResult(null);
  };

  @Override
  public N newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final C config, 
                         final QueryPlanner planner) {
    // UGG!!! initialization order issue.
    if (cache == null) {
      synchronized (this) {
        if (cache == null) {
          cache = tsdb.getRegistry().getDefaultPlugin(
              PredictionCache.class);
          LOG.info("Anomaly Cache: " + cache);
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
      if (LOG.isTraceEnabled()) {
        LOG.trace("No state found for " + Arrays.toString(cache_key));
      }
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

  public void submitJob(final AnomalyPredictionJob job) {
    queue.put(job);
  }
  
  @Override
  public String type() {
    return type;
  }
  
  public byte[] generateCacheKey(final TimeSeriesQuery query, 
                                    final int prediction_start) {
    // hash of model ID, query hash, prediction timestamp w jitter
    byte[] key = new byte[4 + 8 + 4];
    Bytes.setInt(key, type_hash, 0);
    Bytes.setLong(key, query.buildHashCode().asLong(), 4);
    Bytes.setInt(key, prediction_start, 12); 
    return key;
  }
  
  public int predictionStartTime(final TimeSeriesQuery query, 
      final C config, 
      final QueryPlanner planner) {
    final long baseline_span;
    if (!Strings.isNullOrEmpty(config.getTrainingInterval())) {
      baseline_span = DateTime.parseDuration(config.getTrainingInterval()) / 1000;
    } else {
      baseline_span = (query.endTime().epoch() - query.startTime().epoch()) * 3;
    }
    final ChronoUnit model_units; 
    if (baseline_span < 86400) {
      model_units = ChronoUnit.HOURS;
    } else {
      model_units = ChronoUnit.DAYS;
    }
    
    DownsampleConfig ds = null;
    for (final QueryNodeConfig node : query.getExecutionGraph()) {
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
  
  public int jitter(final TimeSeriesQuery query, 
                    final ChronoUnit model_units) {
    if (model_units == ChronoUnit.DAYS) {
      // 1 hour jitter on 1m
      return (int) Math.abs(query.buildHashCode().asLong() % 59) * 60;
    } else {
      // 5m jitter on 15s
      return (int) Math.abs(query.buildHashCode().asLong() % 20) * 15;
    }
  }
  
  public PredictionCache cache() {
    return cache;
  }

  public String hostName() {
    return host_name;
  }
 
  @VisibleForTesting
  public void setCache(final PredictionCache cache) {
    this.cache = cache;
  }
  
  protected void registerConfigs() {
    if (!tsdb.getConfig().hasProperty(getConfigKey(QUEUE_THRESHOLD_KEY))) {
      tsdb.getConfig().register(
          getConfigKey(QUEUE_THRESHOLD_KEY),
          DEFAULT_QUEUE_THRESHOLD,
          true,
          "Threshold to chose small or big queue for prediction jobs.");
    }
    
    if (!tsdb.getConfig().hasProperty(getConfigKey(THREAD_COUNT_KEY))) {
      tsdb.getConfig().register(
          getConfigKey(THREAD_COUNT_KEY),
          DEFAULT_THREAD_COUNT,
          false,
          "Anomaly prediction worker thread count.");
    }
  }
  
  protected String getConfigKey(final String suffix) {
    return CONFIG_PREFIX + (Strings.isNullOrEmpty(id) || id.equals(type) ? 
        "" : id + ".")
      + suffix;
  }
}
