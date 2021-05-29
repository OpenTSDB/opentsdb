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
package net.opentsdb.query.processor.bucketquantile;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.pools.ArrayObjectPool;
import net.opentsdb.pools.DoubleArrayPool;
import net.opentsdb.pools.IntArrayPool;
import net.opentsdb.pools.LongArrayPool;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.rollup.RollupConfig;

/**
 * Quantile node that expects a certain number of histogram metrics and once
 * all are received, joins and creates a result set for computing quantiles
 * from the counts.
 * 
 * @since 3.0
 */
public class BucketQuantile extends AbstractQueryNode {
  private static final Logger LOG = LoggerFactory.getLogger(
      BucketQuantile.class);
  
  private final BucketQuantileConfig config;
  private final Map<QueryResultId, QueryResult> results;
  private final Bucket[] buckets;
  private final AtomicBoolean failed;
  private final ArrayObjectPool int_array_pool;
  private final ArrayObjectPool long_array_pool;
  private final ArrayObjectPool double_array_pool;
  
  /**
   * Default ctor.
   * @param factory The non-null factory.
   * @param context The non-null context.
   * @param config The non-null config.
   */
  public BucketQuantile(final QueryNodeFactory factory, 
                        final QueryPipelineContext context,
                        final BucketQuantileConfig config) {
    super(factory, context);
    this.config = config;
    results = Maps.newConcurrentMap();
    failed = new AtomicBoolean();
    if (config.underflowId() != null) {
      results.put(config.underflowId(), DUMMY);
    }
    if (config.overflowId() != null) {
      results.put(config.overflowId(), DUMMY);
    }
    
    for (final QueryResultId id : config.histogramIds()) {
      results.put(id, DUMMY);
    }
    
    // parse out the metrics
    buckets = new Bucket[results.size()];
    int bucket_idx = 0;
    for (final String histogram : config.histogramMetrics()) {
      final Matcher matcher = config.pattern().matcher(histogram);
      if (!matcher.find()) {
        throw new IllegalArgumentException("Histogram metric [" + histogram 
            + "] did not match the regex pattern: " + config.getBucketRegex());
      }
      if (matcher.groupCount() < 2) {
        throw new IllegalArgumentException("Histogram metric [" + histogram 
            + "] did not match two groups from the regex pattern: " 
            + config.getBucketRegex());
      }
      double lower = Double.NaN;
      try {
         lower = Double.parseDouble(matcher.group(1));
      } catch (NumberFormatException e) {
        // actually shouldn't happen as the regex would toss it
        throw new IllegalArgumentException("Failed to parse lower bucket from "
            + "metric [" + histogram + "] using pattern: " 
            + config.getBucketRegex() + " that matched: " + matcher.group(1));
      }
      
      double upper = Double.NaN;
      try {
        // actually shouldn't happen as the regex would toss it
        upper = Double.parseDouble(matcher.group(2));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Failed to parse upper bucket from "
            + "metric [" + histogram + "] using pattern: " 
            + config.getBucketRegex() + " that matched: " + matcher.group(2));
      }
      buckets[bucket_idx++] = new Bucket(histogram, lower, upper);
    }
    
    if (!Strings.isNullOrEmpty(config.underflowMetric())) {
      buckets[bucket_idx++] = new Bucket(config.underflowMetric(), false, true);
    }
    
    if (!Strings.isNullOrEmpty(config.overflowMetric())) {
      buckets[bucket_idx++] = new Bucket(config.overflowMetric(), true, false);
    }
    
    Arrays.sort(buckets, BUCKET_COMPARATOR);
    ObjectPool pool = context.tsdb().getRegistry().getObjectPool(IntArrayPool.TYPE); 
    int_array_pool = pool != null ? (ArrayObjectPool) pool : null;
    pool = context.tsdb().getRegistry().getObjectPool(LongArrayPool.TYPE);
    long_array_pool = pool != null ? (ArrayObjectPool) pool : null;
    pool = context.tsdb().getRegistry().getObjectPool(DoubleArrayPool.TYPE);
    double_array_pool = pool != null ? (ArrayObjectPool) pool : null;
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public void onNext(final QueryResult next) {
    if (results.get(next.dataSource()) != DUMMY) {
      LOG.warn("Unexpected result: " + next.dataSource());
      return;
    }
    
    if (!Strings.isNullOrEmpty(next.error()) || next.exception() != null) {
      // failed
      if (failed.compareAndSet(false, true)) {
        if (next.exception() != null) {
          onError(next.exception());
        } else {
          onError(new QueryDownstreamException(next.error()));
        }
      }
    }
    
    results.put(next.dataSource(), next);
    int dummies = 0;
    for (final Entry<QueryResultId, QueryResult> entry : results.entrySet()) {
      if (entry.getValue() == DUMMY) {
        dummies++;
      }
    }
    
    if (dummies == 0) {
      LOG.debug("All results in! Processing.");
    } else {
      LOG.debug("Still waiting: Dummies: " + dummies);
      return;
    }
    
    final BucketQuantileResult result = new BucketQuantileResult(this);
    List<Deferred<Void>> deferreds = Lists.newArrayList();
    for (final Entry<QueryResultId, QueryResult> entry : results.entrySet()) {
      deferreds.add(result.addResult(entry.getValue()));
    }

    class ErrCB implements Callback<Void, Exception> {

      @Override
      public Void call(final Exception e) throws Exception {
        LOG.error("Failed to join results", e);
        onError(e);
        return null;
      }
      
    }
    
    class ResolveCB implements Callback<Void, ArrayList<Void>> {

      @Override
      public Void call(final ArrayList<Void> arg) throws Exception {
        result.finishSetup();
        sendUpstream(result);
        return null;
      }
      
    }
    Deferred.group(deferreds)
      .addCallback(new ResolveCB())
      .addErrback(new ErrCB());
  }
  
  Bucket[] buckets() {
    return buckets;
  }

  Map<QueryResultId, QueryResult> results() {
    return results;
  }
  
  ArrayObjectPool intArrayPool() {
    return int_array_pool;
  }
  
  ArrayObjectPool longArrayPool() {
    return long_array_pool;
  }
  
  ArrayObjectPool doubleArrayPool() {
    return double_array_pool;
  }
  
  /**
   * A bucket definition that is sorted once we have the under flow, over flow
   * and histos.
   */
  public class Bucket {
    String metric;
    double lower;
    double upper;
    double report;
    boolean is_overflow;
    boolean is_underflow;
    
    Bucket(String metric, double lower, double upper) {
      this.metric = metric;
      this.lower = lower;
      this.upper = upper;
      switch (config.getOutputOfBucket()) {
      case BOTTOM:
        report = lower;
        break;
      case TOP:
        report = upper;
        break;
      case MEAN:
        report = lower + (upper - lower) / 2;
        break;
      default:
        throw new IllegalArgumentException("No handler for: " + 
            config.getOutputOfBucket());
      }
    }
    
    Bucket(String metric, boolean is_overflow, boolean is_underflow) {
      this.metric = metric;
      this.is_overflow = is_overflow;
      this.is_underflow = is_underflow;
      if (is_overflow) {
        report = config.getOverflowMax();
      } else {
        report = config.getUnderflowMin();
      }
    }
    
  }
  
  /**
   * A static object used to tell the node that it hasn't seen a result yet
   * but expects one.
   */
  static class DummyResult implements QueryResult {

    @Override
    public TimeSpecification timeSpecification() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<TimeSeries> timeSeries() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String error() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Throwable exception() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public long sequenceId() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public QueryNode source() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public QueryResultId dataSource() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ChronoUnit resolution() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public RollupConfig rollupConfig() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }

    @Override
    public boolean processInParallel() {
      // TODO Auto-generated method stub
      return false;
    }
    
  }
  protected final static DummyResult DUMMY = new DummyResult();

  static class BucketComparator implements Comparator<Bucket> {

    @Override
    public int compare(final Bucket o1, final Bucket o2) {
      if (o1.is_underflow) {
        return -1;
      } else if (o1.is_overflow) {
        return 1;
      }
      return o1.lower < o2.lower ? -1 : 
          o1.lower == o2.lower ? 0 : 1;
    }
    
  }
  private static final BucketComparator BUCKET_COMPARATOR = new BucketComparator();
}
