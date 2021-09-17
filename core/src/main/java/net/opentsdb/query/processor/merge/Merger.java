// This file is part of OpenTSDB.
// Copyright (C) 2017-2021  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.merge;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorFactory;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.query.BaseWrappedQueryResult;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.processor.merge.MergerConfig.MergeMode;
import net.opentsdb.query.processor.merge.MergerFactory.NumericArrayIteratorFactory;
import net.opentsdb.query.processor.merge.MergerFactory.NumericIteratorFactory;
import net.opentsdb.query.readcache.CachedQueryNode;
import net.opentsdb.utils.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.stats.Span;

/**
 * Handles waiting for results from an HA or split query. If timeouts are
 * configured then on receipt of the first result, timeouts for any value that
 * hasn't been received are started.
 *
 * @since 3.0
 */
public class Merger extends AbstractQueryNode {
  private static final Logger LOG = LoggerFactory.getLogger(Merger.class);

  /** The source config. */
  private final MergerConfig config;

  /** Whether or not a result is ready to be sent upstream due to all results
   * being in or a timeout executing. */
  protected final AtomicBoolean complete;

  /** The result to populate and return. */
  protected Map<QueryResultId, Waiter> results;

  /** The result we'll send upstream. */
  protected volatile MergerResult mergerResult;

  /** How many results we're waiting on. */
  protected AtomicInteger outstanding;

  protected NumericAggregatorFactory numericAggregatorFactory;
  protected NumericArrayAggregatorFactory aggregatorFactory;

  /**
   * Default ctor.
   * @param factory The factory we came from.
   * @param context The non-null context.
   * @param config The non-null config.
   */
  public Merger(final QueryNodeFactory factory, 
                final QueryPipelineContext context,
                final MergerConfig config) {
    super(factory, context);
    if (config == null) {
      throw new IllegalArgumentException("Merger config cannot be null.");
    }
    this.config = config;
    complete = new AtomicBoolean();
  }

  @Override
  public Deferred<Void> initialize(final Span span) {
    super.initialize(span);
    final Collection<QueryResultId> expected =
        context.downstreamQueryResultIds(this);

    results = Maps.newHashMapWithExpectedSize(expected.size());
    for (final QueryResultId id : expected) {
      results.put(id, new Waiter(id));
    }

    for (int i = 0; i < config.sortedSources().size(); i++) {
      final String id = config.sortedSources().get(i);
      Waiter waiter = null;
      for (Entry<QueryResultId, Waiter> entry : results.entrySet()) {
        if (entry.getKey().dataSource().equals(id)) {
          waiter = entry.getValue();
          break;
        }
      }
      if (waiter == null) {
        return Deferred.fromError(new QueryDownstreamException(
                "Mismatch between expected sources " + expected
                        + " and the sorted list of sources "
                        + config.sortedSources()));
      }

      if (config.timeouts() != null && config.timeouts().size() > i) {
        waiter.timeoutMillis = DateTime.parseDuration(config.timeouts().get(i));
      }
    }

    aggregatorFactory = context.tsdb().getRegistry().getPlugin(
            NumericArrayAggregatorFactory.class, config.getAggregator());
    numericAggregatorFactory = context.tsdb().getRegistry().getPlugin(
            NumericAggregatorFactory.class, config.getAggregator());

    if (LOG.isTraceEnabled()) {
      LOG.trace("Expect to have results: " + results);
    }

    outstanding = new AtomicInteger(results.size());
    return Deferred.fromResult(null);
  }
  
  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {
    for (final Entry<QueryResultId, Waiter> entry : results.entrySet()) {
      final Waiter waiter = entry.getValue();
      if (waiter.timeout != null) {
        waiter.timeout.cancel();
      }
    }
  }
  
  @Override
  public void onNext(final QueryResult next) {
    if (complete.get()) {
      // lost a race so don't bother.
      return;
    }

    int countdown = 0;
    Waiter extant = results.get(next.dataSource());
    if (extant == null) {
      LOG.warn("Received a result in node " + config.getId()
              + " for an unexpected result " + next.dataSource()
              + ". Expecting " + config.sortedSources());
      return;
    }

    if (extant.result != null) {
      sendUpstream(new IllegalStateException("Received a duplicate result for "
              + next.dataSource() + " in node " + config.getId()));
      return;
    }

    extant.result = next;

    if (extant.timeout != null) {
      extant.timeout.cancel();
      extant.timeout = null;
    }
    countdown = outstanding.decrementAndGet();

    if (countdown != 0) {
      if (LOG.isTraceEnabled()) {
        final StringBuilder buffer = new StringBuilder()
                .append("Still waiting for: ");
        int wrote = 0;
        for (final Entry<QueryResultId, Waiter> entry : results.entrySet()) {
          if (entry.getValue().result == null) {
            if (wrote++ > 0) {
              buffer.append(", ");
            }
            buffer.append(entry.getKey());
          }
        }
        buffer.append(" on node ")
              .append(config.getId());
        LOG.trace(buffer.toString());
      }

      // start timers
      for (final Entry<QueryResultId, Waiter> entry : results.entrySet()) {
        if (entry.getKey().equals(next.dataSource())) {
          continue;
        }

        final Waiter waiter = entry.getValue();
        if (waiter.timeoutMillis > 0 && waiter.timeout == null) {
          waiter.timeout = context.tsdb().getMaintenanceTimer().newTimeout(
                  waiter, waiter.timeoutMillis, TimeUnit.MILLISECONDS);
        }
      }
      return;
    }

    if (!complete.compareAndSet(false, true)) {
      LOG.warn("Results in for " + config.getId() + " but we lost the timeout race.");
      return;
    }

    mergerResult = new MergerResult(this, next);
    for (final Entry<QueryResultId, Waiter> entry : results.entrySet()) {
      if (entry.getValue().timeout != null) {
        entry.getValue().timeout.cancel();
      }
      mergerResult.add(entry.getValue().result);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Received all results, joining and then sending for " + config.getId());
    }

    // got em all!
    mergerResult.join();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Sending merged results upstream for " + config.getId());
    }
    sendUpstream(mergerResult);
  }

  /** @return The number of upstream consumers. */
  protected int upstreams() {
    return upstream.size();
  }

  protected NumericArrayAggregator getNAI() {
    return aggregatorFactory.newAggregator(false);
  }

  class Waiter implements TimerTask {
    final QueryResultId id;
    volatile QueryResult result;
    volatile Timeout timeout;
    long timeoutMillis;

    Waiter(final QueryResultId id) {
      this.id = id;
    }

    @Override
    public void run(final Timeout timeout) throws Exception {
      if (!complete.compareAndSet(false, true)) {
        // lost a race.
        return;
      }

      // stop the other timers first, then we'll figure out the response.
      for (final Entry<QueryResultId, Waiter> entry : results.entrySet()) {
        if (entry.getValue().timeout != null) {
          entry.getValue().timeout.cancel();
        }
      }

      if (config.getMode() == MergeMode.SPLIT) {
        // TODO - eventually we may figure out a setting to return partial
        // results, but for now, fail it.
        sendUpstream(new QueryDownstreamException("Timeout waiting for results "
                + id + " for query node " + config.getId()));
        return;
      }

      // HA so if we have one good value we can send it
      QueryResult good_result = null;
      for (final Entry<QueryResultId, Waiter> entry : results.entrySet()) {
        if (entry.getValue().result != null) {
          // yay!
          good_result = entry.getValue().result;
          break;
        }
      }

      if (good_result == null) {
        sendUpstream(new QueryDownstreamException("Timeout waiting for results "
                + id + " for query node " + config.getId()
                + " and no other source responded in time."));
        return;
      }

      if (config.sortedSources().get(0).equals(id)) {
        context.queryContext().logWarn(Merger.this,
                "Secondary " + id + " timed out after: " + timeoutMillis + "ms");
      } else {
        context.queryContext().logWarn(Merger.this,
                "Primary " + id + " timed out after: " + timeoutMillis + "ms");
      }

      if (mergerResult == null) {
        mergerResult = new MergerResult(Merger.this, good_result);
      }
      for (final Entry<QueryResultId, Waiter> entry : results.entrySet()) {
        if (entry.getValue().result != null) {
          if (entry.getValue().result == good_result) {
            continue;
          }

          mergerResult.add(entry.getValue().result);
        } else {
          mergerResult.add(
                  new EmptyResult(good_result, Merger.this, entry.getKey()));
        }
      }

      mergerResult.join();
      sendUpstream(mergerResult);
    }
  }

  class EmptyResult extends BaseWrappedQueryResult {

    private final QueryNode node;
    private final QueryResultId missing;

    EmptyResult(final QueryResult result,
                final QueryNode node,
                final QueryResultId missing) {
      super(node, result);
      this.node = new CachedQueryNode(missing.nodeID(), node);
      this.missing = missing;
    }

    @Override
    public List<TimeSeries> timeSeries() {
      return Collections.emptyList();
    }

    @Override
    public QueryNode source() {
      return node;
    }

    @Override
    public QueryResultId dataSource() {
      return missing;
    }

  }
}
