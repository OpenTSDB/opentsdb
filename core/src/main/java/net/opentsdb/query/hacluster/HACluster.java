// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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
package net.opentsdb.query.hacluster;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.BaseWrappedQueryResult;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;

/**
 * A node that handles downstream HA sources. When a result comes in it
 * will be held until either another source arrives or a timeout is 
 * reached. See {@link HAClusterFactory}.
 * <p>
 * If a timeout is reached, then an empty result is created with the
 * ID and source of the expected node and sent upstream. If the data
 * still arrives after the timeout, it's simply dropped.
 * 
 * @since 3.0
 */
public class HACluster extends AbstractQueryNode implements TimeSeriesDataSource{
  private static final Logger LOG = LoggerFactory.getLogger(HACluster.class);
  
  /** The config. */
  protected final HAClusterConfig config;

  /** A flag to determine if the node is finished. */
  protected final AtomicBoolean completed;
  
  /** The map of results we expect from downstream. */
  protected Map<String, QueryResult> results;
  
  /** Timers when data is received. */
  protected Timeout primary_timer;
  protected Timeout secondary_timer;
  
  /**
   * Default ctor.
   * @param factory The non-null factory we came from.
   * @param context The non-null query context.
   * @param config The non-null config.
   */
  public HACluster(final QueryNodeFactory factory, 
            final QueryPipelineContext context,
            final HAClusterConfig config) {
    super(factory, context);
    this.config = config;
    completed = new AtomicBoolean();
  }

  @Override
  public HAClusterConfig config() {
    return config;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }

  @Override
  public void onNext(final QueryResult next) {
    final String id = next.source().config().getId();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Result: " + id + ":" + next.dataSource() + " Expect: " 
          + results.keySet());
    }
    
    // ignore sources we don't care about. They shouldn't be linked
    // to this node anyway.
    if (!results.containsKey(id)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received unexpected data from: " + id + ":" + next.dataSource());
      }
      if (context.query().isDebugEnabled()) {
        context.queryContext().logDebug(this, "Received unexpected data from: " 
            + id + ":" + next.dataSource());
      }
      return;
    }
    
    boolean all_in = true;
    synchronized (results) {
      QueryResult extant = results.putIfAbsent(id, next);
      if (extant != null) {
        LOG.warn("Duplicate result from source: " + id + ":" + next.dataSource());
        context.queryContext().logWarn(this, "Duplicate result from source: " 
            + id + next.dataSource());
        return;
      }
     
      for (final Entry<String, QueryResult> entry : results.entrySet()) {
        if (entry.getValue() == null) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("MISSING: " + entry.getKey());
          }
          all_in = false;
          break;
        }
      }
    }
    
    if (all_in) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("All in!");
      }
      complete(false);
      return;
    }
    
    // if the result was in error then don't start a timer so we can let the
    // other sources respond.
    if (Strings.isNullOrEmpty(next.error()) && next.exception() == null) {
      if (config.getDataSources().get(0).equals(next.dataSource())) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Received primary: " + id + ":" + next.dataSource());
        }
        if (context.query().isTraceEnabled()) {
          context.queryContext().logTrace(this, "Received primary: " 
              + id + ":" + next.dataSource());
        }
        synchronized (this) {
          if (secondary_timer == null) {
            // start timer!
            secondary_timer = context.tsdb().getQueryTimer()
              .newTimeout(new ResultTimeout(false), 
                  DateTime.parseDuration(config.getSecondaryTimeout()), 
                  TimeUnit.MILLISECONDS);
          } else {
            // got enough results! pop em up
            complete(false);
          }
        }
      } else {
        boolean matched = false;
        for (final String source : config.getDataSources()) {
          LOG.trace("CMP: " + id + " want src: " + source);
          if (source.equals(id)) {
            matched = true;
            break;
          }
        }
        
        if (!matched) {
          // WTF?
          if (LOG.isTraceEnabled()) {
            LOG.trace("Source " + id + ":" 
                + next.dataSource() + " did not match a configured data source!");
          }
        } else {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Received secondary: " + id + ":" + next.dataSource());
          }
          if (context.query().isTraceEnabled()) {
            context.queryContext().logTrace(this, "Received secondary: " 
                + id + ":" + next.dataSource());
          }
          synchronized (this) {
            if (primary_timer == null) {
              // start it!
              primary_timer = context.tsdb().getQueryTimer()
                .newTimeout(new ResultTimeout(true), 
                    DateTime.parseDuration(config.getPrimaryTimeout()), 
                    TimeUnit.MILLISECONDS);
            } else {
              // got enough results, pop em up!
              complete(false);
            }
          }
        }
      }
    }
  }
  
  @Override
  public void onError(final Throwable t) {
    if (completed.compareAndSet(false, true)) {
      if (primary_timer != null) {
        primary_timer.cancel();
      }
      if (secondary_timer != null) {
        secondary_timer.cancel();
      }
      sendUpstream(t);
    }
  }
  
  @Override
  public Deferred<Void> initialize(final Span span) {
    super.initialize(span);
    results = Maps.newHashMapWithExpectedSize(downstream_sources.size());
    Collection<TimeSeriesDataSource> downstream_sources = this.downstream_sources;
    for (final TimeSeriesDataSource source : downstream_sources) {
      results.put(source.config().getId(), null);
    }
    if (results.isEmpty()) {
      return Deferred.fromError(new IllegalStateException(
          "Missing downstream sources."));
    }
    return INITIALIZED;
  }

  @Override
  public void fetchNext(Span span) {
  }

  @Override
  public String[] setIntervals() {
    return new String[0];
  }

  /** A timeout class that can generate empty results and trigger sending
   * data upstream. */
  private class ResultTimeout implements TimerTask {
    final boolean is_primary;
    
    ResultTimeout(final boolean is_primary) {
      this.is_primary = is_primary;
    }
    
    @Override
    public void run(final Timeout ignored) throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Executing timeout for " 
            + (is_primary ? "primary." : "secondary."));
      }
      
      if (!completed.compareAndSet(false, true)) {
        // Already sent upstream, lost the race.
        return;
      }
      if (is_primary) {        
        context.queryContext().logWarn(HACluster.this, 
            "Primary timed out after: " + config.getPrimaryTimeout());
      } else {
        context.queryContext().logWarn(HACluster.this, 
            "Secondary timed out after: " + config.getSecondaryTimeout());
      }
      
      QueryResult good_result = null;
      for (final QueryResult result : results.values()) {
        if (result != null) {
          good_result = result;
          break;
        }
      }
      
      final Iterator<Entry<String, QueryResult>> iterator = 
          results.entrySet().iterator();
      while (iterator.hasNext()) {
        final Entry<String, QueryResult> entry = iterator.next();
        if (entry.getValue() != null) {
          continue;
        }

        Collection<QueryNode> downstream_sources = HACluster.this.downstream_sources;
        for (final QueryNode down : downstream_sources) {
          if (down.config().getId().equals(entry.getKey())) {
            results.put(entry.getKey(), new EmptyResult(good_result, down));
            break;
          }
        }
      }
      
      complete(true);
    }
    
  }
  
  void complete(final boolean timed_out) {
    if (!timed_out && !completed.compareAndSet(false, true)) {
      LOG.warn("HA cluster node was trying to mark as complete but has "
          + "already timed out.");
      return;
    }
    
    if (primary_timer != null) {
      primary_timer.cancel();
    }
    if (secondary_timer != null) {
      secondary_timer.cancel();
    }
    
    try {
    // do it
    synchronized (results) {
      QueryResult good_result = null;
      for (final QueryResult result : results.values()) {
        if (result != null) {
          good_result = result;
          break;
        }
      }
      
      for (final Entry<String, QueryResult> entry : results.entrySet()) {
        if (entry.getValue() != null) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Sending up real result: " 
                + entry.getValue().source().config().getId() 
                + ":" + entry.getValue().dataSource());
          }
          if (timed_out) {
            context.tsdb().getQueryThreadPool().submit(new Runnable() {
              @Override
              public void run() {
                sendUpstream(entry.getValue());
              }
            }, context.queryContext());
          } else {
            sendUpstream(entry.getValue());
          }
        } else {
          // we need to send an empty result with the actual downstream 
          // node....
          Collection<QueryNode> downstream_sources = this.downstream_sources;
          for (final QueryNode node : downstream_sources) {
            final QueryResult cluster_result = new EmptyResult(good_result, node);
            if (node.config().getId().equals(entry.getKey())) {
              if (LOG.isTraceEnabled()) {
                LOG.trace("Sending up missing result: " 
                    + cluster_result.source().config().getId() 
                    + ":" + cluster_result.dataSource());
              }
              if (timed_out) {
                context.tsdb().getQueryThreadPool().submit(new Runnable() {
                  @Override
                  public void run() {
                    sendUpstream(cluster_result);
                  }
                });
              } else {
                sendUpstream(cluster_result);
              }
              break;
            }
          }
        }
      }
    }
    } catch (Throwable t) {
      LOG.error("WTF?", t);
    }
  }
  
  class EmptyResult extends BaseWrappedQueryResult {
    
    private final QueryNode node;
    
    EmptyResult(final QueryResult result, final QueryNode node) {
      super(result);
      this.node = node;
    }
    
    @Override
    public Collection<TimeSeries> timeSeries() {
      return Collections.emptyList();
    }
    
    @Override
    public QueryNode source() {
      return node;
    }

    @Override
    public String dataSource() {
      return result.dataSource();
    }

  }
}
