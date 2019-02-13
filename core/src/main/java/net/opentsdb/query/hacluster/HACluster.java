//This file is part of OpenTSDB.
//Copyright (C) 2018  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 2.1 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.
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
import net.opentsdb.query.QueryNodeConfig;
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
public class HACluster extends AbstractQueryNode {
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
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }

  @Override
  public void onNext(final QueryResult next) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Result: " + next.source().config().getId() + ":" 
          + next.dataSource() + " Expect: " + results.keySet());
    }
    
    // ignore sources we don't care about. They shouldn't be linked
    // to this node anyway.
    if (!results.containsKey(next.dataSource())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received unexpected data from: " 
            + next.source().config().getId() + next.dataSource());
      }
      if (context.query().isDebugEnabled()) {
        context.queryContext().logDebug(this, "Received unexpected data from: " 
            + next.source().config().getId() + next.dataSource());
      }
      return;
    }
    
    boolean all_in = true;
    synchronized (results) {
      QueryResult extant = results.putIfAbsent(next.dataSource(), next);
      if (extant != null) {
        LOG.warn("Late result from source: " 
            + next.source().config().getId() + ":" + next.dataSource());
        context.queryContext().logWarn(this, "Late result from source: " 
            + next.source().config().getId() + next.dataSource());
        return;
      }
     
      for (final Entry<String, QueryResult> entry : results.entrySet()) {
        if (entry.getValue() == null) {
          LOG.trace("MISSING: " + entry.getKey());
          all_in = false;
          break;
        }
      }
    }
    
    if (all_in) {
      complete(false);
      return;
    }
    
    // if the result was in error then don't start a timer so we can let the
    // other sources respond.
    if (Strings.isNullOrEmpty(next.error()) && next.exception() == null) {
      if (config.getDataSources().get(0).equals(next.dataSource())) {
        if (context.query().isTraceEnabled()) {
          context.queryContext().logTrace(this, "Received primary: " 
              + next.source().config().getId() + next.dataSource());
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
          if (next.dataSource().equals(source)) {
            matched = true;
            break;
          }
        }
        
        if (!matched) {
          // WTF? 
        } else {
          if (context.query().isTraceEnabled()) {
            context.queryContext().logTrace(this, "Received secondary: " 
                + next.source().config().getId() + next.dataSource());
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
    for (final TimeSeriesDataSource source : downstream_sources) {
      results.put(source.config().getId(), null);
    }
    if (results.isEmpty()) {
      return Deferred.fromError(new IllegalStateException(
          "Missing downstream sources."));
    }
    return INITIALIZED;
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
      LOG.warn("WTF? Already timed out??????");
      return;
    }
    
    if (primary_timer != null) {
      primary_timer.cancel();
    }
    if (secondary_timer != null) {
      secondary_timer.cancel();
    }
    
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
          if (timed_out) {
            context.tsdb().getQueryThreadPool().submit(new Runnable() {
              @Override
              public void run() {
                sendUpstream(new WrappedResult(entry.getValue()));
              }
            });
          } else {
            sendUpstream(new WrappedResult(entry.getValue()));
          }
        } else {
          // we need to send an empty result with the actual downstream 
          // node....
          for (final QueryNode node : downstream_sources) {
            final QueryResult cluster_result = new WrappedResult(
                new EmptyResult(good_result, node));
            if (node.config().getId().equals(entry.getKey())) {
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
  }
  
  class WrappedResult extends BaseWrappedQueryResult {
    
    WrappedResult(final QueryResult result) {
      super(result);
    }
    
    @Override
    public QueryNode source() {
      return HACluster.this;
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
      return node.config().getId();
    }

  }
}
