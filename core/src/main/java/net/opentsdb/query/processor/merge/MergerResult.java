// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.rollup.RollupConfig;

/**
 * A result from the {@link Merger} node for a segment. The grouping is 
 * performed on the tags specified in the config and then grouped by hash code
 * instead of string on the resulting time series IDs.
 * 
 * TODO - validate all the results share the same downsampling, etc.
 * 
 * @since 3.0
 */
public class MergerResult implements QueryResult {
  
  /** Used to denote when all of the upstreams are done with this result set. */
  protected final CountDownLatch latch;
  
  /** The parent node. */
  protected final Merger node;
  
  /** The downstream result received by the group by node. */
  protected final List<QueryResult> next;
  
  /** The map of hash codes to groups. */
  protected final Map<Long, TimeSeries> groups;
  
  /** The first non-null time specification. */
  protected TimeSpecification time_spec;
  
  /** The first non-null rollup config. */
  protected RollupConfig rollup_config;
  
  /** Errors or exceptions from downstream. */
  protected String error;
  protected Throwable exception;
  
  /**
   * The default ctor.
   * @param node The non-null group by node this result belongs to.
   * @param next The non-null original query result.
   * @throws IllegalArgumentException if the node or result was null.
   */
  public MergerResult(final Merger node, final QueryResult next) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (next == null) {
      throw new IllegalArgumentException("Query results cannot be null.");
    }
    
    latch = new CountDownLatch(node.upstreams());
    this.node = node;
    this.next = Lists.newArrayList();
    this.next.add(next);
    groups = Maps.newHashMap();
    time_spec = next.timeSpecification();
    rollup_config = next.rollupConfig();
  }
  
  /**
   * Adds the result to the list.
   * @param next
   */
  void add(final QueryResult next) {
    this.next.add(next);
    if (time_spec == null && next.timeSpecification() != null) {
      time_spec = next.timeSpecification();
    }
    if (rollup_config == null && next.rollupConfig() != null) {
      rollup_config = next.rollupConfig();
    }
  }
  
  /**
   * Join the series.
   */
  void join() {
    int with_error = 0;
    for (final QueryResult next : this.next) {
      if (!Strings.isNullOrEmpty(next.error())) {
        with_error++;
        continue;
      }
      
      for (final TimeSeries series : next.timeSeries()) {
        final long hash = series.id().buildHashCode();
        TimeSeries ts = groups.get(hash);
        if (ts == null) {
          ts = new MergerTimeSeries(node, this);
          groups.put(hash, ts);
        }
        ((MergerTimeSeries) ts).addSource(series);
      }
    }
    
    if (with_error == next.size()) {
      // TODO - maybe find a common error, otherwise pick one.
      error = next.get(0).error();
      exception = next.get(0).exception();
    }
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    return time_spec;
  }

  @Override
  public Collection<TimeSeries> timeSeries() {
    return groups.values();
  }
  
  @Override
  public String error() {
    return error;
  }
  
  @Override
  public Throwable exception() {
    return exception;
  }
  
  @Override
  public long sequenceId() {
    return next.get(0).sequenceId();
  }
  
  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public String dataSource() {
    return ((MergerConfig) node.config()).getDataSource();
  }
  
  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return next.get(0).idType();
  }
  
  @Override
  public ChronoUnit resolution() {
    return next.get(0).resolution();
  }
  
  @Override
  public RollupConfig rollupConfig() {
    return rollup_config;
  }
  
  @Override
  public void close() {
    // NOTE - a race here. Should be idempotent.
    latch.countDown();
    if (latch.getCount() <= 0) {
      for (final QueryResult next : this.next) {
        next.close();
      }
    }
  }
  
}
