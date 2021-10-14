// This file is part of OpenTSDB.
// Copyright (C) 2018-2021  The OpenTSDB Authors.
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

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.processor.merge.MergerConfig.MergeMode;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.utils.DateTime;

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
  protected final List<QueryResult> queryResults;
  
  /** The list of results. */
  protected List<TimeSeries> results;
  
  /** The first non-null time specification. */
  protected TimeSpecification time_spec;
  
  /** The first non-null rollup config. */
  protected RollupConfig rollup_config;
  
  /** Errors or exceptions from downstream. */
  protected String error;
  protected Throwable exception;
  protected final List<String> sources;
  
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
    this.queryResults = Lists.newArrayList();
    sources = ((MergerConfig) node.config()).sortedSources();
    for (int i = 0; i < sources.size(); i++) {
      String id = sources.get(i);
      if (id.equals(next.dataSource().dataSource())) {
        this.queryResults.add(next);
      } else {
        this.queryResults.add(null);
      }
    }
    if (((MergerConfig) node.config()).getMode() == MergeMode.SPLIT &&
            next.timeSpecification() != null) {
      // it's downsampled so we need to create the time spec
      time_spec = new MergerTimeSpec();
    } else {
      time_spec = next.timeSpecification();
    }
    rollup_config = next.rollupConfig();
  }
  
  /**
   * Adds the result to the list.
   * @param next
   */
  void add(final QueryResult next) {
    boolean set = false;
    for (int i = 0; i < sources.size(); i++) {
      String id = sources.get(i);
      if (id.equals(next.dataSource().dataSource())) {
        this.queryResults.set(i, next);
        set = true;
        break;
      }
    }

    if (!set) {
      throw new IllegalArgumentException("Result " + next.dataSource()
              + " wasn't in our list: " + sources);
    }

    if (time_spec == null && next.timeSpecification() != null) {
      if (((MergerConfig) node.config()).getMode() == MergeMode.SPLIT &&
              next.timeSpecification() != null) {
        // it's downsampled so we need to create the time spec
        time_spec = new MergerTimeSpec();
      } else {
        time_spec = next.timeSpecification();
      }
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
    if (((MergerConfig) node.config()).getAggregator() == null) {
      // Shard mode without a group by so no need to join.
      final List<TimeSeries> timeSeries = Lists.newArrayList();
      for (final QueryResult next : this.queryResults) {
        if (!Strings.isNullOrEmpty(next.error())) {
          error = next.error();
          exception = next.exception();
          if (((MergerConfig) node.config()).getAllowPartialResults()) {
            with_error++;
          } else {
            results = Collections.emptyList();
            return;
          }
        }
        for (final TimeSeries series : next.timeSeries()) {
          timeSeries.add(series);
        }
      }
      results = timeSeries;
    } else {
      final TLongObjectMap<TimeSeries> groups = new TLongObjectHashMap<TimeSeries>();
      for (final QueryResult next : this.queryResults) {
        if (!Strings.isNullOrEmpty(next.error())) {
          error = next.error();
          exception = next.exception();
          if (((MergerConfig) node.config()).getAllowPartialResults() ||
              ((MergerConfig) node.config()).getMode() == MergeMode.HA) {
            with_error++;
          } else {
            results = Collections.emptyList();
            return;
          }
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
      results = Lists.newArrayList(groups.valueCollection());
    }

    if (with_error == queryResults.size()) {
      results = Collections.emptyList();
    } else {
      // allowed due to HA or
      error = null;
      exception = null;
    }
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    return time_spec;
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return results;
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
    return queryResults.get(0).sequenceId();
  }
  
  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public QueryResultId dataSource() {
    return ((MergerConfig) node.config()).resultIds().get(0);
  }
  
  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return queryResults.get(0).idType();
  }
  
  @Override
  public ChronoUnit resolution() {
    return queryResults.get(0).resolution();
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
      for (final QueryResult next : this.queryResults) {
        next.close();
      }
    }
  }

  @Override
  public boolean processInParallel() {
    return false;
  }

  class MergerTimeSpec implements TimeSpecification {

    private final TimeStamp end;
    private final TemporalAmount interval;

    MergerTimeSpec() {
      end = ((MergerConfig) node.config()).firstDataTimestamp().getCopy();
      interval = DateTime.parseDuration2(((MergerConfig) node.config()).aggregatorInterval());
      for (int i = 0; i < ((MergerConfig) node.config()).getAggregatorArraySize(); i++) {
        end.add(interval);
      }
    }

    @Override
    public TimeStamp start() {
      return ((MergerConfig) node.config()).firstDataTimestamp();
    }

    @Override
    public TimeStamp end() {
      return end;
    }

    @Override
    public TemporalAmount interval() {
      return interval;
    }

    @Override
    public String stringInterval() {
      return ((MergerConfig) node.config()).aggregatorInterval();
    }

    @Override
    public ChronoUnit units() {
      return ChronoUnit.SECONDS;
    }

    @Override
    public ZoneId timezone() {
      return null;
    }

    @Override
    public void updateTimestamp(int offset, TimeStamp timestamp) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void nextTimestamp(TimeStamp timestamp) {
      throw new UnsupportedOperationException();
    }
  }
}
