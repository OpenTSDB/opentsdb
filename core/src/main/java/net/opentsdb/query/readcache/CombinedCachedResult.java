// This file is part of OpenTSDB.
// Copyright (C) 2019-2020  The OpenTSDB Authors.
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
package net.opentsdb.query.readcache;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.QuerySink;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.utils.DateTime;

/**
 * Result that splices together multiple cached or fresh results into a single
 * view for upstream.
 * 
 * @since 3.0
 */
public class CombinedCachedResult implements QueryResult, TimeSpecification {
  /** The non-null context. */
  protected final QueryPipelineContext context;
  
  /** The source results. */
  protected final QueryResult[] results;
  
  /** The sinks we'll deliver to. */
  protected final List<QuerySink> sinks;
  
  /** The result interval, i.e. width of each result. */
  protected final int result_interval;
  
  /** Parsed result units. */
  protected final ChronoUnit result_units;
  
  /** List of final time series results. */
  protected final List<TimeSeries> final_results;
  
  /** The source node. */
  protected final QueryNode<?> node;
  
  /** The data source name. */
  protected final QueryResultId data_source;
  
  /** Whether or not the query is aligned on hour boundaries. */
  protected final boolean aligned;
  
  /** Time spec start when applicable. */
  protected TimeStamp spec_start;
  
  /** Time spec end when applicable */
  protected TimeStamp spec_end;
  
  /** A time spec reference. 
   * TODO - see if this is invalidated on close of source. If so we may need
   * a deep copy here. */
  protected TimeSpecification spec;
  
  /** An optional rollup config. */
  protected RollupConfig rollup_config;
  
  /** An optional error. */
  protected String error;
  
  /** An optional exception. */
  protected Throwable exception;
  
  /**
   * Default ctor.
   * <b>WARNING: We assume that every QueryResult has the same time spec (aside 
   * from start and end times and rollup config and interval.
   * @param context The non-null context we belong to.
   * @param results The non-null array of results. May be empty.
   * @param node The non-null source node.
   * @param data_source The non-null and non-empty source name.
   * @param sinks The non-null list of sinks.
   * @param result_interval The result interval, i.e. width of each result.
   */
  public CombinedCachedResult(final QueryPipelineContext context,
                              final QueryResult[] results,
                              final QueryNode<?> node,
                              final QueryResultId data_source,
                              final List<QuerySink> sinks, 
                              final String result_interval) {
    this.context = context;
    this.results = results;
    this.node = node;
    this.data_source = data_source;
    this.sinks = sinks;
    this.result_interval = DateTime.getDurationInterval(result_interval);
    // TODO - if we have more in the future, handle the proper units.
    result_units = DateTime.getDurationUnits(result_interval).equals("h") ? 
        ChronoUnit.HOURS : ChronoUnit.DAYS;
    
    // TODO - figure out how to maintain order if at all possible.
    final TLongObjectMap<TimeSeries> time_series = 
        new TLongObjectHashMap<TimeSeries>();
    for (int i = 0; i < results.length; i++) {
      if (results[i] == null) {
        continue;
      }
      // SNAP!!!!
      if (spec_start == null && results[i].timeSpecification() != null) {
        int interval = DateTime.getDurationInterval(
            results[i].timeSpecification().stringInterval());
        ChronoUnit u = DateTime.unitsToChronoUnit(
            DateTime.getDurationUnits(results[i].timeSpecification().stringInterval()));
        spec_start = context.query().startTime();
        spec_start.snapToPreviousInterval(interval, u);
        while (spec_start.compare(Op.LT, context.query().startTime())) {
          spec_start.add(results[i].timeSpecification().interval());
        }
        
        spec_end = context.query().endTime();
        spec_end.snapToPreviousInterval(interval, u);
        while (spec_end.compare(Op.GT, context.query().endTime())) {
          spec_end.subtract(results[i].timeSpecification().interval());
        }
      }
      
      if (spec == null) {
        spec = results[i].timeSpecification();
      }
      if (rollup_config == null) {
        rollup_config = results[i].rollupConfig();
      }
      
      // if one or more contained an error we error out the whole shebang for 
      // now.
      if (!Strings.isNullOrEmpty(results[i].error()) || 
          results[i].exception() != null) {
        time_series.clear();
        error = results[i].error();
        exception = results[i].exception();
        break;
      }
      
      // TODO handle tip merge eventually
      for (final TimeSeries ts : results[i].timeSeries()) {
        final long hash = ts.id().buildHashCode();
        TimeSeries combined = time_series.get(hash);
        if (combined == null) {
          combined = new CombinedCachedTimeSeries(this, i, ts);
          time_series.put(hash, combined);
        } else {
          ((CombinedCachedTimeSeries) combined).add(i, ts);
        }
      }
    }
    final_results = Lists.newArrayList(time_series.valueCollection());
    
    // determine if we're aligned
    long start = context.query().startTime().epoch();
    start = start - (start % (result_units == ChronoUnit.HOURS ? 3600 : 86400));
    long end = context.query().endTime().epoch();
    end = end - (end % (result_units == ChronoUnit.HOURS ? 3600 : 86400));
    if (spec == null) {
      aligned = context.query().startTime().epoch() == start &&
          context.query().endTime().epoch() == end;
    } else {
      // we can fudge it if we're within the downsample window
      aligned = (context.query().startTime().epoch() - start < 
            spec.interval().get(ChronoUnit.SECONDS)) &&
          (context.query().endTime().epoch() - end < 
              spec.interval().get(ChronoUnit.SECONDS));
    }
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    return spec_start == null ? null : this;
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return final_results;
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
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public QueryResultId dataSource() {
    return data_source;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public ChronoUnit resolution() {
    return ChronoUnit.SECONDS;
  }

  @Override
  public RollupConfig rollupConfig() {
    return rollup_config;
  }

  @Override
  public void close() {
//    if (latch.decrementAndGet() == 0) {
//      for (final QuerySink sink : sinks) {
//        sink.onComplete();
//      }
//    }
    // TODO close results and series?
  }

  @Override
  public boolean processInParallel() {
    return false;
  }

  @Override
  public TimeStamp start() {
    return spec_start;
  }

  @Override
  public TimeStamp end() {
    return spec_end;
  }

  @Override
  public TemporalAmount interval() {
    return spec.interval();
  }

  @Override
  public String stringInterval() {
    return spec.stringInterval();
  }

  @Override
  public ChronoUnit units() {
    return spec.units();
  }

  @Override
  public ZoneId timezone() {
    return spec.timezone();
  }

  @Override
  public void updateTimestamp(final int offset, final TimeStamp timestamp) {
    spec.updateTimestamp(offset, timestamp);
  }

  @Override
  public void nextTimestamp(final TimeStamp timestamp) {
    spec.nextTimestamp(timestamp);
  }

  /** @return The source query results. */
  QueryResult[] results() {
    return results;
  }
  
  /** @return Package private the interval number from the result interval, 
   * e.g. "1" for "1h". */
  int resultInterval() {
    return result_interval;
  }
  
  /** @return Package private to return the result interval units. */
  ChronoUnit resultUnits() {
    return result_units;
  }

  /** @return Whether or not the query is aligned on cache boundaries. */
  boolean aligned() {
    return aligned;
  }
}
