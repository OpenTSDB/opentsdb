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
package net.opentsdb.query.processor.dedup;

import com.google.common.reflect.TypeToken;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.rollup.RollupConfig;

/**
 * A node that handles deduplication and/or sorting of time series values
 * from underlying iterators.
 * 
 * @since 3.0
 */
public class DedupNode extends AbstractQueryNode {

  /** The node config. */
  private DedupConfig config;

  public DedupNode(final QueryNodeFactory factory,
                   final QueryPipelineContext context,
                   final String id,
                   final DedupConfig config) {
    super(factory, context, id);
    this.config = config;
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {
  }

  @Override
  public void onComplete(final QueryNode downstream, 
                         final long final_sequence,
                         final long total_sequences) {
    completeUpstream(final_sequence, total_sequences);
  }

  @Override
  public void onNext(final QueryResult next) {
    DedupResult dedupResult = new DedupResult(next);

    for (TimeSeries ts : next.timeSeries()) {
      Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> optional = ts
          .iterator(NumericType.TYPE);
      if (optional.isPresent()) {
        Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = 
            optional.get();
        TreeMap<TimeStamp, TimeSeriesValue<? extends TimeSeriesDataType>> tsMap = 
            new TreeMap<>();
        while (iterator.hasNext()) {
          TimeSeriesValue<? extends TimeSeriesDataType> tsValue = 
              iterator.next();
          tsMap.put(tsValue.timestamp(), tsValue);
        }
        dedupResult.results.add(new DedupTimeSeries(ts, 
            tsMap.values().iterator()));
      }
    }

    sendUpstream(dedupResult);
  }

  @Override
  public void onError(final Throwable t) {
    sendUpstream(t);
  }

  private class DedupResult implements QueryResult {

    private QueryResult next;
    private List<TimeSeries> results = new ArrayList<>();

    public DedupResult(QueryResult next) {
      this.next = next;
    }

    @Override
    public TimeSpecification timeSpecification() {
      return next.timeSpecification();
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
      return results;
    }

    @Override
    public long sequenceId() {
      return next.sequenceId();
    }

    @Override
    public QueryNode source() {
      return next.source();
    }

    @Override
    public String dataSource() {
      return next.dataSource();
    }
    
    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      return next.idType();
    }

    @Override
    public ChronoUnit resolution() {
      return next.resolution();
    }

    @Override
    public RollupConfig rollupConfig() {
      return next.rollupConfig();
    }

    @Override
    public void close() {
        next.close();
    }
  }

  private class DedupTimeSeries implements TimeSeries {

    private TimeSeries timeSeries;
    private Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator;

    public DedupTimeSeries(
        final TimeSeries timeSeries, 
        final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator) {
      this.timeSeries = timeSeries;
      this.iterator = iterator;
    }

    @Override
    public TimeSeriesId id() {
      return timeSeries.id();
    }

    @Override
    public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> 
        iterator(TypeToken<?> type) {
      if (type == NumericType.TYPE) {
        return Optional.of(iterator);
      }
      return timeSeries.iterator(type);
    }

    @Override
    public Collection<TypedIterator<
      TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
      List<TypedIterator<TimeSeriesValue<? extends TimeSeriesDataType>>> 
          iterators = timeSeries.iterators().stream().filter(
              ti -> ti.getType() == NumericType.TYPE
              ).collect(Collectors.toList());
      iterators.add(new TypedIterator(iterator, NumericType.TYPE));
      return iterators;
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return timeSeries.types();
    }

    @Override
    public void close() {
        timeSeries.close();
    }
  }
}
