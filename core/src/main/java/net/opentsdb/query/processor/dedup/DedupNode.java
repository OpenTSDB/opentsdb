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
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.BaseWrappedQueryResult;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
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
                   final DedupConfig config) {
    super(factory, context);
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
  public void onNext(final QueryResult next) {
    DedupResult dedupResult = new DedupResult(next);

    for (TimeSeries ts : next.timeSeries()) {
      Optional<TypedTimeSeriesIterator> optional = ts
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
  
  private class DedupResult extends BaseWrappedQueryResult {

    private List<TimeSeries> results = new ArrayList<>();

    public DedupResult(final QueryResult next) {
      super(next);
    }
    
    @Override
    public Collection<TimeSeries> timeSeries() {
      return results;
    }
    
    @Override
    public QueryNode source() {
      return DedupNode.this;
    }
    
  }

  private class DedupTimeSeries implements TimeSeries, TypedTimeSeriesIterator {

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
    public Optional<TypedTimeSeriesIterator> iterator(
        final TypeToken<? extends TimeSeriesDataType> type) {
      if (type == NumericType.TYPE) {
        return Optional.of(this);
      }
      return timeSeries.iterator(type);
    }

    @Override
    public Collection<TypedTimeSeriesIterator> iterators() {
      List<TypedTimeSeriesIterator> iterators = 
          timeSeries.iterators().stream().filter(
              ti -> ti.getType() == NumericType.TYPE
              ).collect(Collectors.toList());
      iterators.add(this);
      return iterators;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return timeSeries.types();
    }

    @Override
    public void close() {
        timeSeries.close();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public TimeSeriesValue<? extends TimeSeriesDataType> next() {
      return iterator.next();
    }

    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return NumericType.TYPE;
    }
    
  }
}
