// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.summarizer;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.query.QueryIterator;

/**
 * A time series that travels to pass-through iterators and is updated as
 * the iteration occurs on the source.
 * 
 * @since 3.0
 */
public class SummarizedTimeSeries implements TimeSeries {
  public static final TimeStamp ZERO_TS = new SecondTimeStamp(0);
  
  /** The pass through result. */
  protected final SummarizerPassThroughResult result;
  
  /** The non-null source. */
  protected final TimeSeries source;
  
  /** The summary updated by the pass through iterator. */
  protected final MutableNumericSummaryValue summary;
  
  /**
   * The package private ctor.
   * @param result The non-null result.
   * @param source The non-null source.
   */
  SummarizedTimeSeries(final SummarizerPassThroughResult result, 
                       final TimeSeries source) {
    this.result = result;
    this.source = source;
    summary = new MutableNumericSummaryValue();
  }
  
  @Override
  public TimeSeriesId id() {
    return source.id();
  }
  
  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      final TypeToken<? extends TimeSeriesDataType> type) {
    if (type == NumericSummaryType.TYPE) {
      final TypedTimeSeriesIterator iterator = new SummarizedIterator();
      return Optional.of(iterator);
    }
    return Optional.empty();
  }
  
  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators =
        Lists.newArrayListWithCapacity(1);
    iterators.add(new SummarizedIterator());
    return iterators;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return Lists.newArrayList(NumericSummaryType.TYPE);
  }

  @Override
  public void close() {
    source.close();
  }
  
  void fillEmpty() {
    summary.resetNull(ZERO_TS);
  }
  
  void summarize(final long[] values, int offset, int end) {
    final MutableNumericValue number = new MutableNumericValue();
    for (Entry<String, NumericAggregator> entry : 
      ((Summarizer) result.summarizerNode()).aggregators().entrySet()) {
      entry.getValue().run(values, offset, end, number);
      summary.resetValue(result.rollupConfig()
          .getIdForAggregator(entry.getKey()), number.value());
    }
  }
  
  void summarize(final double[] values, int offset, int end) {
    final MutableNumericValue number = new MutableNumericValue();
    for (Entry<String, NumericAggregator> entry : 
      ((Summarizer) result.summarizerNode()).aggregators().entrySet()) {
      entry.getValue().run(values, offset, end, 
          ((SummarizerConfig) result.summarizerNode().config()).getInfectiousNan(), number);
      summary.resetValue(result.rollupConfig()
          .getIdForAggregator(entry.getKey()), number.value());
    }
  }
  
  /**
   * Simple iterator that just returns the summary value.
   */
  class SummarizedIterator implements QueryIterator {
    boolean has_next = true;
    
    @Override
    public boolean hasNext() {
      if (has_next) {
        has_next = false;
        return true;
      }
      return false;
    }
    
    @Override
    public TimeSeriesValue<? extends TimeSeriesDataType> next() {
      return summary;
    }
    
    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return NumericSummaryType.TYPE;
    }
    
  }
}
