// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.timedifference;

import java.util.Collection;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.BaseQueryNodeFactory;

/**
 * Factory for the time difference function. Straight forward.
 * 
 * @since 3.0
 */
public class TimeDifferenceFactory extends BaseQueryNodeFactory<TimeDifferenceConfig, TimeDifference> {

  public static final String TYPE = "TimeDifference";

  public TimeDifferenceFactory() {
    super();
    registerIteratorFactory(NumericType.TYPE, 
        new NumericIteratorFactory());
    registerIteratorFactory(NumericSummaryType.TYPE, 
        new NumericSummaryIteratorFactory());
    registerIteratorFactory(NumericArrayType.TYPE, 
        new NumericArrayIteratorFactory());
  }
  
  @Override
  public TimeDifference newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TimeDifference newNode(final QueryPipelineContext context, 
                                final TimeDifferenceConfig config) {
    return new TimeDifference(this, context, config);
  }
  
  @Override
  public TimeDifferenceConfig parseConfig(final ObjectMapper mapper, 
                                          final TSDB tsdb, 
                                          final JsonNode node) {
    return TimeDifferenceConfig.parse(mapper, tsdb, node);
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }
 
  /**
   * The default numeric iterator factory.
   */
  protected class NumericIteratorFactory implements QueryIteratorFactory<TimeDifference, NumericType> {

    @Override
    public TypedTimeSeriesIterator newIterator(final TimeDifference node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new TimeDifferenceNumericIterator(node, result, sources);
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final TimeDifference node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new TimeDifferenceNumericIterator(node, result, sources);
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return NumericType.SINGLE_LIST;
    }
    
  }
  
  /**
   * Factory for summary iterators.
   */
  protected class NumericSummaryIteratorFactory implements QueryIteratorFactory<TimeDifference, NumericSummaryType> {

    @Override
    public TypedTimeSeriesIterator newIterator(final TimeDifference node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new TimeDifferenceNumericSummaryIterator(node, result, sources);
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final TimeDifference node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new TimeDifferenceNumericSummaryIterator(node, result, sources);
    }
    
    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return NumericSummaryType.SINGLE_LIST;
    }
  }

  /**
   * Handles array numerics.
   */
  protected class NumericArrayIteratorFactory implements QueryIteratorFactory<TimeDifference, NumericArrayType> {

    @Override
    public TypedTimeSeriesIterator newIterator(final TimeDifference node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new TimeDifferenceNumericArrayIterator(node, result, sources);
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final TimeDifference node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new TimeDifferenceNumericArrayIterator(node, result, sources);
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return NumericArrayType.SINGLE_LIST;
    }
    
  }

}
