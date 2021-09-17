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

import java.util.Collection;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;

/**
 * Factory for creating Merger iterators, aggregating multiple time series into
 * one.
 * 
 * @since 3.0
 */
public class MergerFactory extends BaseQueryNodeFactory<MergerConfig, Merger> {
  
  public static final String TYPE = "Merger";
  
  /**
   * Default ctor. Registers the numeric iterator.
   */
  public MergerFactory() {
    super();
    registerIteratorFactory(NumericType.TYPE, 
        new NumericIteratorFactory());
    registerIteratorFactory(NumericSummaryType.TYPE, 
        new NumericSummaryIteratorFactory());
    registerIteratorFactory(NumericArrayType.TYPE, 
        new NumericArrayIteratorFactory());
  }
  
  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    return Deferred.fromResult(null);
  }
  
  @Override
  public Merger newNode(final QueryPipelineContext context,
                        final MergerConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    return new Merger(this, context, (MergerConfig) config);
  }
  
  @Override
  public Merger newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public MergerConfig parseConfig(final ObjectMapper mapper,
                                  final TSDB tsdb,
                                  final JsonNode node) {
    try {
      return mapper.treeToValue(node, MergerConfig.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse config", e);
    }
  }
  
  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final MergerConfig config,
                         final QueryPlanner plan) {
    // no-op
  }

  /**
   * The default numeric iterator factory.
   */
  protected class NumericIteratorFactory implements QueryIteratorFactory<Merger, NumericType> {

    @Override
    public TypedTimeSeriesIterator newIterator(final Merger node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      switch (((MergerConfig) node.config()).getMode()) {
        case HA:
          return new MergerNumericIterator(node, result, sources);
        case SPLIT:
          throw new UnsupportedOperationException("TODO");
        default:
          throw new IllegalArgumentException("No implementation for mode: "
                  + ((MergerConfig) node.config()).getMode());
      }
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final Merger node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      switch (((MergerConfig) node.config()).getMode()) {
        case HA:
          return new MergerNumericIterator(node, result, sources);
        case SPLIT:
          throw new UnsupportedOperationException("TODO");
        default:
          throw new IllegalArgumentException("No implementation for mode: "
                  + ((MergerConfig) node.config()).getMode());
      }
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return NumericType.SINGLE_LIST;
    }
    
  }

  /**
   * Factory for summary iterators.
   */
  protected class NumericSummaryIteratorFactory implements QueryIteratorFactory<Merger, NumericSummaryType> {

    @Override
    public TypedTimeSeriesIterator newIterator(final Merger node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      switch (((MergerConfig) node.config()).getMode()) {
        case HA:
          return new MergerNumericSummaryIterator(node, result, sources);
        case SPLIT:
          throw new UnsupportedOperationException("TODO");
        default:
          throw new IllegalArgumentException("No implementation for mode: "
                  + ((MergerConfig) node.config()).getMode());
      }
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final Merger node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      switch (((MergerConfig) node.config()).getMode()) {
        case HA:
          return new MergerNumericSummaryIterator(node, result, sources);
        case SPLIT:
          throw new UnsupportedOperationException("TODO");
        default:
          throw new IllegalArgumentException("No implementation for mode: "
                  + ((MergerConfig) node.config()).getMode());
      }
    }
    
    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return NumericSummaryType.SINGLE_LIST;
    }
  }

  /**
   * Handles array numerics.
   */
  protected class NumericArrayIteratorFactory implements QueryIteratorFactory<Merger, NumericArrayType> {

    @Override
    public TypedTimeSeriesIterator newIterator(final Merger node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      switch (((MergerConfig) node.config()).getMode()) {
        case HA:
          return new MergerNumericArrayIterator(node, result, sources);
        case SPLIT:
          return new SplitNumericArrayIterator(node, result, sources);
        default:
          throw new IllegalArgumentException("No implementation for mode: "
                  + ((MergerConfig) node.config()).getMode());
      }
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final Merger node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      switch (((MergerConfig) node.config()).getMode()) {
        case HA:
          return new MergerNumericArrayIterator(node, result, sources);
        case SPLIT:
          return new SplitNumericArrayIterator(node, result, sources.values());
        default:
          throw new IllegalArgumentException("No implementation for mode: "
                  + ((MergerConfig) node.config()).getMode());
      }
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return NumericArrayType.SINGLE_LIST;
    }
    
  }
  
}
