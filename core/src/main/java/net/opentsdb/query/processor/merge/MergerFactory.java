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
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;

/**
 * Factory for creating Merger iterators, aggregating multiple time series into
 * one.
 * 
 * @since 3.0
 */
public class MergerFactory extends BaseQueryNodeFactory {
  
  public static final String ID = "Merger";
  
  /**
   * Default ctor. Registers the numeric iterator.
   */
  public MergerFactory() {
    super(ID);
    registerIteratorFactory(NumericType.TYPE, 
        new NumericIteratorFactory());
    registerIteratorFactory(NumericSummaryType.TYPE, 
        new NumericSummaryIteratorFactory());
    registerIteratorFactory(NumericArrayType.TYPE, 
        new NumericArrayIteratorFactory());
  }
  
  @Override
  public QueryNode newNode(final QueryPipelineContext context,
                           final String id,
                           final QueryNodeConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    return new Merger(this, context, id, (MergerConfig) config);
  }
  
  @Override
  public QueryNode newNode(QueryPipelineContext context, String id) {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public QueryNodeConfig parseConfig(final ObjectMapper mapper, 
                                     final TSDB tsdb,
                                     final JsonNode node) {
    try {
      return mapper.treeToValue(node, MergerConfig.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse config", e);
    }
  }
  
  @Override
  public void setupGraph(final TimeSeriesQuery query, 
                         final QueryNodeConfig config, 
                         final QueryPlanner plan) {
    // TODO Auto-generated method stub
  }
  
  /**
   * The default numeric iterator factory.
   */
  protected class NumericIteratorFactory implements QueryIteratorFactory {

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new MergerNumericIterator(node, result, sources);
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new MergerNumericIterator(node, result, sources);
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericType.TYPE);
    }
    
  }

  /**
   * Factory for summary iterators.
   */
  protected class NumericSummaryIteratorFactory implements QueryIteratorFactory {

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new MergerNumericSummaryIterator(node, result, sources);
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new MergerNumericSummaryIterator(node, result, sources);
    }
    
    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericSummaryType.TYPE);
    }
  }

  /**
   * Handles array numerics.
   */
  protected class NumericArrayIteratorFactory implements QueryIteratorFactory {

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new MergerNumericArrayIterator(node, result, sources);
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new MergerNumericArrayIterator(node, result, sources);
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericArrayType.TYPE);
    }
    
  }
  
}
