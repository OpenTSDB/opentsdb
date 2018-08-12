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
package net.opentsdb.query.processor.slidingwindow;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.BaseQueryNodeFactory;

/**
 * A factory to generate sliding window nodes.
 * 
 * @since 3.0
 */
public class SlidingWindowFactory extends BaseQueryNodeFactory {

  /**
   * Default plugin ctor.
   */
  public SlidingWindowFactory() {
    super("slidingwindow");
    registerIteratorFactory(NumericType.TYPE, 
        new NumericIteratorFactory());
    registerIteratorFactory(NumericSummaryType.TYPE, 
        new NumericSummaryIteratorFactory());
  }
  
  /**
   * Optional ctor.
   * @param id A non-null and non-empty ID.
   */
  public SlidingWindowFactory(final String id) {
    super(id);
    registerIteratorFactory(NumericType.TYPE, 
        new NumericIteratorFactory());
    registerIteratorFactory(NumericSummaryType.TYPE, 
        new NumericSummaryIteratorFactory());
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id) {
    return new SlidingWindow(this, context, id, null);
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id,
                           final QueryNodeConfig config) {
    return new SlidingWindow(this, context, id, (SlidingWindowConfig) config);
  }
  
  @Override
  public QueryNodeConfig parseConfig(final ObjectMapper mapper, 
                                     final TSDB tsdb,
                                     final JsonNode node) {
    try {
      return mapper.treeToValue(node, SlidingWindowConfig.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to parse config", e);
    }
  }
  
  /**
   * The default numeric iterator factory.
   */
  protected class NumericIteratorFactory implements QueryIteratorFactory {

    @Override
    public Iterator<TimeSeriesValue<?>> newIterator(final QueryNode node,
                                                    final QueryResult result,
                                                    final Collection<TimeSeries> sources) {
      return new SlidingWindowNumericIterator(node, result, sources);
    }

    @Override
    public Iterator<TimeSeriesValue<?>> newIterator(final QueryNode node,
                                                    final QueryResult result,
                                                    final Map<String, TimeSeries> sources) {
      return new SlidingWindowNumericIterator(node, result, sources);
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericType.TYPE);
    }
    
  }
  
  /**
   * The default numeric summary iterator factory.
   */
  protected class NumericSummaryIteratorFactory implements QueryIteratorFactory {

    @Override
    public Iterator<TimeSeriesValue<?>> newIterator(final QueryNode node,
                                                    final QueryResult result,
                                                    final Collection<TimeSeries> sources) {
      return new SlidingWindowNumericSummaryIterator(node, result, sources);
    }

    @Override
    public Iterator<TimeSeriesValue<?>> newIterator(final QueryNode node,
                                                    final QueryResult result,
                                                    final Map<String, TimeSeries> sources) {
      return new SlidingWindowNumericSummaryIterator(node, result, sources);
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericSummaryType.TYPE);
    }
    
  }
}
