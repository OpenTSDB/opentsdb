// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
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
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;

/**
 * A factory to spit out summarizers.
 * 
 * @since 3.0
 */
public class SummarizerFactory extends BaseQueryNodeFactory<SummarizerConfig, Summarizer> {

  public static final String TYPE = "Summarizer";
  
  /**
   * Default ctor for the plugin. All numeric types will return the
   * numeric iterator.
   */
  public SummarizerFactory() {
    super();
    registerIteratorFactory(NumericType.TYPE, new NumericIteratorFactory());
    registerIteratorFactory(NumericArrayType.TYPE, new NumericIteratorFactory());
    registerIteratorFactory(NumericSummaryType.TYPE, new NumericIteratorFactory());
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
  public Summarizer newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Summarizer newNode(final QueryPipelineContext context,
                           final SummarizerConfig config) {
    return new Summarizer(this, context, config);
  }

  @Override
  public SummarizerConfig parseConfig(final ObjectMapper mapper,
                                     final TSDB tsdb,
                                     final JsonNode node) {
    try {
      return mapper.treeToValue(node, SummarizerConfig.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to parse Json.", e);
    }
  }

  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final SummarizerConfig config,
                         final QueryPlanner plan) {
    boolean pass_through = false;
    final Map<String, String> sink_filters = ((DefaultQueryPlanner) plan).sinkFilters();
    for (final QueryNodeConfig successor : plan.configGraph().successors(config)) {
      if (sink_filters.containsKey(successor.getId())) {
        sink_filters.remove(successor.getId());
        pass_through = true;
      }
    }
    
    if (pass_through) {
      SummarizerConfig new_config = config.toBuilder()
          .setPassThrough(true)
          .build();
      plan.replace(config, new_config);
      
    }
  }

  /**
   * The default numeric iterator factory.
   */
  protected class NumericIteratorFactory implements QueryIteratorFactory<Summarizer, NumericSummaryType> {

    @Override
    public TypedTimeSeriesIterator newIterator(final Summarizer node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new SummarizerNonPassthroughNumericIterator(node, result, 
          sources.iterator().next());
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final Summarizer node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new SummarizerNonPassthroughNumericIterator(node, result, 
          sources.values().iterator().next());
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return Lists.newArrayList(NumericSummaryType.TYPE);
    }
        
  }
  
}
