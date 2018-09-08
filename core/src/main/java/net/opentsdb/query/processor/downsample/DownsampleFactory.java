// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.downsample;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph.CycleFoundException;
import org.jgrapht.graph.DefaultEdge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.processor.BaseQueryNodeFactory;

/**
 * Simple class for generating Downsample processors.
 * 
 * @since 3.0
 */
public class DownsampleFactory extends BaseQueryNodeFactory {

  /**
   * Default ctor.
   */
  public DownsampleFactory() {
    super("downsample");
    registerIteratorFactory(NumericType.TYPE, new NumericIteratorFactory());
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
    return new Downsample(this, context, id, (DownsampleConfig) config);
  }
  
  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id) {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public QueryNodeConfig parseConfig(final ObjectMapper mapper,
                                     final TSDB tsdb,
                                     final JsonNode node) {
    return DownsampleConfig.parse(mapper, tsdb, node);
  }
  
  @Override
  public void setupGraph(
      final TimeSeriesQuery query, 
      final ExecutionGraphNode config, 
      final DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> graph) {
    // For downsampling we need to set the config start and end times
    // to the query start and end times. The config will then align them.
    DownsampleConfig.Builder builder = DownsampleConfig
        .newBuilder((DownsampleConfig) config.getConfig())
        .setStart(query.getStart())
        .setEnd(query.getEnd())
        .setId(config.getId());
    
    ExecutionGraphNode node = ExecutionGraphNode.newBuilder(config)
        .setConfig(builder.build())
        .build();
    ExecutionGraph.replace(config, node, graph);
  }
  
  /**
   * The default numeric iterator factory.
   */
  protected class NumericIteratorFactory implements QueryIteratorFactory {

    @Override
    public Iterator<TimeSeriesValue<?>> newIterator(final QueryNode node,
                                                    final QueryResult result,
                                                    final Collection<TimeSeries> sources) {
      return new DownsampleNumericIterator(node, result, sources.iterator().next());
    }

    @Override
    public Iterator<TimeSeriesValue<?>> newIterator(final QueryNode node,
                                                    final QueryResult result,
                                                    final Map<String, TimeSeries> sources) {
      return new DownsampleNumericIterator(node, result, sources.values().iterator().next());
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericType.TYPE);
    }
        
  }

  /**
   * Handles summaries.
   */
  protected class NumericSummaryIteratorFactory implements QueryIteratorFactory {

    @Override
    public Iterator<TimeSeriesValue<?>> newIterator(final QueryNode node,
                                                    final QueryResult result,
                                                    final Collection<TimeSeries> sources) {
      return new DownsampleNumericSummaryIterator(node, result, sources.iterator().next());
    }

    @Override
    public Iterator<TimeSeriesValue<?>> newIterator(final QueryNode node,
                                                    final QueryResult result,
                                                    final Map<String, TimeSeries> sources) {
      return new DownsampleNumericSummaryIterator(node, result, sources.values().iterator().next());
    }
    
    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericSummaryType.TYPE);
    }
  }
  
  /**
   * Handles arrays.
   */
  protected class NumericArrayIteratorFactory implements QueryIteratorFactory {

    @Override
    public Iterator<TimeSeriesValue<?>> newIterator(final QueryNode node,
                                                    final QueryResult result,
                                                    final Collection<TimeSeries> sources) {
      return new DownsampleNumericArrayIterator(node, result, sources.iterator().next());
    }

    @Override
    public Iterator<TimeSeriesValue<?>> newIterator(final QueryNode node,
                                                    final QueryResult result,
                                                    final Map<String, TimeSeries> sources) {
      return new DownsampleNumericArrayIterator(node, result, sources.values().iterator().next());
    }
    
    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericArrayType.TYPE);
    }
  }
  
}
