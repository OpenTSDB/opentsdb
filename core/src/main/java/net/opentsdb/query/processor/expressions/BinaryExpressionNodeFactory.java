//This file is part of OpenTSDB.
//Copyright (C) 2018  The OpenTSDB Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
package net.opentsdb.query.processor.expressions;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

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
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.processor.BaseQueryNodeFactory;

/**
 * Returns a node and iterators for a binary expression (usually created
 * from an ExpressionConfig and factory).
 * 
 * @since 3.0
 */
public class BinaryExpressionNodeFactory extends BaseQueryNodeFactory {

  /**
   * Required empty ctor.
   */
  public BinaryExpressionNodeFactory() {
    super("binaryexpression");
    registerIteratorFactory(NumericType.TYPE, new NumericIteratorFactory());
    registerIteratorFactory(NumericSummaryType.TYPE, 
        new NumericSummaryIteratorFactory());
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id) {
    throw new UnsupportedOperationException("Default configs are not "
        + "allowed for Binary Expressions.");
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id,
                           final QueryNodeConfig config) {
    return new BinaryExpressionNode(this, context, id, (ExpressionParseNode) config);
  }

  @Override
  public QueryNodeConfig parseConfig(ObjectMapper mapper, TSDB tsdb,
      JsonNode node) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setupGraph(
      final TimeSeriesQuery query, 
      final ExecutionGraphNode config,
      final DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> graph) {
    // nothing to do here.
  }

  /**
   * The default numeric iterator factory.
   */
  protected class NumericIteratorFactory implements QueryIteratorFactory {

    @Override
    public Iterator<TimeSeriesValue<?>> newIterator(final QueryNode node,
                                                    final QueryResult result,
                                                    final Collection<TimeSeries> sources) {
      throw new UnsupportedOperationException("Expression iterators must have a map.");
    }

    @Override
    public Iterator<TimeSeriesValue<?>> newIterator(final QueryNode node,
                                                    final QueryResult result,
                                                    final Map<String, TimeSeries> sources) {
      return new ExpressionNumericIterator(node, result, sources);
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
      throw new UnsupportedOperationException("Expression iterators must have a map.");
    }

    @Override
    public Iterator<TimeSeriesValue<?>> newIterator(final QueryNode node,
                                                    final QueryResult result,
                                                    final Map<String, TimeSeries> sources) {
      return new ExpressionNumericSummaryIterator(node, result, sources);
    }
    
    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericSummaryType.TYPE);
    }
  }
}
