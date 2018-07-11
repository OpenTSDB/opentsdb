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
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.processor.BaseMultiQueryNodeFactory;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;

/**
 * A factory used to instantiate expression nodes in the graph. This 
 * config will modify the list of nodes so make sure to pass in a copy 
 * of those given by the user.
 * 
 * TODO - we can walk the nodes and look for duplicates. It's a pain to
 * do though.
 * 
 * @since 3.0
 */
public class ExpressionFactory extends BaseMultiQueryNodeFactory {
  
  /**
   * Required empty ctor.
   */
  public ExpressionFactory() {
    super("expression");
    registerIteratorFactory(NumericType.TYPE, new NumericIteratorFactory());
    registerIteratorFactory(NumericSummaryType.TYPE, 
        new NumericSummaryIteratorFactory());
  }

  @Override
  public Collection<QueryNode> newNodes(final QueryPipelineContext context, 
                                        final String id,
                                        final QueryNodeConfig config, 
                                        final List<ExecutionGraphNode> nodes) {
    
    final ExpressionConfig c = (ExpressionConfig) config;
    final ExpressionParser parser = 
        new ExpressionParser(c.getExpression(), id);
    final List<ExpressionParseNode> configs = parser.parse();
    final List<QueryNode> query_nodes = 
        Lists.newArrayListWithExpectedSize(configs.size());
    
    for (final ExpressionParseNode parse_node : configs) {
      final BinaryExpressionNode node =
          new BinaryExpressionNode(this, context, parse_node.getId(), 
          (ExpressionConfig) config, parse_node);
      query_nodes.add(node);
      
      final ExecutionGraphNode.Builder builder = ExecutionGraphNode
          .newBuilder()
            .setConfig(parse_node)
            .setId(parse_node.getId());
      if (parse_node.leftType() == OperandType.SUB_EXP || 
          parse_node.leftType() == OperandType.VARIABLE) {
        builder.addSource((String) parse_node.left());
      }
      if (parse_node.rightType() == OperandType.SUB_EXP || 
          parse_node.rightType() == OperandType.VARIABLE) {
        builder.addSource((String) parse_node.right());
      }
      
      nodes.add(builder.build());
    }
    return query_nodes;
  }

  @Override
  public Class<? extends QueryNodeConfig> nodeConfigClass() {
    return ExpressionConfig.class;
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
