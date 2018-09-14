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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph.CycleFoundException;
import org.jgrapht.graph.DefaultEdge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.processor.BaseMultiQueryNodeFactory;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;

/**
 * A factory used to instantiate expression nodes in the graph. This 
 * config will modify the list of nodes so make sure to pass in a copy 
 * of those given by the user.
 * 
 * NOTE: This factory will not generate nodes. Instead it will mutate the
 * execution graph with {@link ExpressionParseNode} entries when 
 * {@link #setupGraph(TimeSeriesQuery, ExecutionGraphNode, DirectedAcyclicGraph)} 
 * is called.
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
  }

  @Override
  public Collection<QueryNode> newNodes(final QueryPipelineContext context, 
                                        final String id,
                                        final QueryNodeConfig config, 
                                        final List<ExecutionGraphNode> nodes) {
    return Collections.emptyList();
  }
  
  @Override
  public QueryNodeConfig parseConfig(final ObjectMapper mapper, 
                                     final TSDB tsdb,
                                     final JsonNode node) {
    return ExpressionConfig.parse(mapper, tsdb, node);
  }
  
  @Override
  public void setupGraph(
      final TimeSeriesQuery query, 
      final ExecutionGraphNode config, 
      final DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> graph) {
    
    // parse the expression
    final ExpressionConfig c = (ExpressionConfig) config.getConfig();
    final ExpressionParser parser = new ExpressionParser(c);
    final List<ExpressionParseNode> configs = parser.parse();
    
    final Map<String, ExecutionGraphNode> node_map = Maps.newHashMap();
    for (final ExecutionGraphNode node: graph.vertexSet()) {
      node_map.put(node.getId(), node);
    }
    
    final List<ExecutionGraphNode> new_nodes = 
        Lists.newArrayListWithCapacity(configs.size());
    for (final ExpressionParseNode parse_node : configs) {
      final ExecutionGraphNode.Builder builder = ExecutionGraphNode
          .newBuilder()
            .setConfig(parse_node)
            .setId(parse_node.getId())
            .setType("BinaryExpression");
      if (parse_node.leftType() == OperandType.SUB_EXP) {
        builder.addSource((String) parse_node.left());
      }
      if (parse_node.rightType() == OperandType.SUB_EXP) {
        builder.addSource((String) parse_node.right());
      }
      
      // we may need to fix up variable names. Do so by searching 
      // recursively.
      String last_source = null;
      if (parse_node.leftType() == OperandType.VARIABLE) {
        String ds = validate(parse_node, true, graph, config, 0);
        if (ds != null) {
          builder.addSource(ds);
          last_source = ds;
        } else {
          throw new RuntimeException("WTF? No node for left?");
        }
      }
      
      if (parse_node.rightType() == OperandType.VARIABLE) {
        String ds = validate(parse_node, false, graph, config, 0);
        if (ds != null) {
          // dedupe
          if (last_source == null || !last_source.equals(ds)) {
            builder.addSource(ds);
          }
        } else {
          throw new RuntimeException("WTF? No node for right?");
        }
      }
      
      final ExecutionGraphNode new_node = builder.build();
      new_nodes.add(new_node);
      node_map.put(parse_node.getId(), new_node);
    }
    
    // remove the old config and get the in and outgoing edges.
    final List<ExecutionGraphNode> upstream = Lists.newArrayList();
    for (final DefaultEdge up : graph.incomingEdgesOf(config)) {
      final ExecutionGraphNode n = graph.getEdgeSource(up);
      upstream.add(n);
    }
    for (final ExecutionGraphNode n : upstream) {
      graph.removeEdge(n, config);
    }
    
    final List<ExecutionGraphNode> downstream = Lists.newArrayList();
    for (final DefaultEdge down : graph.outgoingEdgesOf(config)) {
      final ExecutionGraphNode n = graph.getEdgeTarget(down);
      downstream.add(n);
    }
    for (final ExecutionGraphNode n : downstream) {
      graph.removeEdge(config, n);
    }
    
    // now yank ourselves out and link.
    graph.removeVertex(config);
    for (final ExecutionGraphNode node : new_nodes) {
      graph.addVertex(node);
      for (final String source : node.getSources()) {
        try {
          graph.addDagEdge(node, node_map.get(source));
        } catch (CycleFoundException e) {
          throw new IllegalStateException("Cylcle found when generating "
              + "sub expression graph.", e);
        }
      }
    }
    
    for (final ExecutionGraphNode up : upstream) {
      try {graph.addDagEdge(up, new_nodes.get(new_nodes.size() - 1));
      } catch (CycleFoundException e) {
        throw new IllegalStateException("Cylcle found when generating "
            + "sub expression graph.", e);
      }
    }
  }
  
  static String validate(final ExpressionParseNode node, 
                         final boolean left, 
                         final DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> graph, 
                         final ExecutionGraphNode downstream, 
                         final int depth) {
    final String key = left ? (String) node.left() : (String) node.right();
    if (depth > 0 && 
        !Strings.isNullOrEmpty(downstream.getType()) && 
        downstream.getType().toLowerCase().equals("expression")) {
      if (left && key.equals(downstream.getId())) {
        if (downstream.getConfig() instanceof ExpressionConfig) {
          node.setLeft(((ExpressionConfig) downstream.getConfig()).getAs());
        } else {
          node.setLeft(((ExpressionParseNode) downstream.getConfig()).as());
        }
        return downstream.getId();
      } else if (key.equals(downstream.getId())) {
        if (downstream.getConfig() instanceof ExpressionConfig) {
          node.setRight(((ExpressionConfig) downstream.getConfig()).getAs());
        } else {
          node.setRight(((ExpressionParseNode) downstream.getConfig()).as());
        }
        return downstream.getId();
      }
    } else if (!Strings.isNullOrEmpty(downstream.getType()) && 
        downstream.getType().toLowerCase().equals("datasource")) {
      if (left && key.equals(downstream.getId())) {
        // TODO - cleanup the filter checks as it may be a regex or something else!!!
        node.setLeft(((QuerySourceConfig) downstream.getConfig()).getMetric().getMetric());
        return downstream.getId();
      } else if (left && 
          key.equals(((QuerySourceConfig) downstream.getConfig()).getMetric().getMetric())) {
        return downstream.getId();
        // right
      } else if (key.equals(downstream.getId())) {
        node.setRight(((QuerySourceConfig) downstream.getConfig()).getMetric().getMetric());
        return downstream.getId();
      } else if (key.equals(((QuerySourceConfig) downstream.getConfig()).getMetric().getMetric())) {
        return downstream.getId();
      }
    }
    
    for (final DefaultEdge edge : graph.outgoingEdgesOf(downstream)) {
      final ExecutionGraphNode graph_node = graph.getEdgeTarget(edge);
      final String ds = validate(node, left, graph, graph_node, depth + 1);
      if (ds != null) {
        if (depth == 0) {
          return ds;
        }
        return downstream.getId();
      }
    }
    
    return null;
  }
  
}
