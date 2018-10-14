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
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;

/**
 * A factory used to instantiate expression nodes in the graph. This 
 * config will modify the list of nodes so make sure to pass in a copy 
 * of those given by the user.
 * 
 * NOTE: This factory will not generate nodes. Instead it will mutate the
 * execution graph with {@link ExpressionParseNode} entries when 
 * {@link #setupGraph(TimeSeriesQuery, QueryNodeConfig, DirectedAcyclicGraph)} 
 * is called.
 * 
 * TODO - we can walk the nodes and look for duplicates. It's a pain to
 * do though.
 * 
 * @since 3.0
 */
public class ExpressionFactory extends BaseQueryNodeFactory {
  
  public static final String ID = "Expression";
  
  /**
   * Required empty ctor.
   */
  public ExpressionFactory() {
    super(ID);
  }
  
  @Override
  public QueryNodeConfig parseConfig(final ObjectMapper mapper, 
                                     final TSDB tsdb,
                                     final JsonNode node) {
    return ExpressionConfig.parse(mapper, tsdb, node);
  }
  
  @Override
  public void setupGraph(final TimeSeriesQuery query, 
                         final QueryNodeConfig config, 
                         final QueryPlanner plan) {
    
    // parse the expression
    final ExpressionConfig c = (ExpressionConfig) config;
    final ExpressionParser parser = new ExpressionParser(c);
    final List<ExpressionParseNode> configs = parser.parse();
    
    final Map<String, QueryNodeConfig> node_map = Maps.newHashMap();
    for (final QueryNodeConfig node: plan.configGraph().vertexSet()) {
      node_map.put(node.getId(), node);
    }
    
    final List<QueryNodeConfig> new_nodes = 
        Lists.newArrayListWithCapacity(configs.size());
    for (final ExpressionParseNode parse_node : configs) {
      if (parse_node.getLeftType() == OperandType.SUB_EXP) {
        parse_node.addSource((String) parse_node.getLeft());
      }
      if (parse_node.getRightType() == OperandType.SUB_EXP) {
        parse_node.addSource((String) parse_node.getRight());
      }
      
      // we may need to fix up variable names. Do so by searching 
      // recursively.
      String last_source = null;
      if (parse_node.getLeftType() == OperandType.VARIABLE) {
        String ds = validate(parse_node, true, plan, config, 0);
        if (ds != null) {
          parse_node.addSource(ds);
          last_source = ds;
        } else {
          throw new RuntimeException("WTF? No node for left?");
        }
      }
      
      if (parse_node.getRightType() == OperandType.VARIABLE) {
        String ds = validate(parse_node, false, plan, config, 0);
        if (ds != null) {
          // dedupe
          if (last_source == null || !last_source.equals(ds)) {
            parse_node.addSource(ds);
          }
        } else {
          throw new RuntimeException("WTF? No node for right?");
        }
      }
      
      new_nodes.add(parse_node);
      node_map.put(parse_node.getId(), parse_node);
    }
    
    // remove the old config and get the in and outgoing edges.
    final List<QueryNodeConfig> upstream = Lists.newArrayList();
    for (final DefaultEdge up : plan.configGraph().incomingEdgesOf(config)) {
      final QueryNodeConfig n = plan.configGraph().getEdgeSource(up);
      upstream.add(n);
    }
    for (final QueryNodeConfig n : upstream) {
      plan.configGraph().removeEdge(n, config);
    }
    
    final List<QueryNodeConfig> downstream = Lists.newArrayList();
    for (final DefaultEdge down : plan.configGraph().outgoingEdgesOf(config)) {
      final QueryNodeConfig n = plan.configGraph().getEdgeTarget(down);
      downstream.add(n);
    }
    for (final QueryNodeConfig n : downstream) {
      plan.configGraph().removeEdge(config, n);
    }
    
    // now yank ourselves out and link.
    plan.configGraph().removeVertex(config);
    System.out.println("REMOVED: " + config);
    for (final QueryNodeConfig node : new_nodes) {
      plan.configGraph().addVertex(node);
      for (final String source : node.getSources()) {
        try {
          plan.configGraph().addDagEdge(node, node_map.get(source));
        } catch (CycleFoundException e) {
          throw new IllegalStateException("Cylcle found when generating "
              + "sub expression graph.", e);
        }
      }
    }
    
    for (final QueryNodeConfig up : upstream) {
      try {plan.configGraph().addDagEdge(up, new_nodes.get(new_nodes.size() - 1));
      } catch (CycleFoundException e) {
        throw new IllegalStateException("Cylcle found when generating "
            + "sub expression graph.", e);
      }
    }
  }
  
  static String validate(final ExpressionParseNode node, 
                         final boolean left, 
                         final QueryPlanner plan,
                         final QueryNodeConfig downstream, 
                         final int depth) {
    final String key = left ? (String) node.getLeft() : (String) node.getRight();
    if (depth > 0 && 
        !Strings.isNullOrEmpty(downstream.getType()) && 
        downstream.getType().toLowerCase().equals("expression")) {
      if (left && key.equals(downstream.getId())) {
        if (downstream instanceof ExpressionConfig) {
          node.setLeft(((ExpressionConfig) downstream).getAs());
        } else {
          node.setLeft(((ExpressionParseNode) downstream).getAs());
        }
        return downstream.getId();
      } else if (key.equals(downstream.getId())) {
        if (downstream instanceof ExpressionConfig) {
          node.setRight(((ExpressionConfig) downstream).getAs());
        } else {
          node.setRight(((ExpressionParseNode) downstream).getAs());
        }
        return downstream.getId();
      }
    } else if (!Strings.isNullOrEmpty(downstream.getType()) && 
        downstream.getType().toLowerCase().equals("datasource")) {
      if (left && key.equals(downstream.getId())) {
        // TODO - cleanup the filter checks as it may be a regex or something else!!!
        node.setLeft(((QuerySourceConfig) downstream).getMetric().getMetric());
        return downstream.getId();
      } else if (left && 
          key.equals(((QuerySourceConfig) downstream).getMetric().getMetric())) {
        return downstream.getId();
        // right
      } else if (key.equals(downstream.getId())) {
        node.setRight(((QuerySourceConfig) downstream).getMetric().getMetric());
        return downstream.getId();
      } else if (key.equals(((QuerySourceConfig) downstream).getMetric().getMetric())) {
        return downstream.getId();
      }
    }
    
    for (final DefaultEdge edge : plan.configGraph().outgoingEdgesOf(downstream)) {
      final QueryNodeConfig graph_node = plan.configGraph().getEdgeTarget(edge);
      final String ds = validate(node, left, plan, graph_node, depth + 1);
      if (ds != null) {
        if (depth == 0) {
          return ds;
        }
        return downstream.getId();
      }
    }
    
    return null;
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id) {
    throw new UnsupportedOperationException("This node should have been "
        + "removed from the graph.");
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id,
                           final QueryNodeConfig config) {
    throw new UnsupportedOperationException("This node should have been "
        + "removed from the graph.");
  }
  
}
