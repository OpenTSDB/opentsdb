//This file is part of OpenTSDB.
//Copyright (C) 2018-2019  The OpenTSDB Authors.
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.graph.Graphs;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A factory used to instantiate expression nodes in the graph. This 
 * config will modify the list of nodes so make sure to pass in a copy 
 * of those given by the user.
 * 
 * NOTE: This factory will not generate nodes. Instead it will mutate the
 * execution graph with {@link ExpressionParseNode} entries when 
 * {@link #setupGraph(QueryPipelineContext, ExpressionConfig, QueryPlanner )}
 * is called.
 * 
 * TODO - we can walk the nodes and look for duplicates. It's a pain to
 * do though.
 * 
 * @since 3.0
 */
public class ExpressionFactory extends BaseQueryNodeFactory<ExpressionConfig, 
    BinaryExpressionNode> {
  
  public static final String TYPE = "Expression";
  
  /**
   * Required empty ctor.
   */
  public ExpressionFactory() {
    super();
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
  public ExpressionConfig parseConfig(final ObjectMapper mapper,
                                     final TSDB tsdb,
                                     final JsonNode node) {
    return ExpressionConfig.parse(mapper, tsdb, node);
  }
  
  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final ExpressionConfig config,
                         final QueryPlanner plan) {
    // parse the expression
    final ExpressionConfig c = config;
    final ExpressionParser parser = new ExpressionParser(c);
    final List<ExpressionParseNode> configs = parser.parse();
    
    final Map<String, QueryNodeConfig> node_map = Maps.newHashMap();
    for (final QueryNodeConfig node: plan.configGraph().nodes()) {
      node_map.put(node.getId(), node);
    }
    
    final List<QueryNodeConfig> new_nodes = 
        Lists.newArrayListWithCapacity(configs.size());
    for (final ExpressionParseNode parse_node : configs) {
      ExpressionParseNode.Builder builder = parse_node.toBuilder()
          .setSources(Lists.newArrayList());
      if (parse_node.getLeftType() == OperandType.SUB_EXP) {
        builder.addSource((String) parse_node.getLeft());
      }
      if (parse_node.getRightType() == OperandType.SUB_EXP) {
        builder.addSource((String) parse_node.getRight());
      }
      
      // we may need to fix up variable names. Do so by searching 
      // recursively.
      String last_source = null;
      if (parse_node.getLeftType() == OperandType.VARIABLE) {
        String ds = validate(builder, true, plan, config, 0);
        if (ds != null) {
          builder.addSource(ds);
          last_source = ds;
        } else {
          throw new RuntimeException("Failed to find a data source for the "
              + "left operand: " + parse_node.getLeft() + " for expression node: " 
              + config.getId() + ((DefaultQueryPlanner) plan).printConfigGraph());
        }
      }
      
      if (parse_node.getRightType() == OperandType.VARIABLE) {
        String ds = validate(builder, false, plan, config, 0);
        if (ds != null) {
          // dedupe
          if (last_source == null || !last_source.equals(ds)) {
            builder.addSource(ds);
          }
        } else {
          throw new RuntimeException("Failed to find a data source for the "
              + "right operand: " + parse_node.getRight() + " for expression node: " 
              + config.getId() + ((DefaultQueryPlanner) plan).printConfigGraph());
        }
      }
      final ExpressionParseNode rebuilt = 
          (ExpressionParseNode) builder.build();
      new_nodes.add(rebuilt);
      node_map.put(rebuilt.getId(), rebuilt);
    }
    configs.clear();
    
    // remove the old config and get the in and outgoing edges.
    final List<QueryNodeConfig> upstream = Lists.newArrayList();
    for (final QueryNodeConfig n : plan.configGraph().predecessors(config)) {
      upstream.add(n);
    }
    for (final QueryNodeConfig n : upstream) {
      plan.configGraph().removeEdge(n, config);
    }
    
    final List<QueryNodeConfig> downstream = Lists.newArrayList();
    for (final QueryNodeConfig n : plan.configGraph().successors(config)) {
      downstream.add(n);
    }
    for (final QueryNodeConfig n : downstream) {
      plan.configGraph().removeEdge(config, n);
    }
    
    // now yank ourselves out and link.
    plan.configGraph().removeNode(config);
    for (final QueryNodeConfig node : new_nodes) {
      List<String> sources = node.getSources();
      for (final String source : sources) {
        final String src = source.contains(":") ? 
            source.substring(0, source.indexOf(":")) : source;
        plan.configGraph().putEdge(node, node_map.get(src));
        if (Graphs.hasCycle(plan.configGraph())) {
          throw new IllegalStateException("Cylcle found when generating "
              + "sub expression graph for " + node.getId() + " => " 
              + node_map.get(source).getId());
        }
      }
    }
    
    for (final QueryNodeConfig up : upstream) {
      plan.configGraph().putEdge(up, new_nodes.get(new_nodes.size() - 1));
      if (Graphs.hasCycle(plan.configGraph())) {
        throw new IllegalStateException("Cylcle found when generating "
            + "sub expression graph for " + up.getId() + " => " 
            + new_nodes.get(new_nodes.size() - 1).getId());
      }
    }
  }
  
  static String validate(final ExpressionParseNode.Builder builder, 
                         final boolean left, 
                         final QueryPlanner plan,
                         final QueryNodeConfig downstream, 
                         final int depth) {
    final String key = left ? (String) builder.left() : (String) builder.right();
    final String node_id = Strings.isNullOrEmpty(downstream.getType()) ? 
        downstream.getId() : downstream.getType();
    if (depth > 0 && (node_id.toLowerCase().equals("expression") || 
        node_id.toLowerCase().equals("binaryexpression"))) {
      if (left && key.equals(downstream.getId())) {
        if (downstream instanceof ExpressionConfig) {
          builder.setLeft(((ExpressionConfig) downstream).getAs());
        } else {
          builder.setLeft(((ExpressionParseNode) downstream).getAs());
        }
        return downstream.getId();
      } else if (key.equals(downstream.getId())) {
        if (downstream instanceof ExpressionConfig) {
          builder.setRight(((ExpressionConfig) downstream).getAs());
        } else {
          builder.setRight(((ExpressionParseNode) downstream).getAs());
        }
        return downstream.getId();
      }
    } else {
      // node data source first
      for (final QueryNodeConfig successor : plan.configGraph().successors(downstream)) {
        List<String> sources = plan.getDataSourceIds(successor);
        Set<String> metrics = plan.getMetrics(successor);
        if (metrics.size() > 1) {
          throw new IllegalStateException("Whoops too many metrics: " + metrics 
              + " and node " + successor.getId());
        }
        
        String metric = metrics.size() == 1 ? metrics.iterator().next() : null;
        for (final String source : sources) {
          String data_src = source.substring(source.indexOf(":") + 1);
          if (key.equals(data_src) || 
              (metric != null && key.equals(metric))) {
            if (left) {
              builder.setLeft(metric)
                     .setLeftId(source);
            } else {
              builder.setRight(metric)
                     .setRightId(source);
            }
            return source;
          }
        }
      }
    }
    
    for (final QueryNodeConfig graph_node : 
      plan.configGraph().successors(downstream)) {
      final String ds = validate(builder, left, plan, graph_node, depth + 1);
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
  public BinaryExpressionNode newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException("This node should have been "
        + "removed from the graph.");
  }

  @Override
  public BinaryExpressionNode newNode(final QueryPipelineContext context,
                           final ExpressionConfig config) {
    throw new UnsupportedOperationException("This node should have been "
        + "removed from the graph.");
  }
  
}
