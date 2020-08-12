//This file is part of OpenTSDB.
//Copyright (C) 2018-2020  The OpenTSDB Authors.
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
import com.google.common.collect.Sets;
import com.google.common.graph.Graphs;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(ExpressionFactory.class);
  
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
    ExpressionConfig c = config;
    final ExpressionParser parser = new ExpressionParser(c);
    final List<ExpressionParseNode> configs = parser.parse();
    
    final Map<String, QueryNodeConfig> node_map = Maps.newHashMap();
    for (final QueryNodeConfig node: plan.configGraph().successors(config)) {
      node_map.put(node.getId(), node);
    }
    
    final List<QueryNodeConfig> new_nodes = 
        Lists.newArrayListWithCapacity(configs.size());
    int variables = 0;
    for (final ExpressionParseNode parse_node : configs) {
      ExpressionParseNode.Builder builder = parse_node.toBuilder()
          .addResultId(new DefaultQueryResultId(parse_node.getId(), parse_node.getId()))
          .setSources(Lists.newArrayList());
      if (parse_node.getLeftType() == OperandType.SUB_EXP) {
        builder.addSource((String) parse_node.getLeft())
        .setLeftId(new DefaultQueryResultId((String) parse_node.getLeft(), (String) parse_node.getLeft()));
      }
      if (parse_node.getRightType() == OperandType.SUB_EXP) {
        builder.addSource((String) parse_node.getRight())
          .setRightId(new DefaultQueryResultId((String) parse_node.getRight(), (String) parse_node.getRight()));
      }
      
      // we may need to fix up variable names. Do so by searching 
      // recursively.
      if (parse_node.getLeftType() == OperandType.VARIABLE) {
        variables++;
        String ds = validate(builder, true, plan, config, 0, node_map);
        if (ds == null) {
          throw new RuntimeException("Failed to find a data source for the "
              + "left operand: " + parse_node.getLeft() + " for expression node:\n" 
              + config.getId() + ((DefaultQueryPlanner) plan).printConfigGraph());
        }
      }
      
      // TODO - this double walk is silly and ugly, fix me!
      if (parse_node.getRightType() == OperandType.VARIABLE) {
        variables++;
        String ds = validate(builder, false, plan, config, 0, node_map);
        if (ds == null) {
          throw new RuntimeException("Failed to find a data source for the "
              + "right operand: " + parse_node.getRight() + " for expression node:\n" 
              + config.getId() + ((DefaultQueryPlanner) plan).printConfigGraph());
        }
      }

      final ExpressionParseNode rebuilt = 
          (ExpressionParseNode) builder.build();
      new_nodes.add(rebuilt);
      node_map.put(rebuilt.getId(), rebuilt);
    }
    configs.clear();
    
    if (variables <= 1 && !config.getInfectiousNan()) {
      // We need to set infectious nan to true in case we have only one metric.
      ExpressionConfig new_config = (ExpressionConfig) c.toBuilder()
          .setInfectiousNan(true)
          .build();
      plan.replace(config, new_config);
      c = new_config;
    }
    
    // remove the old config and get the in and outgoing edges.
    final Set<QueryNodeConfig> upstream = 
        Sets.newHashSet(plan.configGraph().predecessors(c));
    for (final QueryNodeConfig n : upstream) {
      plan.removeEdge(n, c);
    }
    
    final Set<QueryNodeConfig> downstream = 
        Sets.newHashSet(plan.configGraph().successors(c));
    for (final QueryNodeConfig n : downstream) {
      plan.removeEdge(c, n);
    }
    
    // now yank ourselves out and link.
    plan.removeNode(c);
    for (final QueryNodeConfig node : new_nodes) {
      List<String> sources = node.getSources();
      for (final String source : sources) {
        final String src = source.contains(":") ? 
            source.substring(0, source.indexOf(":")) : source;
        plan.addEdge(node, node_map.get(src));
        if (Graphs.hasCycle(plan.configGraph())) {
          throw new IllegalStateException("Cylcle found when generating "
              + "sub expression graph for " + node.getId() + " => " 
              + node_map.get(source).getId());
        }
      }
    }
    
    for (final QueryNodeConfig up : upstream) {
      plan.addEdge(up, new_nodes.get(new_nodes.size() - 1));
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
                         final int depth,
                         final Map<String, QueryNodeConfig> node_map) {
    final String key = left ? (String) builder.left() : (String) builder.right();
    for (final QueryNodeConfig node : node_map.values()) {
      for (final QueryResultId src : (List<QueryResultId>) node.resultIds()) {
        final String metric = plan.getMetricForDataSource(node, src.dataSource());
        if (key.equals(metric) || key.equals(src.dataSource())) {
            // matched!
            if (left) {
              builder.setLeft(metric)
                     .setLeftId(src)
                     .addSource(node.getId());
            } else {
              builder.setRight(metric)
                     .setRightId(src)
                     .addSource(node.getId());
            }
            return src.dataSource();
        }
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
