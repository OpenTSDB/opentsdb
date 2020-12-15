// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.ratio;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.query.processor.expressions.ExpressionConfig;
import net.opentsdb.query.processor.expressions.ExpressionParser.NumericLiteral;
import net.opentsdb.query.processor.expressions.ExpressionParseNode;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.rate.Rate;

/**
 * Handles computing the ratio for a metric from the sum of all of the time 
 * series for that metric. This works by mutating the config graph and adding a
 * group by node that flattens the incoming time series (sums and groups all into
 * one time series) and then dividing each time series by that sum using an 
 * expression node with a cross-join. It's really just a shortcut since we 
 * already have the nodes we need.
 * <p>
 * The config accepts a list of data sources that should match the node ID of 
 * the TimeSeriesDataSource. It will then create a set of group by and expression
 * nodes for each source.
 * 
 * @since 3.0
 */
public class RatioFactory extends BaseQueryNodeFactory<RatioConfig, Rate> {
  
  public static final String TYPE = "Ratio";

  @Override
  public RatioConfig parseConfig(final ObjectMapper mapper, 
                                      final TSDB tsdb, 
                                      final JsonNode node) {
    return RatioConfig.parse(mapper, tsdb, node);
  }

  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final RatioConfig config,
                         final QueryPlanner plan) {
    // here's the fun part. We add a group by with a sum and feed it into an
    // expression.
    final Set<QueryNodeConfig> downstream = plan.configGraph().successors(config);
    final Set<QueryNodeConfig> predecessors = Sets.newHashSet(
        plan.configGraph().predecessors(config));
    for (int i = 0; i < config.getDataSources().size(); i++) {
      final String source = config.getDataSources().get(i);
      final String metric = plan.getMetricForDataSource(config, source);
      if (Strings.isNullOrEmpty(metric)) {
        throw new IllegalArgumentException("No metric found for source: " + source);
      }
      
      QueryNodeConfig right = null;
      for (final QueryNodeConfig ds : downstream) {
        if (hasSourceNode(ds, source, plan)) {
          right = ds;
          break;
        }
      }
      if (right == null) {
        throw new IllegalArgumentException("No data source node found for: " + source);
      }
      
      final QueryResultId gbid = new DefaultQueryResultId(
          "ratio_gb_" + source, config.getDataSources().get(i));
      // TODO - at some point we should look at the feeding nodes and see if
      // there is already a group by with sum and no tags that we can just 
      // re-use.
      GroupByConfig gb = GroupByConfig.newBuilder()
          .setAggregator("sum")
          .setInfectiousNan(config.getInfectiousNan())
          .addSource(source)
          .addResultId(gbid)
          .setInterpolatorConfigs(Lists.newArrayList(
              config.interpolatorConfigs().values()))
          .setId("ratio_gb_" + source)
          .build();
      
      
      ExpressionConfig exp_config = ExpressionConfig.newBuilder()
          .setExpression("ratio")
          .setJoinConfig(JoinConfig.newBuilder()
              .setJoinType(JoinType.CROSS)
              .build())
          .setInterpolatorConfigs(Lists.newArrayList(
              config.interpolatorConfigs().values()))
          .setId("ratio_exp_" + source)
          .build();
      
      ExpressionParseNode ex = ExpressionParseNode.newBuilder()
          .setExpressionConfig(exp_config)
          .setExpressionOp(ExpressionOp.DIVIDE)
          .setLeft(metric)
          .setLeftId((QueryResultId) right.resultIds().get(0))
          .setLeftType(OperandType.VARIABLE)
          
          .setRight(metric)
          .setRightId(gbid)
          .setRightType(OperandType.VARIABLE)
          
          .setAs(config.getAs())
          .addSource(gb.getId())
          .addSource(right.getId())
          .setId(config.getAsPercent() ? "ratio_subexp_" + source : 
            "ratio_exp_" + source)
          .build();
      
      ExpressionParseNode percentage = null;
      if (config.getAsPercent()) {
        percentage = ExpressionParseNode.newBuilder()
            .setExpressionConfig(exp_config)
            .setExpressionOp(ExpressionOp.MULTIPLY)
            .setLeft(config.getAs())
            .setLeftId(new DefaultQueryResultId("ratio_subexp_" + source, source))
            .setLeftType(OperandType.SUB_EXP)
            
            .setRight(new NumericLiteral(100))
            .setRightType(OperandType.LITERAL_NUMERIC)
            
            .setAs(config.getAs())
            .addSource(ex.getId())
            .setId("ratio_exp_" + source)
            .build();
        plan.configGraph().addNode(percentage);
      }
      
      plan.configGraph().addNode(gb);
      plan.configGraph().addNode(ex);
      if (percentage != null) {
        plan.addEdge(percentage, ex);
      }
      
      
      for (final QueryNodeConfig pred : predecessors) {
        plan.removeEdge(pred, config);
        plan.addEdge(pred, percentage != null ? percentage : ex);
      }
      
      plan.addEdge(ex, gb);
      plan.addEdge(ex, right);
      plan.addEdge(gb, right);
    }
    
    plan.configGraph().removeNode(config);
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }

  boolean hasSourceNode(final QueryNodeConfig config, 
                        final String source, 
                        final QueryPlanner plan) {
    for (final QueryResultId id : (List<QueryResultId>) config.resultIds()) {
      if (id.dataSource().equals(source)) {
        return true;
      }
    }
    
    for (final QueryNodeConfig ds : plan.configGraph().successors(config)) {
      if (hasSourceNode(ds, source, plan)) {
        return true;
      }
    }
    return false;
  }
}
