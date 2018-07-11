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

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.utils.Pair;

/**
 * The result of a BinaryExpressionNode.
 * 
 * @since 3.0
 */
public class ExpressionResult implements QueryResult {
  /** The parent node. */
  protected final BinaryExpressionNode node;
  
  /** The list of 1 or 2 results. For now. */
  protected final List<QueryResult> results;
  
  /** The list of joined time series. */
  protected List<TimeSeries> time_series;
  
  /**
   * Package private ctor.
   * @param node A non-null parent node.
   */
  ExpressionResult(final BinaryExpressionNode node) {
    this.node = node;
    results = Lists.newArrayListWithExpectedSize(2);
  }
  
  /**
   * Package private method to add a result.
   * @param result A non-null result.
   * Note that we don't check the Id types here.
   */
  void add(final QueryResult result) {
    results.add(result);
  }
  
  /**
   * Package private method called by the node when it has seen all the
   * results it needs for the expression.
   */
  void join() {
    final Iterable<Pair<TimeSeries, TimeSeries>> joins;
    if ((node.expressionConfig().leftType() == OperandType.SUB_EXP || 
        node.expressionConfig().leftType() == OperandType.VARIABLE) &&
        (node.expressionConfig().rightType() == OperandType.SUB_EXP || 
        node.expressionConfig().rightType() == OperandType.VARIABLE)) {
      final boolean use_alias = 
          node.expressionConfig().leftType() != OperandType.VARIABLE ||
              node.expressionConfig().rightType() != OperandType.VARIABLE;
      joins = node.joiner().join(results, 
          node.leftMetric() != null ? node.leftMetric() : 
            ((String) node.expressionConfig().left()).getBytes(Const.UTF8_CHARSET), 
          node.rightMetric() != null ? node.rightMetric() : 
            ((String) node.expressionConfig().right()).getBytes(Const.UTF8_CHARSET),
          use_alias);
    } else if (node.expressionConfig().leftType() == OperandType.SUB_EXP || 
        node.expressionConfig().leftType() == OperandType.VARIABLE) {
      final boolean use_alias = 
          node.expressionConfig().leftType() != OperandType.VARIABLE;
      // left
      joins = node.joiner().join(
          results, 
          node.leftMetric() != null ? node.leftMetric() : 
            ((String) node.expressionConfig().left()).getBytes(Const.UTF8_CHARSET), 
          true,
          use_alias);
    } else {
      final boolean use_alias = 
          node.expressionConfig().rightType() != OperandType.VARIABLE;
      // right
      joins = node.joiner().join(
          results, 
          node.rightMetric() != null ? node.rightMetric() :
            ((String)  node.expressionConfig().right()).getBytes(Const.UTF8_CHARSET), 
          false, 
          use_alias);
    }
    
    time_series = Lists.newArrayList();
    for (final Pair<TimeSeries, TimeSeries> pair : joins) {
      time_series.add(new ExpressionTimeSeries(node, this, pair.getKey(), pair.getValue()));
    }
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<TimeSeries> timeSeries() {
    return time_series;
  }

  @Override
  public long sequenceId() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return results.get(0).idType();
  }

  @Override
  public ChronoUnit resolution() {
    // TODO Auto-generated method stub
    return ChronoUnit.SECONDS;
  }

  @Override
  public RollupConfig rollupConfig() {
    return results.get(0).rollupConfig();
  }

  @Override
  public void close() {
    for (final QueryResult result : results) {
      result.close();
    }
  }
  
}