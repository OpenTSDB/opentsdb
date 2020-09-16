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

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.utils.Pair;

/**
 * The result of a {@link BinaryExpressionNode} or {@link TernaryExpressionNode}.
 * 
 * @since 3.0
 */
public class ExpressionResult implements QueryResult {
  /** The parent node. */
  protected final BinaryExpressionNode node;
  
  /** Whether or not we're a tenary result. */
  protected final boolean is_ternary;
  
  /** The list of joined time series. */
  protected List<TimeSeries> time_series;
  
  /** The first non-null result we encounter so we can pass through result fields
   * like timespec, etc.
   */
  protected QueryResult non_null_result;
  
  /**
   * Package private ctor.
   * @param node A non-null parent node.
   */
  ExpressionResult(final BinaryExpressionNode node) {
    this.node = node;
    is_ternary = node instanceof TernaryNode;
  }
  
  /**
   * Package private method called by the node when it has seen all the
   * results it needs for the expression.
   */
  void join() {
    final Iterable<TimeSeries[]> joins;
    final ExpressionParseNode config = (ExpressionParseNode) node.config();
    
    final byte[] left_key;
    if (config.getLeft() != null && 
        (config.getLeftType() == OperandType.SUB_EXP || 
         config.getLeftType() == OperandType.VARIABLE)) {
      left_key = node.leftMetric() != null ? node.leftMetric() : 
          ((String) config.getLeft()).getBytes(Const.UTF8_CHARSET);
    } else {
      left_key = null;
    }
    
    final byte[] right_key;
    if (config.getRight() != null && 
        (config.getRightType() == OperandType.SUB_EXP || 
         config.getRightType() == OperandType.VARIABLE)) {
      right_key = node.rightMetric() != null ? node.rightMetric() : 
        ((String) config.getRight()).getBytes(Const.UTF8_CHARSET);
    } else {
      right_key = null;
    }
    
    final byte[] ternary_key;
    if (node instanceof TernaryNode) {
      if (((TernaryNode) node).conditionMetric() != null) {
        ternary_key = ((TernaryNode) node).conditionMetric();
      } else {
        ternary_key = ((TernaryParseNode) node.config()).getCondition()
            .toString().getBytes(Const.UTF8_CHARSET);
      }
    } else {
      ternary_key = null;
    }
    
    joins = node.joiner().join(
        node.results().values(),
        (ExpressionParseNode) node.config(),
        left_key, 
        right_key,
        ternary_key);
    
    time_series = Lists.newArrayList();
    for (final TimeSeries[] pair : joins) {
      time_series.add(new ExpressionTimeSeries(node, this, pair[0], pair[1], 
          (pair.length == 3 ? pair[2] : null)));
    }
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    if (non_null_result == null) {
      non_null_result = node.results().values().iterator().next();
    }
    return non_null_result.timeSpecification();
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return time_series;
  }

  @Override
  public String error() {
    // TODO - implement
    return null;
  }
  
  @Override
  public Throwable exception() {
    // TODO - implement
    return null;
  }
  
  @Override
  public long sequenceId() {
    return 0;
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public QueryResultId dataSource() {
    return node.config().resultIds().get(0);
  }
  
  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    if (non_null_result == null) {
      non_null_result = node.results().values().iterator().next();
    }
    return non_null_result.idType();
  }

  @Override
  public ChronoUnit resolution() {
    if (non_null_result == null) {
      non_null_result = node.results().values().iterator().next();
    }
    return non_null_result.resolution();
  }

  @Override
  public RollupConfig rollupConfig() {
    if (non_null_result == null) {
      non_null_result = node.results().values().iterator().next();
    }
    return non_null_result.rollupConfig();
  }

  @Override
  public void close() {
    for (final Entry<QueryResultId, QueryResult> entry : node.results().entrySet()) {
      entry.getValue().close();
    }
  }

  @Override
  public boolean processInParallel() {
    return false;
  }

}