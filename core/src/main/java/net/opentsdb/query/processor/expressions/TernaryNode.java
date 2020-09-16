//This file is part of OpenTSDB.
//Copyright (C) 2020  The OpenTSDB Authors.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.joins.Joiner;
import net.opentsdb.query.processor.expressions.BinaryExpressionNode.ErrorCB;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;

/**
 * The ternary node implementation. It simply extends the 
 * {@link BinaryExpressionNode} addint the condition metric.
 * 
 * @since 3.0
 */
public class TernaryNode extends BinaryExpressionNode {
  private static final Logger LOG = LoggerFactory.getLogger(
      TernaryNode.class);
  
  protected byte[] condition_metric;
  
  public TernaryNode(final QueryNodeFactory factory,
                               final QueryPipelineContext context, 
                               final ExpressionParseNode expression_config) {
    super(factory, context, expression_config);

    final TernaryParseNode config = (TernaryParseNode) expression_config;
    results.put(config.getConditionId(), null);
  }
  
  @Override
  public void onNext(final QueryResult next) {
    if (results.containsKey(next.dataSource())) {
      if (!Strings.isNullOrEmpty(next.error()) || next.exception() != null) {
        sendUpstream(new FailedQueryResult(next));
        return;
      }
      synchronized (this) {
        results.put(next.dataSource(), next);
      }
    } else {
      LOG.warn("Unexpected result at ternary node " + expression_config.getId() 
        + ": " + next.dataSource());
      return;
    }
    
    if (resolveMetrics(next)) {
      // resolving, don't progress yet.
      return;
    }
    
    if (resolveJoinStrings(next)) {
      // resolving, don't progress yet.
      return;
    }
    
    // see if all the results are in.
    int received = 0;
    synchronized (this) {
      for (final QueryResult result : results.values()) {
        if (result != null) {
          received++;
        }
      }
    }
    
    if (received == results.size()) {
      // order is important here.
      final TernaryParseNode config = 
          (TernaryParseNode) expression_config;
      result.join();
      try {
        sendUpstream(result);
      } catch (Exception e) {
        sendUpstream(e);
      }
    }
  }

  protected boolean resolveMetrics(final QueryResult next) {
    if (resolved_metrics) {
      return false;
    }
    if (next.idType() != Const.TS_BYTE_ID) {
      resolved_metrics = true;
      return false;
    }
    
    final TernaryParseNode config = (TernaryParseNode) expression_config;
    if (next.idType() == Const.TS_BYTE_ID &&
        (config.getLeftType() == OperandType.VARIABLE || 
         config.getRightType() == OperandType.VARIABLE ||
         config.getConditionType() == OperandType.VARIABLE) && 
        !resolved_metrics) {
      final List<String> metrics = Lists.newArrayListWithExpectedSize(2);
      if (config.getLeftType() == OperandType.VARIABLE) {
        metrics.add((String) config.getLeft());
      } else if (config.getLeftType() == OperandType.SUB_EXP){
        left_metric = ((String) config.getLeft()).getBytes(Const.UTF8_CHARSET);
      }
      
      if (config.getRightType() == OperandType.VARIABLE) {
        metrics.add((String) config.getRight());
      } else if (config.getRightType() == OperandType.SUB_EXP) {
        right_metric = ((String) config.getRight()).getBytes(Const.UTF8_CHARSET);
      }
      
      if (config.getConditionType() == OperandType.VARIABLE) {
        metrics.add((String) config.getCondition());
      } else if (config.getConditionType() == OperandType.SUB_EXP) {
        condition_metric = ((String) config.getCondition()).getBytes(Const.UTF8_CHARSET);
      }
      
      class ResolveCB implements Callback<Object, List<byte[]>> {
        @Override
        public Object call(final List<byte[]> uids) throws Exception {
          int idx = 0;
          if (expression_config.getLeftType() == OperandType.VARIABLE) {
            left_metric = uids.get(idx);
            if (expression_config.getRightType() != OperandType.VARIABLE ||
                !expression_config.getRight().equals(expression_config.getLeft())) {
                // ie, left and right are not the same metric
                idx++;
            }
          }
          
          if (expression_config.getRightType() == OperandType.VARIABLE) {
            right_metric = uids.get(idx);
            if (config.getConditionType() != OperandType.VARIABLE ||
                !config.getCondition().equals(config.getRight())) {
              idx++;
            }
          }
          
          if (config.getConditionType() == OperandType.VARIABLE) {
            System.out.println("******** SETTING COND VAR");
            condition_metric = uids.get(idx);
          }
          
          resolved_metrics = true;
          // call back into onNext() to progress to the next step.
          onNext(next);
          return null;
        }
      }
      
      ((TimeSeriesByteId) next.timeSeries().iterator().next().id())
        .dataStore().encodeJoinMetrics(metrics, null /* TODO */)
        .addCallback(new ResolveCB())
        .addErrback(new ErrorCB());
      return true;
    }
    return false;
  }
  
  byte[] conditionMetric() {
    return condition_metric;
  }
}