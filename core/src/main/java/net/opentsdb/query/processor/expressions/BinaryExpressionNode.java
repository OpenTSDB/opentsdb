// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.expressions;

import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.BaseWrappedQueryResult;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.joins.Joiner;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.utils.Bytes.ByteMap;
import net.opentsdb.utils.Pair;

/**
 * A query node that executes a binary expression such as "a + b" or
 * "a > 42". Instantiates a joiner to handle filtering and joining on
 * the operands. This may represent a sub expression if the original
 * expression config parsed into a larger tree.
 * 
 * TODO - Right now we mostly work with metric names. This sucks. Imagine
 * if someone has a DAG like "exp -> gb -> ds -> source" and they 
 * put in "gb.id" as the name. We should walk back to the metric name
 * of the "source + 1". BUT if they have "exp -> alias -> gb -> ds -> source"
 * and alias renames the metric or adds an alias, they should be able to
 * put in "alias + 1" and we should find it.
 * 
 * Aliass kinda work, at least sub expressions will.
 * 
 * @since 3.0
 */
public class BinaryExpressionNode extends AbstractQueryNode<ExpressionParseNode> {
  private static final Logger LOG = LoggerFactory.getLogger(
      BinaryExpressionNode.class);
  
  /** The original expression config */
  protected final ExpressionConfig config;
  
  /** The parsed config for this particular node. */
  protected final ExpressionParseNode expression_config;
  
  /** The joiner. */
  protected final Joiner joiner;
  
  /** Whether or not we need two sources or if we're operating on a 
   * source and a literal. */
  protected final Pair<QueryResult, QueryResult> results;
  protected String left_source;
  protected String right_source;
  protected final int expected;
  
  /** The result to populate and return. */
  protected ExpressionResult result;
  
  /** Used to filtering when we're working on encoded IDs. */
  protected byte[] left_metric;
  protected byte[] right_metric;
  protected volatile boolean resolved_metrics;
  
  /**
   * Default ctor.
   * @param factory The factory we came from.
   * @param context The non-null context.
   * @param expression_config The non-null sub-expression config.
   */
  public BinaryExpressionNode(final QueryNodeFactory factory,
                              final QueryPipelineContext context, 
                              final ExpressionParseNode expression_config) {
    super(factory, context);
    if (expression_config == null) {
      throw new IllegalArgumentException("Expression config cannot be null.");
    }
    this.expression_config = expression_config;
    config = expression_config.getExpressionConfig();
    result = new ExpressionResult(this);
    results = new Pair<QueryResult, QueryResult>();
    
    if (expression_config.getLeftType() == OperandType.SUB_EXP || 
        expression_config.getLeftType() == OperandType.VARIABLE) {
      if (expression_config.getLeftId() == null) {
        left_source = (String) expression_config.getLeft();
      } else {
        left_source = expression_config.getLeftId();
      }
    }
    if (expression_config.getRightType() == OperandType.SUB_EXP || 
         expression_config.getRightType() == OperandType.VARIABLE) {
      if (expression_config.getRightId() == null) {
        right_source = (String) expression_config.getRight();
      } else {
        right_source = expression_config.getRightId();
      }
    }
    if (!Strings.isNullOrEmpty(left_source) && !Strings.isNullOrEmpty(right_source)) {
      expected = 2;
    } else {
      expected = 1;
    }
    joiner = new Joiner(config.getJoin());
  }

  @Override
  public ExpressionParseNode config() {
    return expression_config;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public void onNext(final QueryResult next) {
    final String id = next.source().config().getId();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Result: " + id + ":" + next.dataSource() + " Want<" + left_source + ", " + right_source +">");
    }
    
    if (left_source != null && (left_source.equals(next.dataSource()) ||
        left_source.equalsIgnoreCase(id))) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Matched left [" + left_source + "] with: " + id + ":" + next.dataSource());
      }
      if (!Strings.isNullOrEmpty(next.error()) || next.exception() != null) {
        sendUpstream(new FailedQueryResult(next));
        return;
      }
      synchronized (this) {
        results.setKey(next);
      }
      LOG.trace("SET LEFT!");
    } else if (right_source != null && 
        (right_source.equals(next.dataSource()) || 
            right_source.equals(id))) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Matched right [" + right_source + "] with: " + id + ":" + next.dataSource());
      }
      if (!Strings.isNullOrEmpty(next.error()) || next.exception() != null) {
        sendUpstream(new FailedQueryResult(next));
        return;
      }
      synchronized (this) {
        results.setValue(next);
      }
      LOG.trace("SET RACE!");
    } else {
      LOG.debug("Unmatched result: " + id + ":" + next.dataSource());
      return;
    }
    
    // NOTE: There is a race condition here where two results may resolve
    // the IDs. That's ok though.
    class ErrorCB implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception ex) throws Exception {
        LOG.error("Failure in binary expression node", ex);
        sendUpstream(ex);
        return null;
      }
    }
    
    // Try to resolve the variable names as metrics. This should return
    // and empty
    if (next.idType() == Const.TS_BYTE_ID &&
        (expression_config.getLeftType() == OperandType.VARIABLE || 
         expression_config.getRightType() == OperandType.VARIABLE) && 
        !resolved_metrics) {
      final List<String> metrics = Lists.newArrayListWithExpectedSize(2);
      if (expression_config.getLeftType() == OperandType.VARIABLE) {
        metrics.add((String) expression_config.getLeft());
      } else if (expression_config.getLeftType() == OperandType.SUB_EXP){
        left_metric = ((String) expression_config.getLeft()).getBytes(Const.UTF8_CHARSET);
      }
      
      if (expression_config.getRightType() == OperandType.VARIABLE) {
        metrics.add((String) expression_config.getRight());
      } else if (expression_config.getRightType() == OperandType.SUB_EXP) {
        right_metric = ((String) expression_config.getRight()).getBytes(Const.UTF8_CHARSET);
      }
      
      class ResolveCB implements Callback<Object, List<byte[]>> {
        @Override
        public Object call(final List<byte[]> uids) throws Exception {
          int idx = 0;
          if (expression_config.getLeftType() == OperandType.VARIABLE) {
            left_metric = uids.get(idx++);
          }
          
          if (expression_config.getRightType() == OperandType.VARIABLE) {
            right_metric = uids.get(idx);
          }
          
          resolved_metrics = true;
          // fall through to the next step
          onNext(next);
          return null;
        }
      }
      
      if (next.timeSeries() == null || next.timeSeries().isEmpty()) {
        onNext(new BaseWrappedQueryResult(next) {
          
          @Override
          public QueryNode source() {
            return BinaryExpressionNode.this;
          }
          
          @Override
          public TypeToken<? extends TimeSeriesId> idType() {
            return Const.TS_STRING_ID;
          }
          
        });
      } else {
        ((TimeSeriesByteId) next.timeSeries().iterator().next().id())
          .dataStore().encodeJoinMetrics(metrics, null /* TODO */)
          .addCallback(new ResolveCB())
          .addErrback(new ErrorCB());
      }
      return;
    }
    
    if (next.idType() == Const.TS_BYTE_ID && 
        joiner.encodedJoins() == null && 
        !config.getJoin().getJoins().isEmpty()) {
      // resolve the join tags
      final List<String> tagks = Lists.newArrayListWithExpectedSize(
          config.getJoin().getJoins().size());
      // yeah we could dedupe but *shrug*
      for (final Entry<String, String> entry : 
          config.getJoin().getJoins().entrySet()) {
        tagks.add(entry.getKey());
        tagks.add(entry.getValue());
      }
      
      class ResolveCB implements Callback<Object, List<byte[]>> {
        @Override
        public Object call(final List<byte[]> uids) throws Exception {
          final ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
          for (int i = 0; i < uids.size(); i += 2) {
            final byte[] left = uids.get(i);
            if (left == null) {
              sendUpstream(new QueryDownstreamException(
                  "Unable to resolve tag key: " + tagks.get(i)));
              return null;
            }
            final byte[] right = uids.get(i + 1);
            if (right == null) {
              sendUpstream(new QueryDownstreamException(
                  "Unable to resolve tag key: " + tagks.get(i + 1)));
              return null;
            }
            encoded_joins.put(left, right);
          }
          joiner.setEncodedJoins(encoded_joins);
          // fall through to the next step
          onNext(next);
          return null;
        }
      }
      
      ((TimeSeriesByteId) next.timeSeries().iterator().next().id())
        .dataStore().encodeJoinKeys(tagks, null /* TODO */)
        .addCallback(new ResolveCB())
        .addErrback(new ErrorCB());
      return;
    }
    
    // see if all the results are in.
    int received = 0;
    synchronized (this) {
      if (results.getKey() != null) {
        received++;
      }
      if (results.getValue() != null) {
        received++;
      }
    }
    
    if (received == expected) {
      try {
        result.set(results);
        result.join();
        if (LOG.isTraceEnabled()) {
          LOG.trace("Sending expression upstream: " + config.getId());
        }
        sendUpstream(result);
      } catch (Exception e) {
        sendUpstream(e);
      }
    } else if (LOG.isTraceEnabled()) {
      LOG.trace("Not all results are in for: " + config.getId());
    }
  }
  
  /**
   * Wrapper for failed queries.
   */
  class FailedQueryResult extends BaseWrappedQueryResult {

    public FailedQueryResult(final QueryResult result) {
      super(result);
    }

    @Override
    public QueryNode source() {
      return BinaryExpressionNode.this;
    }
    
  }
  
  ExpressionConfig expressionConfig() {
    return config;
  }
  
  Joiner joiner() {
    return joiner;
  }

  byte[] leftMetric() {
    return left_metric;
  }
  
  byte[] rightMetric() {
    return right_metric;
  }
}