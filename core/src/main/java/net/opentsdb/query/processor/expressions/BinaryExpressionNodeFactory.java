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
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;

/**
 * Returns a node and iterators for a binary expression (usually created
 * from an ExpressionConfig and factory).
 * 
 * @since 3.0
 */
public class BinaryExpressionNodeFactory extends BaseQueryNodeFactory {

  public static final String TYPE = "BinaryExpression";
  
  /**
   * Required empty ctor.
   */
  public BinaryExpressionNodeFactory() {
    super();
    registerIteratorFactory(NumericType.TYPE, new NumericIteratorFactory());
    registerIteratorFactory(NumericSummaryType.TYPE, 
        new NumericSummaryIteratorFactory());
    registerIteratorFactory(NumericArrayType.TYPE, 
        new NumericArrayIteratorFactory());
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
  public QueryNode newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException("Default configs are not "
        + "allowed for Binary Expressions.");
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final QueryNodeConfig config) {
    return new BinaryExpressionNode(this, context, (ExpressionParseNode) config);
  }

  @Override
  public QueryNodeConfig parseConfig(final ObjectMapper mapper, 
                                     final TSDB tsdb,
                                     final JsonNode node) {
    ExpressionParseNode.Builder builder = ExpressionParseNode.newBuilder();
    
    OperandType left_type;
    OperandType right_type;
    
    JsonNode n = node.get("leftType");
    if (n == null) {
      throw new IllegalArgumentException("Node must have the left type.");
    }
    try {
      left_type = mapper.treeToValue(n, OperandType.class);
      builder.setLeftType(left_type);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse left type.", e);
    }
    
    n = node.get("rightType");
    if (n == null) {
      throw new IllegalArgumentException("Node must have the right type.");
    }
    try {
      right_type = mapper.treeToValue(n, OperandType.class);
      builder.setRightType(right_type);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse right type.", e);
    }
    
    n = node.get("left");
    if (n == null && left_type != OperandType.NULL) {
      throw new IllegalArgumentException("Left operand cannot be null.");
    }
    
    switch(left_type) {
    case LITERAL_NUMERIC:
      if (NumericType.looksLikeInteger(n.asText())) {
        builder.setLeft(new ExpressionParser.NumericLiteral(n.asLong()));
      } else {
        builder.setLeft(new ExpressionParser.NumericLiteral(n.asDouble()));
      }
      break;
    default:
      builder.setLeft(n.asText());
    }
    
    n = node.get("right");
    if (n == null && right_type != OperandType.NULL) {
      throw new IllegalArgumentException("Right operand cannot be null.");
    }
    switch(right_type) {
    case LITERAL_NUMERIC:
      if (NumericType.looksLikeInteger(n.asText())) {
        builder.setRight(new ExpressionParser.NumericLiteral(n.asLong()));
      } else {
        builder.setRight(new ExpressionParser.NumericLiteral(n.asDouble()));
      }
      break;
    default:
      builder.setRight(n.asText());
    }
    
    n = node.get("operator");
    if (n == null) {
      throw new IllegalArgumentException("Operation cannot be null.");
    }
    try {
      builder.setExpressionOp( mapper.treeToValue(n, ExpressionOp.class));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse op.", e);
    }
    
    n = node.get("not");
    if (n != null) {
      builder.setNot(n.asBoolean());
    }
    
    n = node.get("negate");
    if (n != null) {
      builder.setNegate(n.asBoolean());
    }
    
    n = node.get("expressionConfig");
    if (n == null) {
      throw new IllegalArgumentException("The expressionConfig cannot "
          + "be null.");
    }
    builder.setExpressionConfig(ExpressionConfig.parse(mapper, tsdb, n));
    
    n = node.get("as");
    if (n != null && !n.isNull()) {
      builder.setAs(n.asText());
    }
    
    n = node.get("leftId");
    if (n != null && !n.isNull()) {
      builder.setLeftId(n.asText());
    }
    
    n = node.get("rightId");
    if (n != null && !n.isNull()) {
      builder.setRightId(n.asText());
    }
    
    n = node.get("id");
    if (n == null || n.isNull()) {
      throw new IllegalArgumentException("Missing ID for node config");
    }
    builder.setId(n.asText());
    
    return builder.build();
  }

  @Override
  public void setupGraph(
      final TimeSeriesQuery query, 
      final QueryNodeConfig config,
      final QueryPlanner plan) {
    // nothing to do here.
  }

  /**
   * The default numeric iterator factory.
   */
  protected class NumericIteratorFactory implements QueryIteratorFactory {

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      throw new UnsupportedOperationException("Expression iterators must have a map.");
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
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
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      throw new UnsupportedOperationException("Expression iterators must have a map.");
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new ExpressionNumericSummaryIterator(node, result, sources);
    }
    
    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericSummaryType.TYPE);
    }
  }

  /**
   * Handles arrays.
   */
  protected class NumericArrayIteratorFactory implements QueryIteratorFactory {

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      throw new UnsupportedOperationException("Expression iterators must have a map.");
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new ExpressionNumericArrayIterator(node, result, sources);
    }
    
    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericArrayType.TYPE);
    }
  }
}
