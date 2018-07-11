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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.BeforeClass;

import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.DefaultInterpolatorFactory;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.Joiner;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;

public class BaseNumericTest {
  
  protected static Joiner JOINER;
  protected static NumericInterpolatorConfig NUMERIC_CONFIG;
  protected static ExpressionConfig CONFIG;
  protected static JoinConfig JOIN_CONFIG;
  protected static QueryResult RESULT;
  protected static TSDB TSDB;
  protected static QueryPipelineContext CONTEXT;
  protected static TimeSeriesId LEFT_ID;
  protected static TimeSeriesId RIGHT_ID;
  
  protected BinaryExpressionNode node;
  protected ExpressionParseNode expression_config;
  
  @SuppressWarnings("unchecked")
  @BeforeClass
  public static void beforeClass() throws Exception {
    JOINER = mock(Joiner.class);
    RESULT = mock(QueryResult.class);
    TSDB = mock(TSDB.class);
    
    NUMERIC_CONFIG = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.NONE)
      .setType(NumericType.TYPE.toString())
      .build();
    
    JOIN_CONFIG = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId("join")
        .build();
    
    CONFIG = (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("a + b + c")
        .setJoinConfig(JOIN_CONFIG)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    
    CONTEXT = mock(QueryPipelineContext.class);
    when(CONTEXT.tsdb()).thenReturn(TSDB);
    final Registry registry = mock(Registry.class);
    when(TSDB.getRegistry()).thenReturn(registry);
    final QueryInterpolatorFactory interp_factory = new DefaultInterpolatorFactory();
    interp_factory.initialize(TSDB).join();
    when(registry.getPlugin(any(Class.class), anyString())).thenReturn(interp_factory);
    
    LEFT_ID = BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build();
    RIGHT_ID = BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build();
  }
  
  @Before
  public void before() throws Exception {
    node = mock(BinaryExpressionNode.class);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    
    when(node.pipelineContext()).thenReturn(CONTEXT);
    when(node.config()).thenReturn(CONFIG);
    when(node.expressionConfig()).thenReturn(expression_config);
    when(node.joiner()).thenReturn(JOINER);
  }
}
