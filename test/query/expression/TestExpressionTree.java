// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.SeekableViewsForTest;
import net.opentsdb.core.TSQuery;
import net.opentsdb.query.expression.ExpressionTree.Parameter;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSQuery.class })
public class TestExpressionTree {
  private final static String EXPR_NAME = "treeTestExpr";
  private static long START_TIME = 1356998400000L;
  private static int INTERVAL = 60000;
  private static int NUM_POINTS = 5;
  private static String METRIC = "sys.cpu";
  
  private TSQuery data_query;
  private SeekableView view;
  private DataPoints dps;
  private DataPoints[] group_bys;
  private List<DataPoints[]> query_results;
  private TreeTestExpr test_expression;
  
  @Before
  public void before() throws Exception {
    view = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, 1, 1);
    data_query = mock(TSQuery.class);
    when(data_query.startTime()).thenReturn(START_TIME);
    when(data_query.endTime()).thenReturn(START_TIME + (INTERVAL * NUM_POINTS));
    
    dps = PowerMockito.mock(DataPoints.class);
    when(dps.iterator()).thenReturn(view);
    when(dps.metricName()).thenReturn(METRIC);
    
    group_bys = new DataPoints[] { dps };
    
    query_results = new ArrayList<DataPoints[]>(1);
    query_results.add(group_bys);
    
    test_expression = new TreeTestExpr();
    ExpressionFactory.addFunction(EXPR_NAME, test_expression);
  }
  
  @Test
  public void ctorString() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    assertEquals(EXPR_NAME + "()", tree.toString());
    assertNull(tree.subExpressions());
    assertNull(tree.funcParams());
    assertNull(tree.subMetricQueries());
    assertTrue(tree.parameterIndex().isEmpty());
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void ctorStringNull() throws Exception {
    new ExpressionTree((String)null, data_query);
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void ctorStringEmpty() throws Exception {
    new ExpressionTree("", data_query);
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void ctorStringUnknown() throws Exception {
    new ExpressionTree("No such method", data_query);
  }
  
  @Test
  public void ctorExpression() throws Exception {
    final ExpressionTree tree = new ExpressionTree(test_expression, data_query);
    assertEquals(EXPR_NAME + "()", tree.toString());
    assertNull(tree.subExpressions());
    assertNull(tree.funcParams());
    assertNull(tree.subMetricQueries());
    assertTrue(tree.parameterIndex().isEmpty());
  }
  
  @Test
  public void addSubExpression() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    final ExpressionTree child = new ExpressionTree("scale", data_query);
    tree.addSubExpression(child, 1);
    assertEquals(1, tree.subExpressions().size());
    assertSame(child, tree.subExpressions().get(0));
    assertNull(tree.funcParams());
    assertNull(tree.subMetricQueries());
    assertEquals(1, tree.parameterIndex().size());
    assertEquals(Parameter.SUB_EXPRESSION, tree.parameterIndex().get(1));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addSubExpressionNullTree() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    tree.addSubExpression(null, 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addSubExpressionNegativeIndex() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    tree.addSubExpression(null, 1);
  }
  
  @Test (expected = IllegalDataException.class)
  public void addSubExpressionRecursion() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    tree.addSubExpression(tree, 1);
  }
  
  @Test
  public void addSubMetricQuery() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    tree.addSubMetricQuery(METRIC, 1, 1);
    assertEquals(EXPR_NAME + "(" + METRIC + ")", tree.toString());
    assertNull(tree.subExpressions());
    assertNull(tree.funcParams());
    assertEquals(1, tree.subMetricQueries().size());
    assertEquals(METRIC, tree.subMetricQueries().get(1));
    assertEquals(1, tree.parameterIndex().size());
    assertEquals(Parameter.METRIC_QUERY, tree.parameterIndex().get(1));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addSubMetricQueryNullMetric() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    tree.addSubMetricQuery(null, 1, 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addSubMetricQueryEmptyMetric() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    tree.addSubMetricQuery("", 1, 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addSubMetricQueryNegativeQueryIndex() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    tree.addSubMetricQuery(METRIC, -1, 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addSubMetricQueryNegativeParamIndex() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    tree.addSubMetricQuery(METRIC, 1, -1);
  }
  
  @Test
  public void addFunctionParameter() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    tree.addFunctionParameter("vimes");
    assertEquals(EXPR_NAME + "()", tree.toString());
    assertNull(tree.subExpressions());
    assertEquals(1, tree.funcParams().size());
    assertEquals("vimes", tree.funcParams().get(0));
    assertNull(tree.subMetricQueries());
    assertTrue(tree.parameterIndex().isEmpty());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addFunctionParameterNull() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    tree.addFunctionParameter(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addFunctionParameterEmpty() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    tree.addFunctionParameter("");
  }
  
  @Test
  public void evaluateNothingSet() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    assertEquals(EXPR_NAME + "()", tree.toString());
    
    final DataPoints[] response = tree.evaluate(query_results);
    assertEquals(1, response.length);
    assertSame(data_query, test_expression.data_query);
    assertEquals(0, test_expression.results.size());
    assertNull(test_expression.params);
  }
  
  @Test
  public void evaluateSubMetricQuerySet() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    tree.addSubMetricQuery(METRIC, 0, 0);
    assertEquals(EXPR_NAME + "(" + METRIC + ")", tree.toString());
    
    final DataPoints[] response = tree.evaluate(query_results);
    assertEquals(1, response.length);
    assertSame(data_query, test_expression.data_query);
    assertEquals(1, test_expression.results.size());
    assertNull(test_expression.params);
  }
  
  @Test
  public void evaluateSubMetricQuerySetWithParam() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    tree.addSubMetricQuery(METRIC, 0, 0);
    tree.addFunctionParameter("foo");
    assertEquals(EXPR_NAME + "(" + METRIC + ")", tree.toString());
    
    final DataPoints[] response = tree.evaluate(query_results);
    assertEquals(1, response.length);
    assertSame(data_query, test_expression.data_query);
    assertEquals(1, test_expression.results.size());
    assertEquals(1, test_expression.params.size());
    assertEquals("foo", test_expression.params.get(0));
  }
  
  @Test
  public void evaluateSubExpressionSet() throws Exception {
    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
    final ExpressionTree child = spy(new ExpressionTree("scale", data_query));
    child.addSubMetricQuery(METRIC, 0, 0);
    child.addFunctionParameter("1");
    tree.addSubExpression(child, 0);
    assertEquals(EXPR_NAME + "(scale(" + METRIC + "))", tree.toString());
    
    final DataPoints[] response = tree.evaluate(query_results);
    assertEquals(1, response.length);
    assertSame(data_query, test_expression.data_query);
    assertEquals(1, test_expression.results.size());
    assertNull(test_expression.params);
    verify(child, times(1)).evaluate(query_results);
  }

// TODO - fix this up
//  @Test
//  public void evaluateSubExpressionAndSubMetricSet() throws Exception {
//    final ExpressionTree tree = new ExpressionTree(EXPR_NAME, data_query);
//    final ExpressionTree child = spy(new ExpressionTree("scale", data_query));
//    child.addSubMetricQuery(METRIC, 0, 0);
//    child.addFunctionParameter("1");
//    tree.addSubExpression(child, 0);
//    
//    tree.addSubMetricQuery(METRIC, 1, 0);
//    tree.addFunctionParameter("foo");
//    
//    assertEquals(EXPR_NAME + "(scale(" + METRIC + ")," + METRIC + ")", 
//        tree.toString());
//    
//    final DataPoints[] response = tree.evaluate(query_results);
//    assertEquals(1, response.length);
//    assertSame(data_query, test_expression.data_query);
//    assertEquals(1, test_expression.results.size());
//    assertEquals(1, test_expression.params.size());
//    assertEquals("foo", test_expression.params.get(0));
//    verify(child, times(1)).evaluate(query_results);
//  }
  
  // TODO - more tests around indexes, etc unless we cleanup the class
  
  private class TreeTestExpr implements Expression {
    TSQuery data_query;
    List<DataPoints[]> results;
    List<String> params;
    
    @Override
    public DataPoints[] evaluate(TSQuery data_query,
        List<DataPoints[]> results, List<String> params) {
      this.data_query = data_query;
      this.results = results;
      this.params = params;
      
      // Returns an array the size of the total incoming results array
      int num_results = 0;
      for (DataPoints[] r: query_results) {
        num_results += r.length;
      }
      final DataPoints[] response = new DataPoints[num_results];
      return response;
    }
    @Override
    public String writeStringField(List<String> params,
        String inner_expression) {
      return EXPR_NAME + "(" + inner_expression + ")";
    }
  }
}
