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
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import net.opentsdb.core.TSQuery;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSQuery.class })
public class TestExpressions {
  private TSQuery data_query;
  private List<String> metric_queries;
  
  @Before
  public void before() throws Exception {
    data_query = mock(TSQuery.class);
    metric_queries = new ArrayList<String>();
  }
  
  @Test
  public void parse() throws Exception {
    final ExpressionTree tree = Expressions.parse(
        "scale(sys.cpu)", metric_queries, data_query);
    assertEquals("scale()", tree.toString());
  }
  
  @Test
  public void parseWithWhitespace() throws Exception {
    final ExpressionTree tree = Expressions.parse(
        "   scale(sys.cpu)", metric_queries, data_query);
    assertEquals("scale()", tree.toString());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseNullExpression() throws Exception {
    Expressions.parse(null, metric_queries, data_query);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseEmptyExpression() throws Exception {
    Expressions.parse("", metric_queries, data_query);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseMissingOpenParens() throws Exception {
    Expressions.parse("scalesys.cpu)", metric_queries, data_query);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseMissingClosingParens() throws Exception {
    Expressions.parse("scale(sys.cpu", metric_queries, data_query);
  }
  
  // TODO - These two may be problematic and need validation/fixing?
  @Test
  public void parseNullMetricQueries() throws Exception {
    final ExpressionTree tree = Expressions.parse(
        "scale(sys.cpu)", null, data_query);
    assertEquals("scale()", tree.toString());
  }
  
  @Test
  public void parseNullTSQuery() throws Exception {
    final ExpressionTree tree = Expressions.parse(
        "scale(sys.cpu)", metric_queries, null);
    assertEquals("scale()", tree.toString());
  }

  //TODO - Need to add more tests around parsing nested functions and params
}
