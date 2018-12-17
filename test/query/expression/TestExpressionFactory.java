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

import static org.junit.Assert.assertTrue;

import java.util.List;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.TSQuery;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
public class TestExpressionFactory {

  @Test
  public void getByName() throws Exception {
    // pick a couple of implementations
    Expression e = ExpressionFactory.getByName("scale");
    assertTrue(e instanceof Scale);
    
    e = ExpressionFactory.getByName("highestMax");
    assertTrue(e instanceof HighestMax);
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void getByNameNoSuchFunction() throws Exception {
    ExpressionFactory.getByName("I don't exist");
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void getByNameNullName() throws Exception {
    ExpressionFactory.getByName(null);
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void getByNameEmptyName() throws Exception {
    ExpressionFactory.getByName("");
  }
  
  @Test
  public void addFunction() throws Exception {
    ExpressionFactory.addFunction("testExpr", new TestExpr());
    final Expression e = ExpressionFactory.getByName("testExpr");
    assertTrue(e instanceof TestExpr);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addFunctionNullName() throws Exception {
    ExpressionFactory.addFunction(null, new TestExpr());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addFunctionEmptyName() throws Exception {
    ExpressionFactory.addFunction("", new TestExpr());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addFunctionNullFunction() throws Exception {
    ExpressionFactory.addFunction("testExpr", null);
  }
  
  /** Dummy expression class used for testing */
  private static class TestExpr implements Expression {
    @Override
    public DataPoints[] evaluate(TSQuery data_query,
        List<DataPoints[]> results, List<String> params) {
      return null;
    }
    @Override
    public String writeStringField(List<String> params,
        String inner_expression) {
      return null;
    }
  }
}
