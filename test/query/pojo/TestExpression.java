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
package net.opentsdb.query.pojo;

import net.opentsdb.query.expression.VariableIterator.SetOperator;
import net.opentsdb.utils.JSON;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestExpression {
  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenIdIsNull() throws Exception {
    String json = "{\"expr\":\"a + b + c\"}";
    Expression expression = JSON.parseToObject(json, Expression.class);
    expression.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenIdIsEmpty() throws Exception {
    String json = "{\"expr\":\"a + b + c\",\"id\":\"\"}";
    Expression expression = JSON.parseToObject(json, Expression.class);
    expression.validate();
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenIdIsInvalid() throws Exception {
    String json = "{\"expr\":\"a + b + c\",\"id\":\"system.busy\"}";
    Expression expression = JSON.parseToObject(json, Expression.class);
    expression.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenExprIsNull() throws Exception {
    String json = "{\"id\":\"1\"}";
    Expression expression = JSON.parseToObject(json, Expression.class);
    expression.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenExprIsEmpty() throws Exception {
    String json = "{\"id\":\"1\",\"expr\":\"\"}";
    Expression expression = JSON.parseToObject(json, Expression.class);
    expression.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenJoinIsInvalid() throws Exception {
    String json = "{\"expr\":\"a + b + c\",\"id\":\"system.busy\","
        + "\"join\":{\"operator\":\"nosuchjoin\"}}";
    Expression expression = JSON.parseToObject(json, Expression.class);
    expression.validate();
  }
  
  @Test
  public void deserialize() throws Exception {
    String json = "{\"id\":\"e\",\"expr\":\"a + b + c\"}";
    Expression expression = JSON.parseToObject(json, Expression.class);
    expression.validate();
    Expression expected = Expression.Builder().setId("e")
        .setExpression("a + b + c").setJoin(
            Join.Builder().setOperator(SetOperator.UNION).build()).build();
    assertEquals(expected, expression);
  }

  @Test
  public void serialize() throws Exception {
    Expression expression = Expression.Builder().setId("e1")
        .setJoin(Join.Builder().setOperator(SetOperator.UNION).build())
        .setExpression("a + b + c").build();
    String actual = JSON.serializeToString(expression);
    assertTrue(actual.contains("\"id\":\"e1\""));
    assertTrue(actual.contains("\"expr\":\"a + b + c\""));
    assertTrue(actual.contains("\"join\":{\"operator\":\"union\""));
    
  }

  @Test
  public void unknownShouldBeIgnored() throws Exception {
    String json = "{\"id\":\"1\",\"expr\":\"a + b + c\",\"unknown\":\"yo\"}";
    JSON.parseToObject(json, Expression.class);
    // pass if no unexpected exception
  }
}
