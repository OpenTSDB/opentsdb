// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.query.pojo;

import net.opentsdb.query.pojo.Join.SetOperator;
import net.opentsdb.utils.JSON;

import org.junit.Test;

import com.google.common.collect.Maps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.Map;

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
    Expression expected = Expression.newBuilder().setId("e")
        .setExpression("a + b + c").setJoin(
            Join.newBuilder()
            .setOperator(SetOperator.UNION))
        .build();
    assertEquals(expected, expression);
    
    json = "{\"id\":\"e\",\"expr\":\"a + b + c\","
        + "\"join\":{\"operator\":\"INTERSECTION\"}}";
    expression = JSON.parseToObject(json, Expression.class);
    expression.validate();
    expected = Expression.newBuilder().setId("e")
        .setExpression("a + b + c")
        .setJoin(
            Join.newBuilder()
            .setOperator(SetOperator.INTERSECTION))
        .build();
    assertEquals(expected, expression);
    
    json = "{\"id\":\"e\",\"expr\":\"a + b + c\","
        + "\"join\":{\"operator\":\"INTERSECTION\"},\"fillPolicy\":"
        + "{\"policy\":\"scalar\",\"value\":42},\"fillPolicies\":"
        + "{\"a\":{\"policy\":\"NAN\"}, \"b\":{\"policy\":\"NAN\"}}}";
    expression = JSON.parseToObject(json, Expression.class);
    expression.validate();
    
    expected = Expression.newBuilder().setId("e")
        .setExpression("a + b + c")
        .setJoin(
            Join.newBuilder().setOperator(SetOperator.INTERSECTION))
        .setFillPolicy(NumericFillPolicy.newBuilder()
            .setPolicy(FillPolicy.SCALAR)
            .setValue(42))
        .addFillPolicy("a", NumericFillPolicy.newBuilder()
            .setPolicy(FillPolicy.NOT_A_NUMBER))
        .addFillPolicy("b", NumericFillPolicy.newBuilder()
            .setPolicy(FillPolicy.NOT_A_NUMBER))
        .build();
    assertEquals(expected, expression);
    
  }

  @Test
  public void serialize() throws Exception {
    Expression expression = Expression.newBuilder().setId("e1")
        .setJoin(Join.newBuilder().setOperator(SetOperator.UNION).build())
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

  @Test
  public void build() throws Exception {
    final Expression expression = Expression.newBuilder()
        .setId("e1")
        .setExpression("a + b")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setJoin(new Join.Builder()
            .setOperator(SetOperator.INTERSECTION)
            .build())
        .addFillPolicy("a", NumericFillPolicy.newBuilder()
            .setPolicy(FillPolicy.SCALAR).setValue(1))
        .addFillPolicy("b", NumericFillPolicy.newBuilder()
            .setPolicy(FillPolicy.SCALAR).setValue(1))
        .build();
    final Expression clone = Expression.newBuilder(expression).build();
    assertNotSame(clone, expression);
    assertEquals("e1", clone.getId());
    assertEquals("a + b", clone.getExpr());
    assertEquals(SetOperator.INTERSECTION, clone.getJoin().getOperator());
    assertEquals(clone.getFillPolicy(), 
        NumericFillPolicy.newBuilder()
        .setPolicy(FillPolicy.NOT_A_NUMBER)
        .build());
    assertEquals(clone.getFillPolicies().get("a"), 
        NumericFillPolicy.newBuilder()
        .setPolicy(FillPolicy.SCALAR)
        .setValue(1)
        .build());
    assertEquals(clone.getFillPolicies().get("b"), 
        NumericFillPolicy.newBuilder()
        .setPolicy(FillPolicy.SCALAR)
        .setValue(1)
        .build());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    Map<String, NumericFillPolicy> fills = Maps.newHashMap();
    fills.put("a", NumericFillPolicy.newBuilder()
        .setPolicy(FillPolicy.SCALAR).setValue(1).build());
    fills.put("b", NumericFillPolicy.newBuilder()
        .setPolicy(FillPolicy.SCALAR).setValue(42).build());
    
    final Expression e1 = new Expression.Builder()
        .setId("e1")
        .setExpression("a + b")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setJoin(new Join.Builder()
            .setOperator(SetOperator.INTERSECTION)
            .build())
        .setFillPolicies(Maps.newHashMap(fills))
        .build();
    
    Expression e2 = new Expression.Builder()
        .setId("e1")
        .setExpression("a + b")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setJoin(new Join.Builder()
            .setOperator(SetOperator.INTERSECTION)
            .build())
        .setFillPolicies(Maps.newHashMap(fills))
        .build();
    assertEquals(e1.hashCode(), e2.hashCode());
    assertEquals(e1, e2);
    assertEquals(0, e1.compareTo(e2));
    
    e2 = new Expression.Builder()
        .setId("e2") // <-- diff
        .setExpression("a + b")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setJoin(new Join.Builder()
            .setOperator(SetOperator.INTERSECTION)
            .build())
        .setFillPolicies(Maps.newHashMap(fills))
        .build();
    assertNotEquals(e1.hashCode(), e2.hashCode());
    assertNotEquals(e1, e2);
    assertEquals(-1, e1.compareTo(e2));
    
    e2 = new Expression.Builder()
        .setId("e1")
        .setExpression("b + a") // <-- diff
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setJoin(new Join.Builder()
            .setOperator(SetOperator.INTERSECTION)
            .build())
        .setFillPolicies(Maps.newHashMap(fills))
        .build();
    assertNotEquals(e1.hashCode(), e2.hashCode());
    assertNotEquals(e1, e2);
    assertEquals(-1, e1.compareTo(e2));
    
    e2 = new Expression.Builder()
        .setId("e1")
        .setExpression("a + b")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.ZERO) // <-- diff
            .build())
        .setJoin(new Join.Builder()
            .setOperator(SetOperator.INTERSECTION)
            .build())
        .setFillPolicies(Maps.newHashMap(fills))
        .build();
    assertNotEquals(e1.hashCode(), e2.hashCode());
    assertNotEquals(e1, e2);
    assertEquals(-1, e1.compareTo(e2));
    
    e2 = new Expression.Builder()
        .setId("e1")
        .setExpression("a + b")
        //.setFillPolicy(new NumericFillPolicy.Builder()  // <-- diff
        //    .setPolicy(FillPolicy.NOT_A_NUMBER)
        //    .build())
        .setJoin(new Join.Builder()
            .setOperator(SetOperator.INTERSECTION)
            .build())
        .setFillPolicies(Maps.newHashMap(fills))
        .build();
    assertNotEquals(e1.hashCode(), e2.hashCode());
    assertNotEquals(e1, e2);
    assertEquals(1, e1.compareTo(e2));
    
    e2 = new Expression.Builder()
        .setId("e1")
        .setExpression("a + b")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setJoin(new Join.Builder()
            .setOperator(SetOperator.UNION)  // <-- diff
            .build())
        .setFillPolicies(Maps.newHashMap(fills))
        .build();
    assertNotEquals(e1.hashCode(), e2.hashCode());
    assertNotEquals(e1, e2);
    assertEquals(-1, e1.compareTo(e2));
    
    e2 = new Expression.Builder()
        .setId("e1")
        .setExpression("a + b")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        //.setJoin(new Join.Builder()   // <-- diff
        //    .setOperator(SetOperator.INTERSECTION)
        //    .build())
        .setFillPolicies(Maps.newHashMap(fills))
        .build();
    assertNotEquals(e1.hashCode(), e2.hashCode());
    assertNotEquals(e1, e2);
    assertEquals(1, e1.compareTo(e2));
    
    Map<String, NumericFillPolicy> fill2 = Maps.newHashMap();
    fills.put("a", NumericFillPolicy.newBuilder()
        .setPolicy(FillPolicy.SCALAR).setValue(1).build());
    
    e2 = new Expression.Builder()
        .setId("e1")
        .setExpression("a + b")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setJoin(new Join.Builder()
            .setOperator(SetOperator.INTERSECTION)
            .build())
        .setFillPolicies(fill2)   // <-- diff
        .build();
    assertNotEquals(e1.hashCode(), e2.hashCode());
    assertNotEquals(e1, e2);
    assertEquals(-1, e1.compareTo(e2));
    
    e2 = new Expression.Builder()
        .setId("e1")
        .setExpression("a + b")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setJoin(new Join.Builder()
            .setOperator(SetOperator.INTERSECTION)
            .build())
        //.setFillPolicies(Maps.newHashMap(fills))   // <-- diff
        .build();
    assertNotEquals(e1.hashCode(), e2.hashCode());
    assertNotEquals(e1, e2);
    assertEquals(1, e1.compareTo(e2));
    
  }

}
