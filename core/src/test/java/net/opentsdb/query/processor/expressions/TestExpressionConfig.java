// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.utils.JSON;

import java.util.ArrayList;

public class TestExpressionConfig {

  @Test
  public void builder() throws Exception {
    NumericInterpolatorConfig numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    ExpressionConfig config = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .addVariableInterpolator("a", numeric_config)
          .setAs("some.metric.name")
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
    assertEquals("e1", config.getId());
    assertEquals("some.metric.name", config.getAs());
    assertEquals("a + b", config.getExpression());
    assertEquals(JoinType.INNER, config.getJoin().getJoinType());
    assertEquals("host", config.getJoin().getJoins().get("host"));
    assertSame(numeric_config, config.interpolatorConfig(NumericType.TYPE));
    assertSame(numeric_config, config.getVariableInterpolators().get("a").get(0));
    
    config = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          //.setAs("some.metric.name") // defaults
          .addVariableInterpolator("a", numeric_config)
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
    assertEquals("e1", config.getId());
    assertEquals("e1", config.getAs());
    assertEquals("a + b", config.getExpression());
    assertEquals(JoinType.INNER, config.getJoin().getJoinType());
    assertEquals("host", config.getJoin().getJoins().get("host"));
    assertSame(numeric_config, config.interpolatorConfig(NumericType.TYPE));
    assertSame(numeric_config, config.getVariableInterpolators().get("a").get(0));
    
    try {
      ExpressionConfig.newBuilder()
          //.setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .setAs("e1")
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ExpressionConfig.newBuilder()
          .setExpression("")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .setAs("e1")
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ExpressionConfig.newBuilder()
          .setExpression("a + b")
          //.setJoinConfig((JoinConfig) JoinConfig.newBuilder()
          //    .addJoins("host", "host")
          //    .setType(JoinType.INNER)
          //    .setId("jc")
          //    .build())
          .setAs("e1")
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ExpressionConfig.newBuilder()
          .setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .setAs("e1")
          //.addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void toBuilder() throws Exception {
    NumericInterpolatorConfig numeric_config =
            (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
                    .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                    .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                    .setDataType(NumericType.TYPE.toString())
                    .build();

    ExpressionConfig config = (ExpressionConfig)
            ExpressionConfig.newBuilder()
                    .setExpression("a + b")
                    .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                            .addJoins("host", "host")
                            .setJoinType(JoinType.INNER)
                            .setId("jc")
                            .build())
                    .addVariableInterpolator("a", numeric_config)
                    .setAs("some.metric.name")
                    .addInterpolatorConfig(numeric_config)
                    .setSources(new ArrayList<String>(){{add("source1");}})
                    .addResultId(new DefaultQueryResultId("e1", "e1"))
                    .setId("e1")
                    .build();
    assertEquals("e1", config.getId());
    assertEquals("some.metric.name", config.getAs());
    assertEquals("a + b", config.getExpression());
    assertEquals(JoinType.INNER, config.getJoin().getJoinType());
    assertEquals("host", config.getJoin().getJoins().get("host"));
    assertSame(numeric_config, config.interpolatorConfig(NumericType.TYPE));
    assertSame(numeric_config, config.getVariableInterpolators().get("a").get(0));
    assertEquals(new DefaultQueryResultId("e1", "e1"), config.resultIds().get(0));

    final ExpressionConfig fromBuilder = config.toBuilder().build();

    assertEquals(fromBuilder.getId(), config.getId());
    assertEquals(fromBuilder.getAs(), config.getAs());
    assertEquals(fromBuilder.getExpression(), config.getExpression());
    assertEquals(fromBuilder.getJoin().getJoinType(), config.getJoin().getJoinType());

    assertFalse(fromBuilder.getJoin().getJoins() == config.getJoin().getJoins());
    assertEquals(fromBuilder.getJoin().getJoins(), config.getJoin().getJoins());

    assertFalse(fromBuilder.getSources() == config.getSources());
    assertEquals(fromBuilder.getSources(), config.getSources());

    assertFalse(fromBuilder.getInterpolatorConfigs() == config.getInterpolatorConfigs());
    assertEquals(numeric_config, fromBuilder.interpolatorConfig(NumericType.TYPE));

    assertFalse(fromBuilder.getVariableInterpolators() == config.getVariableInterpolators());
    assertSame(numeric_config, fromBuilder.getVariableInterpolators().get("a").get(0));

    assertEquals(new DefaultQueryResultId("e1", "e1"), fromBuilder.resultIds().get(0));
  }

  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    NumericInterpolatorConfig numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    NumericInterpolatorConfig numeric_config2 = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.NEXT_ONLY) // <-- DIFF
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    final ExpressionConfig c1 = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .addVariableInterpolator("a", numeric_config)
          .setAs("e1")
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
    
    ExpressionConfig c2 = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .addVariableInterpolator("a", numeric_config)
          .setAs("e1")
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    assertEquals(0, c1.compareTo(c2));
    
    c2 = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("b + a") // <-- DIFF (though we should be able to detect it some day)
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .addVariableInterpolator("a", numeric_config)
          .setAs("e1")
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "Hostname") // <-- DIFF
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .addVariableInterpolator("a", numeric_config)
          .setAs("e1")
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
    
    c2 = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .addVariableInterpolator("a", numeric_config2) // <-- DIFF
          .setAs("e1")
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          //.addVariableInterpolator("a", numeric_config)
          .setAs("e1")
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
    
    c2 = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .addVariableInterpolator("a", numeric_config)
          .setAs("e2") // <-- DIFF
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .addVariableInterpolator("a", numeric_config)
          .setAs("e1")
          .addInterpolatorConfig(numeric_config2) // <-- DIFF
          .setId("e1")
          .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
    
    c2 = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .addVariableInterpolator("a", numeric_config)
          .setAs("e1")
          .addInterpolatorConfig(numeric_config)
          .setId("e2") // <-- DIFF
          .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .addVariableInterpolator("a", numeric_config)
          .setAs("e1")
          .setInfectiousNan(true) // <-- DIFF
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
  }
  
  @Test
  public void interpolatorConfig() throws Exception {
    NumericInterpolatorConfig numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    NumericInterpolatorConfig numeric_config2 = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.ZERO)
      .setRealFillPolicy(FillWithRealPolicy.NONE)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    ExpressionConfig config = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .addVariableInterpolator("a", numeric_config2)
          .setAs("e1")
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
    
    assertSame(numeric_config, config.interpolatorConfig(NumericType.TYPE, null));
    assertSame(numeric_config, config.interpolatorConfig(NumericType.TYPE, ""));
    assertSame(numeric_config2, config.interpolatorConfig(NumericType.TYPE, "a"));
    assertNull(config.interpolatorConfig(NumericSummaryType.TYPE, null));
  }
  
  @Test
  public void serdes() throws Exception {
    NumericInterpolatorConfig numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    ExpressionConfig config = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("a + b")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .addVariableInterpolator("a", numeric_config)
          .setAs("some.metric.name")
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .addSource("m1")
          .addSource("m2")
          .build();
    
    final String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"e1\""));
    assertTrue(json.contains("\"expression\":\"a + b\""));
    assertTrue(json.contains("\"as\":\"some.metric.name\""));
    assertTrue(json.contains("\"infectiousNan\":false"));
    assertTrue(json.contains("\"join\":{"));
    assertTrue(json.contains("\"variableInterpolators\":{"));
    assertTrue(json.contains("\"interpolatorConfigs\":["));
    assertTrue(json.contains("\"type\":\"Expression\""));
    assertTrue(json.contains("\"sources\":[\"m1\",\"m2\"]"));
    
    MockTSDB tsdb = MockTSDBDefault.getMockTSDB();
    
    JsonNode node = JSON.getMapper().readTree(json);
    config = ExpressionConfig.parse(JSON.getMapper(), tsdb, node);
    
    assertEquals("e1", config.getId());
    assertEquals(2, config.getSources().size());
    assertEquals("m1", config.getSources().get(0));
    assertEquals("m2", config.getSources().get(1));
    assertEquals(ExpressionFactory.TYPE, config.getType());
    assertEquals("some.metric.name", config.getAs());
    assertEquals("a + b", config.getExpression());
    assertEquals(JoinType.INNER, config.getJoin().getJoinType());
    assertEquals("host", config.getJoin().getJoins().get("host"));
    assertTrue(config.interpolatorConfig(NumericType.TYPE) instanceof NumericInterpolatorConfig);
    assertTrue(config.getVariableInterpolators().get("a").get(0) instanceof NumericInterpolatorConfig);
  }
}
