// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.interpolation.types.numeric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.ScalarNumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.utils.JSON;

public class TestScalarNumericInterpolatorConfig {

  @Test
  public void build() throws Exception {
    NumericInterpolatorConfig config = (NumericInterpolatorConfig) 
        ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
    assertTrue(((ScalarNumericInterpolatorConfig) config).isInteger());
    assertEquals(42, ((ScalarNumericInterpolatorConfig) config).longValue());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, config.getRealFillPolicy());
    
    config = (NumericInterpolatorConfig) ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42.5D)
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
    assertFalse(((ScalarNumericInterpolatorConfig) config).isInteger());
    assertEquals(42.5, ((ScalarNumericInterpolatorConfig) config).doubleValue(), 0.01);
    assertEquals(FillWithRealPolicy.PREFER_NEXT, config.getRealFillPolicy());
    
    config = (NumericInterpolatorConfig) ScalarNumericInterpolatorConfig.newBuilder()
        //.setValue(42) <== defaults to 0
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
    assertTrue(((ScalarNumericInterpolatorConfig) config).isInteger());
    assertEquals(0, ((ScalarNumericInterpolatorConfig) config).longValue());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, config.getRealFillPolicy());
    
    try {
      ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(null)
        .setDataType(NumericType.TYPE.toString())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        //.setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        .setFillPolicy(null)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        //.setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void serdes() throws Exception {
    NumericInterpolatorConfig config = (NumericInterpolatorConfig) 
        ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .setType("Default")
        .build();
    
    String json_int = JSON.serializeToString(config);
    assertTrue(json_int.contains("\"type\":\"Default\""));
    assertTrue(json_int.contains("\"dataType\":\"net.opentsdb.data.types.numeric.NumericType\""));
    assertTrue(json_int.contains("\"fillPolicy\":\"SCALAR\""));
    assertTrue(json_int.contains("\"realFillPolicy\":\"PREFER_NEXT\""));
    assertTrue(json_int.contains("\"value\":42"));
    
    config = (NumericInterpolatorConfig) 
        ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42.75)
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
    
    String json_double = JSON.serializeToString(config);
    assertFalse(json_double.contains("\"type\":\"Default\""));
    assertTrue(json_double.contains("\"dataType\":\"net.opentsdb.data.types.numeric.NumericType\""));
    assertTrue(json_double.contains("\"fillPolicy\":\"SCALAR\""));
    assertTrue(json_double.contains("\"realFillPolicy\":\"PREFER_NEXT\""));
    assertTrue(json_double.contains("\"value\":42.75"));
    
    TSDB tsdb = mock(TSDB.class);
    JsonNode node = JSON.getMapper().readTree(json_int);
    config = (NumericInterpolatorConfig) ScalarNumericInterpolatorConfig
        .parse(JSON.getMapper(), tsdb, node);
    assertTrue(((ScalarNumericInterpolatorConfig) config).isInteger());
    assertEquals(42, ((ScalarNumericInterpolatorConfig) config).longValue());
    assertEquals(FillPolicy.SCALAR, config.getFillPolicy());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, config.getRealFillPolicy());
    assertEquals("net.opentsdb.data.types.numeric.NumericType", config.getDataType());
    assertEquals("Default", config.getType());
    
    node = JSON.getMapper().readTree(json_double);
    config = (NumericInterpolatorConfig) ScalarNumericInterpolatorConfig
        .parse(JSON.getMapper(), tsdb, node);
    assertFalse(((ScalarNumericInterpolatorConfig) config).isInteger());
    assertEquals(42.75, ((ScalarNumericInterpolatorConfig) config).doubleValue(), 0.001);
    assertEquals(FillPolicy.SCALAR, config.getFillPolicy());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, config.getRealFillPolicy());
    assertEquals("net.opentsdb.data.types.numeric.NumericType", config.getDataType());
    assertNull(config.getType());
  }
  
}
