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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import net.opentsdb.core.FillPolicy;
import net.opentsdb.utils.JSON;

import org.junit.Test;

public class TestNumericFillPolicy {

  @Test
  public void builder() throws Exception {
    NumericFillPolicy nfp = NumericFillPolicy.Builder()
        .setPolicy(FillPolicy.NOT_A_NUMBER).build();
    assertEquals(FillPolicy.NOT_A_NUMBER, nfp.getPolicy());
    assertTrue(Double.isNaN((Double)nfp.getValue()));
    
    nfp = NumericFillPolicy.Builder()
        .setPolicy(null).build();
    assertEquals(FillPolicy.ZERO, nfp.getPolicy());
    assertEquals(0, nfp.getValue(), 0.0001);
  }
  
  @Test
  public void policyCtor() throws Exception {
    NumericFillPolicy nfp = new NumericFillPolicy(FillPolicy.NONE);
    assertTrue(Double.isNaN((Double)nfp.getValue()));
    assertEquals(FillPolicy.NONE, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(FillPolicy.NOT_A_NUMBER);
    assertTrue(Double.isNaN((Double)nfp.getValue()));
    assertEquals(FillPolicy.NOT_A_NUMBER, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(FillPolicy.NULL);
    assertTrue(Double.isNaN((Double)nfp.getValue()));
    assertEquals(FillPolicy.NULL, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(FillPolicy.ZERO);
    assertEquals(0, nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.ZERO, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(null);
    assertEquals(0, nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.ZERO, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(FillPolicy.SCALAR);
    assertEquals(0, nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.SCALAR, nfp.getPolicy());

  }
  
  @Test
  public void policyAndValueCtor() throws Exception {
    NumericFillPolicy nfp = new NumericFillPolicy(FillPolicy.NONE, Double.NaN);
    assertTrue(Double.isNaN((Double)nfp.getValue()));
    assertEquals(FillPolicy.NONE, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(FillPolicy.NOT_A_NUMBER, Double.NaN);
    assertTrue(Double.isNaN((Double)nfp.getValue()));
    assertEquals(FillPolicy.NOT_A_NUMBER, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(FillPolicy.NULL, 0);
    assertTrue(Double.isNaN((Double)nfp.getValue()));
    assertEquals(FillPolicy.NULL, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(FillPolicy.ZERO, 0);
    assertEquals(0, nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.ZERO, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(null, 0);
    assertEquals(0, nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.ZERO, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(FillPolicy.SCALAR, 42);
    assertEquals(42, nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.SCALAR, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(FillPolicy.SCALAR, 0);
    assertEquals(0, nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.SCALAR, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(FillPolicy.SCALAR, Double.NaN);
    assertTrue(Double.isNaN((Double)nfp.getValue()));
    assertEquals(FillPolicy.SCALAR, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(FillPolicy.SCALAR, 42.5);
    assertEquals(42.5, (Double)nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.SCALAR, nfp.getPolicy());
    
    // defaults from value
    nfp = new NumericFillPolicy(null, Double.NaN);
    assertTrue(Double.isNaN((Double)nfp.getValue()));
    assertEquals(FillPolicy.NOT_A_NUMBER, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(null, 42);
    assertEquals(42, nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.SCALAR, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(null, 42.5);
    assertEquals(42.5, (Double)nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.SCALAR, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(null, -42.5);
    assertEquals(-42.5, (Double)nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.SCALAR, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(null, Double.NaN);
    assertTrue(Double.isNaN((Double)nfp.getValue()));
    assertEquals(FillPolicy.NOT_A_NUMBER, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(null, 0);
    assertEquals(0, nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.ZERO, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(null, 0.0);
    assertEquals(0.0, (Double)nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.ZERO, nfp.getPolicy());
     
    nfp = new NumericFillPolicy(null, -0.0);
    assertEquals(-0.0, (Double)nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.ZERO, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(null, -0);
    assertEquals(-0, nfp.getValue(), 0.0001);
    assertEquals(FillPolicy.ZERO, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(FillPolicy.NOT_A_NUMBER, 0);
    assertTrue(Double.isNaN((Double)nfp.getValue()));
    assertEquals(FillPolicy.NOT_A_NUMBER, nfp.getPolicy());
    
    nfp = new NumericFillPolicy(FillPolicy.NULL, Double.NaN);
    assertTrue(Double.isNaN((Double)nfp.getValue()));
    assertEquals(FillPolicy.NULL, nfp.getPolicy());
    
    // inappropriate combos
    try {
      nfp = new NumericFillPolicy(FillPolicy.ZERO, 42);
      fail("expected an IllegalArgumentException");
    } catch (IllegalArgumentException iae) { }
    
    try {
      nfp = new NumericFillPolicy(FillPolicy.NONE, 42);
      fail("expected an IllegalArgumentException");
    } catch (IllegalArgumentException iae) { }
    
    try {
      nfp = new NumericFillPolicy(FillPolicy.NULL, 42);
      fail("expected an IllegalArgumentException");
    } catch (IllegalArgumentException iae) { }
    
    try {
      nfp = new NumericFillPolicy(FillPolicy.NOT_A_NUMBER, 42);
      fail("expected an IllegalArgumentException");
    } catch (IllegalArgumentException iae) { }

  }

  @Test
  public void serdes() throws Exception {
    
    NumericFillPolicy ser_nfp = new NumericFillPolicy(FillPolicy.NONE);
    String json = JSON.serializeToString(ser_nfp);
    assertTrue(json.contains("\"policy\":\"none\""));
    assertTrue(json.contains("\"value\":\"NaN\""));
    NumericFillPolicy des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertTrue(des_nfp != ser_nfp);
    assertTrue(des_nfp.equals(ser_nfp));
    
    ser_nfp = new NumericFillPolicy(FillPolicy.ZERO);
    json = JSON.serializeToString(ser_nfp);
    assertTrue(json.contains("\"policy\":\"zero\""));
    assertTrue(json.contains("\"value\":0"));
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertTrue(des_nfp != ser_nfp);
    assertTrue(des_nfp.equals(ser_nfp));
    
    ser_nfp = new NumericFillPolicy(FillPolicy.NOT_A_NUMBER);
    json = JSON.serializeToString(ser_nfp);
    assertTrue(json.contains("\"policy\":\"nan\""));
    assertTrue(json.contains("\"value\":\"NaN\""));
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertTrue(des_nfp != ser_nfp);
    assertTrue(des_nfp.equals(ser_nfp));
    
    ser_nfp = new NumericFillPolicy(FillPolicy.NULL);
    json = JSON.serializeToString(ser_nfp);
    assertTrue(json.contains("\"policy\":\"null\""));
    assertTrue(json.contains("\"value\":\"NaN\""));
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertTrue(des_nfp != ser_nfp);
    assertTrue(des_nfp.equals(ser_nfp));
    
    ser_nfp = new NumericFillPolicy(FillPolicy.SCALAR, 42);
    json = JSON.serializeToString(ser_nfp);
    assertTrue(json.contains("\"policy\":\"scalar\""));
    assertTrue(json.contains("\"value\":42"));
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertTrue(des_nfp != ser_nfp);
    assertTrue(des_nfp.equals(ser_nfp));
    
    ser_nfp = new NumericFillPolicy(FillPolicy.SCALAR, 42.5);
    json = JSON.serializeToString(ser_nfp);
    assertTrue(json.contains("\"policy\":\"scalar\""));
    assertTrue(json.contains("\"value\":42.5"));
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertTrue(des_nfp != ser_nfp);
    assertTrue(des_nfp.equals(ser_nfp));
    
    ser_nfp = new NumericFillPolicy(FillPolicy.SCALAR, -42.5);
    json = JSON.serializeToString(ser_nfp);
    assertTrue(json.contains("\"policy\":\"scalar\""));
    assertTrue(json.contains("\"value\":-42.5"));
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertTrue(des_nfp != ser_nfp);
    assertTrue(des_nfp.equals(ser_nfp));
    
    json = "{\"policy\":\"zero\"}";
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertEquals(FillPolicy.ZERO, des_nfp.getPolicy());
    assertEquals(0, des_nfp.getValue(), 0.0001);
    
    json = "{\"policy\":\"nan\"}";
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertEquals(FillPolicy.NOT_A_NUMBER, des_nfp.getPolicy());
    assertTrue(Double.isNaN((Double)des_nfp.getValue()));
    
    json = "{\"policy\":\"scalar\"}";
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertEquals(FillPolicy.SCALAR, des_nfp.getPolicy());
    assertEquals(0, des_nfp.getValue(), 0.0001);
    
    json = "{\"policy\":\"none\"}";
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertEquals(FillPolicy.NONE, des_nfp.getPolicy());
    assertTrue(Double.isNaN((Double)des_nfp.getValue()));
    
    json = "{\"policy\":\"null\"}";
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertEquals(FillPolicy.NULL, des_nfp.getPolicy());
    assertTrue(Double.isNaN((Double)des_nfp.getValue()));
    
    json = "{\"policy\":\"scalar\",\"value\":42}";
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertEquals(FillPolicy.SCALAR, des_nfp.getPolicy());
    assertEquals(42, des_nfp.getValue(), 0.0001);
    
    json = "{\"policy\":\"scalar\",\"value\":\"42\"}";
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertEquals(FillPolicy.SCALAR, des_nfp.getPolicy());
    assertEquals(42, des_nfp.getValue(), 0.0001);
    
    json = "{\"policy\":\"scalar\",\"value\":42.5}";
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertEquals(FillPolicy.SCALAR, des_nfp.getPolicy());
    assertEquals(42.5, (Double)des_nfp.getValue(), 0.0001);
    
    json = "{\"policy\":\"nan\",\"value\":NaN}";
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertEquals(FillPolicy.NOT_A_NUMBER, des_nfp.getPolicy());
    assertTrue(Double.isNaN((Double)des_nfp.getValue()));
    
    json = "{\"policy\":\"scalar\",\"value\":0}";
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertEquals(FillPolicy.SCALAR, des_nfp.getPolicy());
    assertEquals(0, des_nfp.getValue(), 0.0001);
    
    json = "{\"policy\":\"scalar\",\"value\":0.0}";
    des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
    assertEquals(FillPolicy.SCALAR, des_nfp.getPolicy());
    assertEquals(0.0, (Double)des_nfp.getValue(), 0.0001);
    
    try {
      json = "{\"policy\":\"unknown\"}";
      des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
      fail("Expected a IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      json = "{\"policy\":\"scalar\",value\":\"foo\"}";
      des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
      fail("Expected a IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      json = "{\"policy\":\"badjson";
      des_nfp = JSON.parseToObject(json, NumericFillPolicy.class);
      fail("Expected a IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
