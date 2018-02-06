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
package net.opentsdb.query.pojo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import net.opentsdb.utils.JSON;

public class TestRateOptions {

  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenIntervalIsInvalid() throws Exception {
    RateOptions.newBuilder()
      .setInterval("")
    .build()
    .validate();
  }
  
  @Test
  public void serdes() throws Exception {
    RateOptions options = RateOptions.newBuilder()
        .build();
    String json = JSON.serializeToString(options);
    // defaults are empty
    assertEquals("{}", json);
    
    options = RateOptions.newBuilder()
        .setCounter(true)
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        .build();
    json = JSON.serializeToString(options);
    assertTrue(json.contains("\"counter\":true"));
    assertTrue(json.contains("\"interval\":\"60s\""));
    assertTrue(json.contains("\"counterMax\":" + Integer.MAX_VALUE));
    
    json = "{\"counter\":true,\"interval\":\"15s\",\"counterMax\":16,"
        + "\"resetValue\":-1,\"dropResets\":true,\"someOtherField\":\"Boo!\"}";
    options = JSON.parseToObject(json, RateOptions.class);
    assertTrue(options.isCounter());
    assertTrue(options.getDropResets());
    assertEquals("15s", options.getInterval());
    assertEquals(-1, options.getResetValue());
    assertEquals(16, options.getCounterMax());
  }
  
  @Test
  public void build() throws Exception {
    final RateOptions options = RateOptions.newBuilder()
        .setCounter(true)
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        .build();
    final RateOptions clone = RateOptions.newBuilder(options).build();
    assertTrue(clone.isCounter());
    assertFalse(clone.getDropResets());
    assertEquals("60s", clone.getInterval());
    assertEquals(0, clone.getResetValue());
    assertEquals(Integer.MAX_VALUE, clone.getCounterMax());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final RateOptions r1 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .build();
    
    RateOptions r2 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .build();
    assertEquals(r1.hashCode(), r2.hashCode());
    assertEquals(r1, r2);
    assertEquals(0, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        //.setCounter(true) // <-- Diff
        .setDropResets(true)
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        .setCounter(true)
        //.setDropResets(true) // <-- Diff
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval("15s") // <-- Diff
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(1, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        //.setInterval("60s") // <-- Diff
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(1, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval("60s")
        .setCounterMax(Short.MAX_VALUE) // <-- Diff
        .setResetValue(-1)
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(1, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval("60s")
        //.setCounterMax(Integer.MAX_VALUE) // <-- Diff
        .setResetValue(-1)
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(100) // <-- Diff
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
    
    r2 = RateOptions.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        //.setResetValue(-1) // <-- Diff
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
  }
  
}
