// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.rate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.utils.JSON;

public class TestRateConfig {
public static MockTSDB TSDB;
  
  @BeforeClass
  public static void beforeClass() {
    TSDB = mock(MockTSDB.class);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenIntervalIsInvalid() throws Exception {
    RateConfig.newBuilder()
      .setInterval("")
    .build()
    .validate(TSDB);
  }
  
  @Test
  public void serdes() throws Exception {
    RateConfig options = (RateConfig) RateConfig.newBuilder()
        .setCounter(true)
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        .setDropResets(true)
        .setDeltaOnly(true)
        .setId("rate")
        .build();
    String json = JSON.serializeToString(options);
    assertTrue(json.contains("\"counter\":true"));
    assertTrue(json.contains("\"interval\":\"60s\""));
    assertTrue(json.contains("\"counterMax\":" + Integer.MAX_VALUE));
    assertTrue(json.contains("\"dropResets\":true"));
    assertTrue(json.contains("\"deltaOnly\":true"));
    
    json = "{\"id\":\"rate\",\"type\":\"Rate\",\"counter\":true,"
        + "\"interval\":\"60s\",\"dropResets\":true,\"counterMax\":2147483647,"
        + "\"deltaOnly\":true}";
    options = JSON.parseToObject(json, RateConfig.class);
    assertTrue(options.isCounter());
    assertTrue(options.getDropResets());
    assertEquals("60s", options.getInterval());
    assertEquals(Integer.MAX_VALUE, options.getCounterMax());
    assertTrue(options.getDropResets());
    assertTrue(options.getDeltaOnly());
  }
  
  @Test
  public void build() throws Exception {
    final RateConfig options = (RateConfig) RateConfig.newBuilder()
        .setCounter(true)
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        .setId("rate")
        .build();
    final RateConfig clone = (RateConfig) RateConfig.newBuilder(options)
        .setId("rate")
        .build();
    assertTrue(clone.isCounter());
    assertFalse(clone.getDropResets());
    assertEquals("60s", clone.getInterval());
    assertEquals(0, clone.getResetValue());
    assertEquals(Integer.MAX_VALUE, clone.getCounterMax());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final RateConfig r1 = (RateConfig) RateConfig.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .setId("rate")
        .build();
    
    RateConfig r2 = (RateConfig) RateConfig.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .setId("rate")
        .build();
    assertEquals(r1.hashCode(), r2.hashCode());
    assertEquals(r1, r2);
    assertEquals(0, r1.compareTo(r2));
    
    r2 = (RateConfig) RateConfig.newBuilder()
        //.setCounter(true) // <-- Diff
        .setDropResets(true)
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .setId("rate")
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
    
    r2 = (RateConfig) RateConfig.newBuilder()
        .setCounter(true)
        //.setDropResets(true) // <-- Diff
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .setId("rate")
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
    
    r2 = (RateConfig) RateConfig.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval("15s") // <-- Diff
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .setId("rate")
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(1, r1.compareTo(r2));
    
    r2 = (RateConfig) RateConfig.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        //.setInterval("60s") // <-- Diff
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(-1)
        .setId("rate")
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(1, r1.compareTo(r2));
    
    r2 = (RateConfig) RateConfig.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval("60s")
        .setCounterMax(Short.MAX_VALUE) // <-- Diff
        .setResetValue(-1)
        .setId("rate")
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(1, r1.compareTo(r2));
    
    r2 = (RateConfig) RateConfig.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval("60s")
        //.setCounterMax(Integer.MAX_VALUE) // <-- Diff
        .setResetValue(-1)
        .setId("rate")
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
    
    r2 = (RateConfig) RateConfig.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        .setResetValue(100) // <-- Diff
        .setId("rate")
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
    
    r2 = (RateConfig) RateConfig.newBuilder()
        .setCounter(true)
        .setDropResets(true)
        .setInterval("60s")
        .setCounterMax(Integer.MAX_VALUE)
        //.setResetValue(-1) // <-- Diff
        .setId("rate")
        .build();
    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertNotEquals(r1, r2);
    assertEquals(-1, r1.compareTo(r2));
  }
  
  @Test
  public void autoInterval() throws Exception {
    RateFactory factory = new RateFactory();
    MockTSDB tsdb = new MockTSDB();
    factory.initialize(tsdb, null).join(250);
    RateConfig config = (RateConfig) RateConfig.newBuilder()
        .setInterval("auto")
        .setFactory(factory)
        .setStartTime(new SecondTimeStamp(1514843302))
        .setEndTime(new SecondTimeStamp(1514843303))
        .addSource("m1")
        .setId("foo")
        .build();
    assertEquals("1m", config.getInterval());
    
    // not configured
    factory = new RateFactory();
    try {
      config = (RateConfig) RateConfig.newBuilder()
          .setInterval("auto")
          .setFactory(factory)
          .setStartTime(new SecondTimeStamp(1514843302))
          .setEndTime(new SecondTimeStamp(1514843303))
          .addSource("m1")
          .setId("foo")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
  }
}
