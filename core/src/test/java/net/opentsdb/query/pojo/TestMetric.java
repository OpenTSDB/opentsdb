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

import net.opentsdb.utils.JSON;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class TestMetric {
  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenMetricIsNull() throws Exception {
    String json = "{\"id\":\"1\",\"filter\":\"2\","
        + "\"timeOffset\":\"1h-ago\",\"aggregator\":\"sum\","
        + "\"fillPolicy\":{\"policy\":\"nan\"}}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenMetricIsEmpty() throws Exception {
    String json = "{\"metric\":\"\",\"id\":\"1\",\"filter\":\"2\","
        + "\"timeOffset\":\"1h-ago\",\"aggregator\":\"sum\","
        + "\"fillPolicy\":{\"policy\":\"nan\"}}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenIDIsNull() throws Exception {
    String json = "{\"metric\":\"system.cpu\",\"id\":null,\"filter\":\"2\","
        + "\"timeOffset\":\"1h-ago\",\"aggregator\":\"sum\","
        + "\"fillPolicy\":{\"policy\":\"nan\"}}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenIDIsEmpty() throws Exception {
    String json = "{\"metric\":\"system.cpu\",\"id\":\"\",\"filter\":\"2\","
        + "\"timeOffset\":\"1h-ago\",\"aggregator\":\"sum\","
        + "\"fillPolicy\":{\"policy\":\"nan\"}}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenIDIsInvalid() throws Exception {
    String json = "{\"metric\":\"system.cpu\",\"id\":\"system.cpu\",\"filter\":\"2\","
        + "\"timeOffset\":\"1h-ago\",\"aggregator\":\"sum\","
        + "\"fillPolicy\":{\"policy\":\"nan\"}}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
  }
  
  @Test
  public void deserializeAllFields() throws Exception {
    String json = "{\"metric\":\"YAMAS.cpu.idle\",\"id\":\"e1\",\"filter\":\"f2\","
        + "\"timeOffset\":\"1h-ago\",\"aggregator\":\"sum\","
        + "\"fillPolicy\":{\"policy\":\"nan\"}}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
    Metric expectedMetric = Metric.newBuilder().setMetric("YAMAS.cpu.idle")
        .setId("e1").setFilter("f2").setTimeOffset("1h-ago")
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy(FillPolicy.NOT_A_NUMBER))
        .build();
    
    assertEquals(expectedMetric, metric);
  }

  @Test
  public void serialize() throws Exception {
    Metric metric = Metric.newBuilder().setMetric("YAMAS.cpu.idle")
        .setId("e1").setFilter("f2").setTimeOffset("1h-ago")
        .setFillPolicy(new NumericFillPolicy(FillPolicy.NOT_A_NUMBER))
        .build();

    String actual = JSON.serializeToString(metric);
    assertTrue(actual.contains("\"metric\":\"YAMAS.cpu.idle\""));
    assertTrue(actual.contains("\"id\":\"e1\""));
    assertTrue(actual.contains("\"filter\":\"f2\""));
    assertTrue(actual.contains("\"timeOffset\":\"1h-ago\""));
    assertTrue(actual.contains("\"fillPolicy\":{"));
  }

  @Test
  public void unknownShouldBeIgnored() throws Exception {
    String json = "{\"aggregator\":\"sum\",\"tags\":[\"foo\",\"bar\"]"
        + ",\"unknown\":\"garbage\"}";
    JSON.parseToObject(json, Metric.class);
    // pass if no unexpected exception
  }

  @Test(expected = IllegalArgumentException.class)
  public void validationtErrorWhenTimeOffsetIsInvalid() throws Exception {
    String json = "{\"metric\":\"YAMAS.cpu.idle\",\"id\":\"1\",\"filter\":\"2\","
        + "\"timeOffset\":\"what?\"}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void validationtErrorBadFill() throws Exception {
    String json = "{\"metric\":\"YAMAS.cpu.idle\",\"id\":\"1\",\"filter\":\"2\","
        + "\"fillPolicy\":{\"policy\":\"zero\",\"value\":42}}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
  }

  @Test
  public void build() throws Exception {
    final Metric metric = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        .setTimeOffset("1h-ago")
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .build())
        .build();
    final Metric clone = Metric.newBuilder(metric).build();
    assertNotSame(clone, metric);
    assertEquals("m1", clone.getId());
    assertEquals("f1", clone.getFilter());
    assertEquals("sys.cpu.user", clone.getMetric());
    assertEquals("1h-ago", clone.getTimeOffset());
    assertEquals("sum", clone.getAggregator());
    assertEquals(FillPolicy.NOT_A_NUMBER, clone.getFillPolicy().getPolicy());
    assertEquals("1m", clone.getDownsampler().getInterval());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final Metric m1 = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        .setTimeOffset("1h-ago")
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .build())
        .setIsRate(true)
        .setRateOptions(RateOptions.newBuilder()
            .setCounter(true)
            .setCounterMax(1024))
        .build();
    
    Metric m2 = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        .setTimeOffset("1h-ago")
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .build())
        .setIsRate(true)
        .setRateOptions(RateOptions.newBuilder()
            .setCounter(true)
            .setCounterMax(1024))
        .build();
    assertEquals(m1.hashCode(), m2.hashCode());
    assertEquals(m1, m2);
    assertEquals(0, m1.compareTo(m2));
    
    m2 = new Metric.Builder()
        .setId("m2")  // <-- diff
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        .setTimeOffset("1h-ago")
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .build())
        .setIsRate(true)
        .setRateOptions(RateOptions.newBuilder()
            .setCounter(true)
            .setCounterMax(1024))
        .build();
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m2);
    assertEquals(-1, m1.compareTo(m2));
    
    m2 = new Metric.Builder()
        .setId("m1")
        .setFilter("f2")  // <-- diff
        .setMetric("sys.cpu.user")
        .setTimeOffset("1h-ago")
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .build())
        .setIsRate(true)
        .setRateOptions(RateOptions.newBuilder()
            .setCounter(true)
            .setCounterMax(1024))
        .build();
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m2);
    assertEquals(-1, m1.compareTo(m2));
    
    m2 = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.sys")  // <-- diff
        .setTimeOffset("1h-ago")
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .build())
        .setIsRate(true)
        .setRateOptions(RateOptions.newBuilder()
            .setCounter(true)
            .setCounterMax(1024))
        .build();
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m2);
    assertEquals(1, m1.compareTo(m2));
    
    m2 = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        .setTimeOffset("2h-ago")  // <-- diff
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .build())
        .setIsRate(true)
        .setRateOptions(RateOptions.newBuilder()
            .setCounter(true)
            .setCounterMax(1024))
        .build();
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m2);
    assertEquals(-1, m1.compareTo(m2));
    
    m2 = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        //.setTimeOffset("1h-ago")  // <-- diff
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .build())
        .setIsRate(true)
        .setRateOptions(RateOptions.newBuilder()
            .setCounter(true)
            .setCounterMax(1024))
        .build();
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m2);
    assertEquals(1, m1.compareTo(m2));
    
    m2 = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        .setTimeOffset("1h-ago")
        .setAggregator("max")  // <-- diff
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .build())
        .setIsRate(true)
        .setRateOptions(RateOptions.newBuilder()
            .setCounter(true)
            .setCounterMax(1024))
        .build();
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m2);
    assertEquals(1, m1.compareTo(m2));
    
    m2 = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        .setTimeOffset("1h-ago")
        //.setAggregator("sum")  // <-- diff
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .build())
        .setIsRate(true)
        .setRateOptions(RateOptions.newBuilder()
            .setCounter(true)
            .setCounterMax(1024))
        .build();
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m2);
    assertEquals(1, m1.compareTo(m2));
    
    m2 = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        .setTimeOffset("1h-ago")
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.ZERO)  // <-- diff
            .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .build())
        .setIsRate(true)
        .setRateOptions(RateOptions.newBuilder()
            .setCounter(true)
            .setCounterMax(1024))
        .build();
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m2);
    assertEquals(-1, m1.compareTo(m2));
    
    m2 = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        .setTimeOffset("1h-ago")
        .setAggregator("sum")
        //.setFillPolicy(new NumericFillPolicy.Builder()  // <-- diff
        //    .setPolicy(FillPolicy.NOT_A_NUMBER)
        //    .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .build())
        .setIsRate(true)
        .setRateOptions(RateOptions.newBuilder()
            .setCounter(true)
            .setCounterMax(1024))
        .build();
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m2);
    assertEquals(1, m1.compareTo(m2));
    
    m2 = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        .setTimeOffset("1h-ago")
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("avg")  // <-- diff
            .setInterval("1m")
            .build())
        .setIsRate(true)
        .setRateOptions(RateOptions.newBuilder()
            .setCounter(true)
            .setCounterMax(1024))
        .build();
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m2);
    assertEquals(1, m1.compareTo(m2));
    
    m2 = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        .setTimeOffset("1h-ago")
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
//        .setDownsampler(Downsampler.newBuilder()  // <-- diff
//            .setAggregator("sum")
//            .setInterval("1m")
//            .build())
        .setIsRate(true)
        .setRateOptions(RateOptions.newBuilder()
            .setCounter(true)
            .setCounterMax(1024))
        .build();
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m2);
    assertEquals(1, m1.compareTo(m2));
    
    m2 = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        .setTimeOffset("1h-ago")
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .build())
        .setIsRate(true)
        .setRateOptions(RateOptions.newBuilder()
            .setCounter(true)
            .setCounterMax(32000))  // <-- diff
        .build();
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m2);
    assertEquals(-1, m1.compareTo(m2));
    
    m2 = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        .setTimeOffset("1h-ago")
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setDownsampler(Downsampler.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .build())
        .setIsRate(true)
//        .setRateOptions(RateOptions.newBuilder()  // <-- diff
//            .setCounter(true)
//            .setCounterMax(1024))
        .build();
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m2);
    assertEquals(1, m1.compareTo(m2));
  }
}
