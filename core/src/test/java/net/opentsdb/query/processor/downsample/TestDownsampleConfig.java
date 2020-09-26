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
package net.opentsdb.query.processor.downsample;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;

import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.utils.JSON;

public class TestDownsampleConfig {
  
  private NumericInterpolatorConfig numeric_config;
  private NumericSummaryInterpolatorConfig summary_config;
  
  @Before
  public void before() throws Exception {
    numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
    .setFillPolicy(FillPolicy.NOT_A_NUMBER)
    .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
    .setDataType(NumericType.TYPE.toString())
    .build();
    
    summary_config = 
        (NumericSummaryInterpolatorConfig) 
          NumericSummaryInterpolatorConfig.newBuilder()
      .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setDefaultRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
      .addExpectedSummary(0)
      .setDataType(NumericSummaryType.TYPE.toString())
      .build();
  }
  
  @Test
  public void ctor() throws Exception {
    DownsampleConfig config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setStart("1514843302")
        .setEnd("1514846902")
        .setMinInterval("5s")
        .setReportingInterval("1s")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    assertEquals("sum", config.getAggregator());
    assertEquals("foo", config.getId());
    assertEquals(Duration.of(15, ChronoUnit.SECONDS), config.interval());
    assertEquals(15, config.intervalPart());
    assertEquals("5s", config.getMinInterval());
    assertEquals(15, config.dpsInInterval());
    assertFalse(config.getFill());
    assertEquals(ZoneId.of("UTC"), config.timezone());
    assertEquals(ChronoUnit.SECONDS, config.units());
    assertFalse(config.getInfectiousNan());
    assertEquals(15, config.intervalPart());
    // snap forward
    assertEquals(1514843310, config.startTime().epoch());
    assertEquals(1514846895, config.endTime().epoch());
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        //.setId("foo")
        .setInterval("15s")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("")
        .setInterval("15s")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        //.setInterval("15s")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        //.setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("")
        .setId("foo")
        .setInterval("15s")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        //.setQueryInterpolationConfig(interpolation_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // run all
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setRunAll(true)
        .setStart("1514843302")
        .setEnd("1514846902")
        .addInterpolatorConfig(numeric_config)
        .build();
    assertEquals(1514843302, config.startTime().epoch());
    assertEquals(1514846902, config.endTime().epoch());
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setInterval("15s")
        .setTimeZone("America/Denver")
        .addInterpolatorConfig(numeric_config)
        .setId("foo")
        .build();
    assertEquals(ZoneId.of("America/Denver"), config.timezone());
  }

  @Test
  public void intervals() throws Exception {
    DownsampleConfig config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setStart("1514843302")
        .setEnd("1514846902")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    assertEquals(239, config.intervals());
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("0all")
        .setRunAll(true)
        .setStart("1514843302")
        .setEnd("1514846902")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    assertEquals(1, config.intervals());
  }

  @Test
  public void autoInterval() throws Exception {
    DownsampleFactory factory = new DownsampleFactory();
    MockTSDB tsdb = new MockTSDB();
    factory.initialize(tsdb, null).join(250);
    DownsampleConfig config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("auto")
        .setStart("1514843302")
        .setEnd("1514846902")
        .setIntervals(factory.intervals())
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .addSource("m1")
        .build();
    assertEquals("1m", config.getInterval());
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("auto")
        .setReportingInterval("10s")
        .setStart("1514843302")
        .setEnd("1514846902")
        .setIntervals(factory.intervals())
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .addSource("m1")
        .build();
    assertEquals("1m", config.getInterval());
    
    // delta to small for auto
    try {
      DownsampleConfig.newBuilder()
          .setAggregator("sum")
          .setId("foo")
          .setInterval("auto")
          .setStart("1514843302")
          .setEnd("1514843303")
          .setIntervals(factory.intervals())
          .addInterpolatorConfig(numeric_config)
          .addInterpolatorConfig(summary_config)
          .addSource("m1")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // not configured
    factory = new DownsampleFactory();
    try {
      DownsampleConfig.newBuilder()
          .setAggregator("sum")
          .setId("foo")
          .setInterval("auto")
          .setStart("1514843302")
          .setEnd("1514846902")
          .setIntervals(factory.intervals())
          .addInterpolatorConfig(numeric_config)
          .addInterpolatorConfig(summary_config)
          .addSource("m1")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
  }
  
  @Test
  public void serdes() throws Exception {
    DownsampleConfig config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setMinInterval("5s")
        .setReportingInterval("1s")
        .setStart("1514843302")
        .setEnd("1514846902")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .addSource("m1")
        .build();
    final String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"foo\""));
    assertTrue(json.contains("\"interval\":\"15s\""));
    assertTrue(json.contains("\"minInterval\":\"5s\""));
    assertTrue(json.contains("\"reportingInterval\":\"1s\""));
    assertTrue(json.contains("\"timezone\":\"UTC\""));
    assertTrue(json.contains("\"aggregator\":\"sum\""));
    assertTrue(json.contains("\"fill\":false"));
    assertTrue(json.contains("\"infectiousNan\":false"));
    assertTrue(json.contains("\"runAll\":false"));
    assertTrue(json.contains("\"type\":\"Downsample\""));
    assertTrue(json.contains("\"sources\":[\"m1\"]"));
    assertTrue(json.contains("\"interpolatorConfigs\":["));
    assertTrue(json.contains("\"fillPolicy\":\"nan\""));
    assertTrue(json.contains("\"realFillPolicy\":\"PREFER_NEXT\""));
    assertTrue(json.contains("\"dataType\":\"net.opentsdb.data.types.numeric.NumericType\""));
    assertTrue(json.contains("\"defaultRealFillPolicy\":\"NEXT_ONLY\""));
    assertTrue(json.contains("\"expectedSummaries\":[0]"));
    assertTrue(json.contains("\"start\":\"1514843302\""));

    
    MockTSDB tsdb = MockTSDBDefault.getMockTSDB();
    
    JsonNode node = JSON.getMapper().readTree(json);
    DownsampleFactory factory = new DownsampleFactory();
    config = (DownsampleConfig) factory.parseConfig(JSON.getMapper(), tsdb, node);
    
    assertEquals("sum", config.getAggregator());
    assertEquals("foo", config.getId());
    assertEquals(Duration.of(15, ChronoUnit.SECONDS), config.interval());
    assertEquals(15, config.intervalPart());
    assertEquals("5s", config.getMinInterval());
    assertEquals(15, config.dpsInInterval());
    assertFalse(config.getFill());
    assertEquals(ZoneId.of("UTC"), config.timezone());
    assertEquals(ChronoUnit.SECONDS, config.units());
    assertFalse(config.getInfectiousNan());
    assertEquals(15, config.intervalPart());
    assertEquals(1, config.getSources().size());
    assertEquals("m1", config.getSources().get(0));
    assertEquals(DownsampleFactory.TYPE, config.getType());
    assertEquals(FillPolicy.NOT_A_NUMBER, 
        ((NumericInterpolatorConfig) config.interpolatorConfig(NumericType.TYPE))
          .getFillPolicy());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        ((NumericInterpolatorConfig) config.interpolatorConfig(NumericType.TYPE))
          .getRealFillPolicy());
    assertEquals(FillPolicy.NOT_A_NUMBER, 
        ((NumericSummaryInterpolatorConfig) config.interpolatorConfig(NumericSummaryType.TYPE))
          .getDefaultFillPolicy());
    assertEquals(FillWithRealPolicy.NEXT_ONLY, 
        ((NumericSummaryInterpolatorConfig) config.interpolatorConfig(NumericSummaryType.TYPE))
          .getDefaultRealFillPolicy());
    assertSame(factory.intervals(), config.autoIntervals());
  }

  @Test
  public void toBuilder() throws Exception {
    DownsampleConfig original = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setStart("1514843302")
        .setEnd("1514846902")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    
    DownsampleConfig config = (DownsampleConfig) original.toBuilder().build();
    assertEquals("sum", config.getAggregator());
    assertEquals("foo", config.getId());
    assertEquals(Duration.of(15, ChronoUnit.SECONDS), config.interval());
    assertEquals(15, config.intervalPart());
    assertFalse(config.getFill());
    assertEquals(ZoneId.of("UTC"), config.timezone());
    assertEquals(ChronoUnit.SECONDS, config.units());
    assertFalse(config.getInfectiousNan());
    assertEquals(15, config.intervalPart());
    // snap forward
    assertEquals(1514843310, config.startTime().epoch());
    assertEquals(1514846895, config.endTime().epoch());
  }
  
  @Test
  public void toBuilderRunAll() throws Exception {
    DownsampleConfig original = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("0all")
        .setRunAll(true)
        .setStart("1514843302")
        .setEnd("1514846902")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    
    DownsampleConfig config = (DownsampleConfig) original.toBuilder().build();
    assertEquals("sum", config.getAggregator());
    assertEquals("foo", config.getId());
    assertNull(config.interval());
    assertEquals(0, config.intervalPart());
    assertFalse(config.getFill());
    assertEquals(ZoneId.of("UTC"), config.timezone());
    assertNull(config.units());
    assertFalse(config.getInfectiousNan());
    assertTrue(config.getRunAll());
    // snap forward
    assertEquals(1514843302, config.startTime().epoch());
    assertEquals(1514846902, config.endTime().epoch());
  }
  
  @Test
  public void equality() throws Exception {
    DownsampleConfig config = (DownsampleConfig) DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setId("foo")
            .setInterval("15s")
            .setStart("1514843302")
            .setEnd("1514846902")
            .addInterpolatorConfig(numeric_config)
            .addInterpolatorConfig(summary_config)
            .build();

    DownsampleConfig config2 = (DownsampleConfig) DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setId("foo")
            .setInterval("15s")
            .setStart("1514843302")
            .setEnd("1514846902")
            .addInterpolatorConfig(numeric_config)
            .addInterpolatorConfig(summary_config)
            .build();

    DownsampleConfig config3 = (DownsampleConfig) DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setId("foo")
            .setInterval("15s")
            .setStart("1514843302")
            .setEnd("1514846902")
            .addInterpolatorConfig(numeric_config)
            .addInterpolatorConfig(summary_config)
            .build();


    assertTrue(config.equals(config2));
    assertTrue(!config.equals(config3));
    assertEquals(config.hashCode(), config2.hashCode());
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (DownsampleConfig) DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setId("boo")
            .setInterval("15s")
            .setStart("1514843302")
            .setEnd("1514846902")
            .addInterpolatorConfig(numeric_config)
            .addInterpolatorConfig(summary_config)
            .build();

    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (DownsampleConfig) DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setId("foo")
            .setInterval("1s")
            .setStart("1514843302")
            .setEnd("1514846902")
            .addInterpolatorConfig(numeric_config)
            .addInterpolatorConfig(summary_config)
            .build();

    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (DownsampleConfig) DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setId("foo")
            .setInterval("15s")
            .setStart("151484330")
            .setEnd("1514846902")
            .addInterpolatorConfig(numeric_config)
            .addInterpolatorConfig(summary_config)
            .build();

    assertFalse(!config.equals(config3));
    assertEquals(config.hashCode(), config3.hashCode());

    config3 = (DownsampleConfig) DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setId("foo")
            .setInterval("15s")
            .setStart("1514843302")
            .setEnd("1514846905")
            .addInterpolatorConfig(numeric_config)
            .addInterpolatorConfig(summary_config)
            .build();

    assertFalse(!config.equals(config3));
    assertEquals(config.hashCode(), config3.hashCode());

    config3 = (DownsampleConfig) DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setId("boo")
            .setTimeZone("UTC")
            .setInterval("15s")
            .setStart("1514843302")
            .setEnd("1514846902")
            .addInterpolatorConfig(numeric_config)
            .addInterpolatorConfig(summary_config)
            .build();

    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

  }

  @Test
  public void implicitRunall() throws Exception {
    DownsampleConfig config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("5m")
        .setStart("1514764835")
        .setEnd("1514765135")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    assertTrue(config.getRunAll());
    assertEquals(1514764835, config.startTime().epoch());
    assertEquals(1514765135, config.endTime().epoch());
    assertEquals(1, config.intervals());
    assertNull(config.interval());
    assertEquals(0, config.dpsInInterval());
  }

  @Test
  public void implicitRunAll0All() throws Exception {
    DownsampleConfig config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("0all")
        .setStart("1514843302")
        .setEnd("1514846902")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    assertTrue(config.getRunAll());
    assertEquals(1514843302, config.startTime().epoch());
    assertEquals(1514846902, config.endTime().epoch());
    assertEquals(1, config.intervals());
    assertNull(config.interval());
    assertEquals(0, config.dpsInInterval());
  }
  
  @Test
  public void testSnapStartAndEndTime() {

    long start = 1546329600; // Tuesday, January 1, 2019 8:00:00 AM
    long end = 1546333200; // Tuesday, January 1, 2019 9:00:00 AM

    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setId("boo")
            .setTimeZone("UTC")
            .setInterval("1m")
            .setStart(String.valueOf(start + 2))
            .setEnd(String.valueOf(end + 2))
            .addInterpolatorConfig(numeric_config)
            .build();

    assertEquals(start + 60, config.startTime().epoch());
    assertEquals(end, config.endTime().epoch());
  }

  @Test
  public void testDoesNotSnapStartAndEndTimeIfRunAll() {

    long start = 1546329602; // Tuesday, January 1, 2019 8:00:02 AM
    long end = 1546333200; // Tuesday, January 1, 2019 9:00:02 AM

    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setId("boo")
            .setTimeZone("UTC")
            .setInterval("1m")
            .setRunAll(true)
            .setStart(String.valueOf(start))
            .setEnd(String.valueOf(end))
            .addInterpolatorConfig(numeric_config)
            .build();

    assertEquals(start, config.startTime().epoch());
    assertEquals(end, config.endTime().epoch());
    assertNull(config.interval());
  }

  @Test
  public void testIntervalIsNullIfRunAll() {

    long start = 1546329602; // Tuesday, January 1, 2019 8:00:02 AM
    long end = 1546333200; // Tuesday, January 1, 2019 9:00:02 AM

    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setId("boo")
            .setTimeZone("UTC")
            .setInterval("1m")
            .setRunAll(true)
            .setStart(String.valueOf(start))
            .setEnd(String.valueOf(end))
            .addInterpolatorConfig(numeric_config)
            .build();
    assertNull(config.interval());
  }

}
