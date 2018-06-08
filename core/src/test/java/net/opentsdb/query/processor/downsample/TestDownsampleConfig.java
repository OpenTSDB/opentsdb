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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;

public class TestDownsampleConfig {
  
  private NumericInterpolatorConfig numeric_config;
  private NumericSummaryInterpolatorConfig summary_config;
  
  @Before
  public void before() throws Exception {
    numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
    .setFillPolicy(FillPolicy.NOT_A_NUMBER)
    .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
    .setType(NumericType.TYPE.toString())
    .build();
    
    summary_config = 
        (NumericSummaryInterpolatorConfig) 
          NumericSummaryInterpolatorConfig.newBuilder()
      .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setDefaultRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
      .addExpectedSummary(0)
      .setType(NumericSummaryType.TYPE.toString())
      .build();
  }
  
  @Test
  public void ctor() throws Exception {
    TimeSeriesQuery q = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1970/01/01-00:00:01")
            .setEnd("1970/01/01-00:01:00")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build();
    
    DownsampleConfig config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    assertEquals("sum", config.aggregator());
    assertEquals("foo", config.getId());
    assertEquals(Duration.of(15, ChronoUnit.SECONDS), config.interval());
    assertEquals(15, config.intervalPart());
    assertFalse(config.fill());
    assertSame(q, config.query());
    assertEquals(ZoneId.of("UTC"), config.timezone());
    assertEquals(ChronoUnit.SECONDS, config.units());
    assertEquals(15000, config.start().msEpoch());
    assertEquals(60000, config.end().msEpoch());
    assertFalse(config.infectiousNan());
    assertEquals(15, config.intervalPart());
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        //.setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("")
        .setInterval("15s")
        .setQuery(q)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        //.setInterval("15s")
        .setQuery(q)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("")
        .setQuery(q)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        //.setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        //.setQueryInterpolationConfig(interpolation_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        //.setQuery(q)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // ru all
    q = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1970/01/01-00:00:01")
            .setEnd("1970/01/01-00:01:00")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build();
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .setRunAll(true)
        .addInterpolatorConfig(numeric_config)
        .build();
    assertEquals(1000, config.start().msEpoch());
    assertEquals(60000, config.end().msEpoch());
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .setTimeZone(ZoneId.of("America/Denver"))
        .addInterpolatorConfig(numeric_config)
        .build();
    assertEquals(15000, config.start().msEpoch());
    assertEquals(60000, config.end().msEpoch());
    assertEquals(ZoneId.of("America/Denver"), config.timezone());
  }

  @Test
  public void ctorBadQuery() throws Exception {
    TimeSeriesQuery q = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1970/01/01-00:00:01")
            .setEnd("1970/01/01-00:00:00")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build();
    
    try {
      DownsampleConfig.newBuilder()
          .setAggregator("sum")
          .setId("foo")
          .setInterval("15s")
          .setQuery(q)
          .addInterpolatorConfig(numeric_config)
          .addInterpolatorConfig(summary_config)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    q = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1970/01/01-00:00:01")
            .setEnd("1970/01/01-00:00:01")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build();
    
    try {
      DownsampleConfig.newBuilder()
          .setAggregator("sum")
          .setId("foo")
          .setInterval("15s")
          .setQuery(q)
          .addInterpolatorConfig(numeric_config)
          .addInterpolatorConfig(summary_config)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    q = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1970/01/01-00:00:01")
            .setEnd("1970/01/01-00:00:29")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build();
    
    try {
      DownsampleConfig.newBuilder()
          .setAggregator("sum")
          .setId("foo")
          .setInterval("15s")
          .setQuery(q)
          .addInterpolatorConfig(numeric_config)
          .addInterpolatorConfig(summary_config)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void updateTimestamp() throws Exception {
    TimeSeriesQuery q = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1970/01/01-00:00:01")
            .setEnd("1970/01/01-00:01:00")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build();
    
    DownsampleConfig config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    
    TimeStamp ts = new MillisecondTimeStamp(80000L);
    assertEquals(80000L, ts.msEpoch());
    config.updateTimestamp(0, ts);
    assertEquals(15000, ts.msEpoch());
    
    config.updateTimestamp(0, ts);
    assertEquals(15000, ts.msEpoch());
    
    config.updateTimestamp(3, ts);
    assertEquals(60000, ts.msEpoch());
   
    try {
      config.updateTimestamp(-1, ts);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      config.updateTimestamp(0, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void nextTimestamp() throws Exception {
    TimeSeriesQuery q = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1970/01/01-00:00:01")
            .setEnd("1970/01/01-00:01:00")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build();
    
    DownsampleConfig config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    
    TimeStamp ts = new MillisecondTimeStamp(0);
    assertEquals(0, ts.msEpoch());
    
    config.nextTimestamp(ts);
    assertEquals(15000, ts.msEpoch());

    config.nextTimestamp(ts);
    assertEquals(30000, ts.msEpoch());
    
    config.nextTimestamp(ts);
    assertEquals(45000, ts.msEpoch());
  }
}
