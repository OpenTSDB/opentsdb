// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.downsample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import org.junit.Test;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.QueryIteratorInterpolatorFactory;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;

public class TestDownsampleConfig {

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
    
    QueryIteratorInterpolatorFactory factory = 
        new NumericInterpolatorFactory.Default();
    NumericInterpolatorConfig factory_config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build();
    
    DownsampleConfig config = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .setQueryIteratorInterpolatorFactory(factory)
        .setQueryIteratorInterpolatorConfig(factory_config)
        .build();
    assertEquals("sum", config.aggregator());
    assertEquals("foo", config.getId());
    assertEquals(Duration.of(15, ChronoUnit.SECONDS), config.interval());
    assertEquals(15, config.intervalPart());
    assertFalse(config.fill());
    assertSame(q, config.query());
    assertSame(factory, config.interpolator());
    assertSame(factory_config, config.interpolatorConfig());
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
        .setQueryIteratorInterpolatorFactory(new NumericInterpolatorFactory.Default())
        .setQueryIteratorInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .build())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("")
        .setInterval("15s")
        .setQuery(q)
        .setQueryIteratorInterpolatorFactory(new NumericInterpolatorFactory.Default())
        .setQueryIteratorInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .build())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        //.setInterval("15s")
        .setQuery(q)
        .setQueryIteratorInterpolatorFactory(new NumericInterpolatorFactory.Default())
        .setQueryIteratorInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .build())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("")
        .setQuery(q)
        .setQueryIteratorInterpolatorFactory(new NumericInterpolatorFactory.Default())
        .setQueryIteratorInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .build())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        //.setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .setQueryIteratorInterpolatorFactory(new NumericInterpolatorFactory.Default())
        .setQueryIteratorInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .build())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .setQueryIteratorInterpolatorFactory(new NumericInterpolatorFactory.Default())
        .setQueryIteratorInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .build())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        //.setQueryIteratorInterpolatorFactory(new NumericInterpolatorFactory.Default())
        .setQueryIteratorInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .build())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .setQueryIteratorInterpolatorFactory(new NumericInterpolatorFactory.Default())
        //.setQueryIteratorInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
        //    .setFillPolicy(FillPolicy.NONE)
        //    .setRealFillPolicy(FillWithRealPolicy.NONE)
        //    .build())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        //.setQuery(q)
        .setQueryIteratorInterpolatorFactory(new NumericInterpolatorFactory.Default())
        .setQueryIteratorInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .build())
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
    
    config = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .setRunAll(true)
        .setQueryIteratorInterpolatorFactory(factory)
        .setQueryIteratorInterpolatorConfig(factory_config)
        .build();
    assertEquals(1000, config.start().msEpoch());
    assertEquals(60000, config.end().msEpoch());
    
    config = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .setTimeZone(ZoneId.of("America/Denver"))
        .setQueryIteratorInterpolatorFactory(factory)
        .setQueryIteratorInterpolatorConfig(factory_config)
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
    
    QueryIteratorInterpolatorFactory factory = 
        new NumericInterpolatorFactory.Default();
    NumericInterpolatorConfig factory_config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build();
    
    try {
      DownsampleConfig.newBuilder()
          .setAggregator("sum")
          .setId("foo")
          .setInterval("15s")
          .setQuery(q)
          .setQueryIteratorInterpolatorFactory(factory)
          .setQueryIteratorInterpolatorConfig(factory_config)
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
          .setQueryIteratorInterpolatorFactory(factory)
          .setQueryIteratorInterpolatorConfig(factory_config)
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
          .setQueryIteratorInterpolatorFactory(factory)
          .setQueryIteratorInterpolatorConfig(factory_config)
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
    
    QueryIteratorInterpolatorFactory factory = 
        new NumericInterpolatorFactory.Default();
    NumericInterpolatorConfig factory_config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build();
    
    DownsampleConfig config = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .setQueryIteratorInterpolatorFactory(factory)
        .setQueryIteratorInterpolatorConfig(factory_config)
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
    
    QueryIteratorInterpolatorFactory factory = 
        new NumericInterpolatorFactory.Default();
    NumericInterpolatorConfig factory_config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build();
    
    DownsampleConfig config = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .setQueryIteratorInterpolatorFactory(factory)
        .setQueryIteratorInterpolatorConfig(factory_config)
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
