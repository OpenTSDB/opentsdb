// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
package net.opentsdb.query.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;

public class TestQueryPlanner {

  @Test
  public void ctor() {
    final TimeSeriesQuery query = TimeSeriesQuery.newBuilder().build();
    final TestImplementation planner = new TestImplementation(query);
    assertSame(query, planner.getOriginalQuery());
    assertFalse(planner.generated_plan);
    assertNull(planner.getTimeRanges());
    assertNull(planner.getPlannedQuery());
    
    try {
      new TestImplementation(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void getTimeRangesLessThan1HourNoDownsampling() {
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(1311022800000L)) // Mon, 18 Jul 2011 21:00:00 GMT
            .setEnd(Long.toString(1311022860000L))  // Mon, 18 Jul 2011 21:01:00 GMT
            .build())
        .build());
    assertEquals(1, times.length);
    assertEquals(1311022800000L, times[0][0].msEpoch());
    assertEquals(1311026400000L, times[0][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesOver1HourNoDownsampling() {
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(1311022800000L)) // Mon, 18 Jul 2011 21:00:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .build())
        .build());
    assertEquals(2, times.length);
    assertEquals(1311022800000L, times[0][0].msEpoch());
    assertEquals(1311026400000L, times[0][1].msEpoch());
    assertEquals(1311026400000L, times[1][0].msEpoch());
    assertEquals(1311030000000L, times[1][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesStartSnapOver1HourNoDownsampling() {
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .build())
        .build());
    assertEquals(2, times.length);
    assertEquals(1311022800000L, times[0][0].msEpoch());
    assertEquals(1311026400000L, times[0][1].msEpoch());
    assertEquals(1311026400000L, times[1][0].msEpoch());
    assertEquals(1311030000000L, times[1][1].msEpoch());
  }

  @Test
  public void getTimeRangesDownsample1Minute() {
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("1m")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .build())
        .build());
    assertEquals(2, times.length);
    assertEquals(1311022800000L, times[0][0].msEpoch());
    assertEquals(1311026400000L, times[0][1].msEpoch());
    assertEquals(1311026400000L, times[1][0].msEpoch());
    assertEquals(1311030000000L, times[1][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesDownsample1MinuteSliceDurationGreaterThanQuery() {
    final TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("1m")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .setSliceConfig("1w")
            .setAggregator("sum")
            .build())
        .setMetrics(Lists.newArrayList(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setId("m1")
            .build()))
        .build();
    query.validate();
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(query);
    assertEquals(1, times.length);
    assertEquals(1311022860000L, times[0][0].msEpoch());
    assertEquals(1311026460000L, times[0][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesDownsample1MinuteSliceDurationSameAsDefault() {
    final TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("1m")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .setSliceConfig("1h")
            .setAggregator("sum")
            .build())
        .setMetrics(Lists.newArrayList(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setId("m1")
            .build()))
        .build();
    query.validate();
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(query);
    assertEquals(2, times.length);
    assertEquals(1311022800000L, times[0][0].msEpoch());
    assertEquals(1311026400000L, times[0][1].msEpoch());
    assertEquals(1311026400000L, times[1][0].msEpoch());
    assertEquals(1311030000000L, times[1][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesDownsample1MinuteSliceDurationSameAsDownsampler() {
    final TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("1m")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .setSliceConfig("1m")
            .setAggregator("sum")
            .build())
        .setMetrics(Lists.newArrayList(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setId("m1")
            .build()))
        .build();
    query.validate();
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(query);
    assertEquals(60, times.length);
    assertEquals(1311022860000L, times[0][0].msEpoch());
    assertEquals(1311022920000L, times[0][1].msEpoch());
    assertEquals(1311022920000L, times[1][0].msEpoch());
    assertEquals(1311022980000L, times[1][1].msEpoch());
    assertEquals(1311026400000L, times[59][0].msEpoch());
    assertEquals(1311026460000L, times[59][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesDownsample1MinuteSliceDurationGreaterThanDownsampler() {
    final TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("1m")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .setSliceConfig("10m")
            .setAggregator("sum")
            .build())
        .setMetrics(Lists.newArrayList(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setId("m1")
            .build()))
        .build();
    query.validate();
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(query);
    assertEquals(7, times.length);
    assertEquals(1311022800000L, times[0][0].msEpoch());
    assertEquals(1311023400000L, times[0][1].msEpoch());
    assertEquals(1311023400000L, times[1][0].msEpoch());
    assertEquals(1311024000000L, times[1][1].msEpoch());
    assertEquals(1311026400000L, times[6][0].msEpoch());
    assertEquals(1311027000000L, times[6][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesDownsample15Minutes() {    
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("15m")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .build())
        .build());
    assertEquals(2, times.length);
    assertEquals(1311022800000L, times[0][0].msEpoch());
    assertEquals(1311026400000L, times[0][1].msEpoch());
    assertEquals(1311026400000L, times[1][0].msEpoch());
    assertEquals(1311030000000L, times[1][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesDownsample15MinutesSlice1h() {
    final TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("15m")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .setSliceConfig("1h")
            .setAggregator("sum")
            .build())
        .setMetrics(Lists.newArrayList(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setId("m1")
            .build()))
        .build();
    query.validate();
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(query);
    assertEquals(2, times.length);
    assertEquals(1311022800000L, times[0][0].msEpoch());
    assertEquals(1311026400000L, times[0][1].msEpoch());
    assertEquals(1311026400000L, times[1][0].msEpoch());
    assertEquals(1311030000000L, times[1][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesDownsample15MinutesSlice30m() {
    final TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("15m")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .setSliceConfig("30m")
            .setAggregator("sum")
            .build())
        .setMetrics(Lists.newArrayList(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setId("m1")
            .build()))
        .build();
    query.validate();
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(query);
    assertEquals(3, times.length);
    assertEquals(1311022800000L, times[0][0].msEpoch());
    assertEquals(1311024600000L, times[0][1].msEpoch());
    assertEquals(1311024600000L, times[1][0].msEpoch());
    assertEquals(1311026400000L, times[1][1].msEpoch());
    assertEquals(1311026400000L, times[2][0].msEpoch());
    assertEquals(1311028200000L, times[2][1].msEpoch());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTimeRangesDownsample15MinutesSlice20m() {
    final TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("15m")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .setSliceConfig("20m")
            .setAggregator("sum")
            .build())
        .setMetrics(Lists.newArrayList(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setId("m1")
            .build()))
        .build();
    query.validate();
    QueryPlanner.getTimeRanges(query);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTimeRangesDownsample15MinutesSlice1m() {
    final TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("15m")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .setSliceConfig("1m")
            .setAggregator("sum")
            .build())
        .setMetrics(Lists.newArrayList(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setId("m1")
            .build()))
        .build();
    query.validate();
    QueryPlanner.getTimeRanges(query);
  }
  
  @Test
  public void getTimeRangesDownsample45Minutes() {
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("45m")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311022800000L)) // Mon, 18 Jul 2011 21:00:00 GMT
            .setEnd(Long.toString(1311022860000L))  // Mon, 18 Jul 2011 21:01:00 GMT
            .build())
        .build());
    assertEquals(1, times.length);
    assertEquals(1311022800000L, times[0][0].msEpoch());
    assertEquals(1311022860000L, times[0][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesDownsample1Hour() {
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("1h")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .build())
        .build());
    // switches to 1 hour rollup
    assertEquals(1, times.length);
    assertEquals(1310947200000L, times[0][0].msEpoch());
    assertEquals(1311033600000L, times[0][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesDownsample2Hours() {
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("2h")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311001200000L)) // Mon, 18 Jul 2011 15:00:00 GMT
            .setEnd(Long.toString(1311026460000L))
            .build())  // Mon, 18 Jul 2011 22:01:00 GMT
        .build());
    assertEquals(1, times.length);
    assertEquals(1310947200000L, times[0][0].msEpoch());
    assertEquals(1311033600000L, times[0][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesDownsample3Hours() {
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("3h")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1305732600000L)) // Wed, 18 May 2011 15:30:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .build())
        .build());
    assertEquals(62, times.length);
    assertEquals(1305676800000L, times[0][0].msEpoch());
    assertEquals(1305763200000L, times[0][1].msEpoch());
    assertEquals(1310947200000L, times[61][0].msEpoch());
    assertEquals(1311033600000L, times[61][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesDownsample7Hours() {
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("7h")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1305732600000L)) // Wed, 18 May 2011 15:30:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .build())
        .build());
    assertEquals(1, times.length);
    assertEquals(1305732600000L, times[0][0].msEpoch());
    assertEquals(1311026460000L, times[0][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesDownsample12Hours() {
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("12h")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1305732600000L)) // Wed, 18 May 2011 15:30:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .build())
        .build());
    assertEquals(62, times.length);
    assertEquals(1305676800000L, times[0][0].msEpoch());
    assertEquals(1305763200000L, times[0][1].msEpoch());
    assertEquals(1310947200000L, times[61][0].msEpoch());
    assertEquals(1311033600000L, times[61][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesDownsample2Days() {
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("48h")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1305732600000L)) // Wed, 18 May 2011 15:30:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .build())
        .build());
    assertEquals(31, times.length);
    assertEquals(1305676800000L, times[0][0].msEpoch());
    assertEquals(1305849600000L, times[0][1].msEpoch());
    assertEquals(1310860800000L, times[30][0].msEpoch());
    assertEquals(1311033600000L, times[30][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesMetricOverride() {
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("1h")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .build())
        .setMetrics(Lists.newArrayList(Metric.newBuilder().setMetric("foo")
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("45m")
                .setAggregator("sum")
                .build())
            .build()))
        .build(), 0);
    assertEquals(1, times.length);
    assertEquals(1311022860000L, times[0][0].msEpoch());
    assertEquals(1311026460000L, times[0][1].msEpoch());
  }
  
  @Test
  public void getTimeRangesMetricOverrideFallback() {
    final TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setDownsampler(Downsampler.newBuilder()
                .setInterval("1h")
                .setAggregator("sum")
                .build())
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026460000L))  // Mon, 18 Jul 2011 22:01:00 GMT
            .build())
        .setMetrics(Lists.newArrayList(Metric.newBuilder().setMetric("foo").build()))
        .build(), 0);
    // switches to 1 hour rollup
    assertEquals(1, times.length);
    assertEquals(1310947200000L, times[0][0].msEpoch());
    assertEquals(1311033600000L, times[0][1].msEpoch());
  }
  
  @Test (expected = IllegalStateException.class)
  public void getTimeRangesOverflow() {
    // before this is ever triggered we better make sure the query is sane.
    QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(1L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(7730944732800000L))  // Tue, 20 Nov 246953 00:00:00 GMT
            .build())
        .build());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTimeRangesNullQuery() {
    QueryPlanner.getTimeRanges(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTimeRangesNoTime() {
    QueryPlanner.getTimeRanges(TimeSeriesQuery.newBuilder().build());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTimeRangesMetricOutOfRange() {
    QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(1311022800000L)) // Mon, 18 Jul 2011 21:00:00 GMT
            .setEnd(Long.toString(1311022860000L))  // Mon, 18 Jul 2011 21:01:00 GMT
            .build())
        .setMetrics(new ArrayList<Metric>(0))
        .build(), 0);
  }

  @Test
  public void getTimeRangesOnBoundary() {
    TimeStamp[][] times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(1311022860000L)) // Mon, 18 Jul 2011 21:01:00 GMT
            .setEnd(Long.toString(1311026400000L))  // Mon, 18 Jul 2011 22:00:00 GMT
            .build())
        .build());
    assertEquals(1, times.length);
    assertEquals(1311022800000L, times[0][0].msEpoch());
    assertEquals(1311026400000L, times[0][1].msEpoch());
    
    times = QueryPlanner.getTimeRanges(
        TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(1311022800000L)) // Mon, 18 Jul 2011 21:00:00 GMT
            .setEnd(Long.toString(1311026400000L))  // Mon, 18 Jul 2011 22:00:00 GMT
            .build())
        .build());
    assertEquals(1, times.length);
    assertEquals(1311022800000L, times[0][0].msEpoch());
    assertEquals(1311026400000L, times[0][1].msEpoch());
  }
  
  /** Dummy implementation for testing */
  class TestImplementation extends QueryPlanner<IteratorGroups> {
    public boolean generated_plan = false;
    
    public TestImplementation(final TimeSeriesQuery query) {
      super(query);
    }

    @Override
    protected void generatePlan() {
      generated_plan = true;
    }
    
  }
}
