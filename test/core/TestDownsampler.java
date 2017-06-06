// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import com.google.common.collect.Lists;

import net.opentsdb.core.SeekableViewsForTest.MockSeekableView;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupQuery;
import net.opentsdb.utils.DateTime;

import org.junit.Before;
import org.junit.Test;

/** Tests {@link Downsampler}. */
@SuppressWarnings("deprecation")
public class TestDownsampler {

  private static final long BASE_TIME = 1356998400000L;
  private static final DataPoint[] DATA_POINTS = new DataPoint[] {
    // timestamp = 1,356,998,400,000 ms
    MutableDataPoint.ofLongValue(BASE_TIME, 40),
    // timestamp = 1,357,000,400,000 ms
    MutableDataPoint.ofLongValue(BASE_TIME + 2000000, 50),
    // timestamp = 1,357,002,000,000 ms
    MutableDataPoint.ofLongValue(BASE_TIME + 3600000, 40),
    // timestamp = 1,357,002,005,000 ms
    MutableDataPoint.ofLongValue(BASE_TIME + 3605000, 50),
    // timestamp = 1,357,005,600,000 ms
    MutableDataPoint.ofLongValue(BASE_TIME + 7200000, 40),
    // timestamp = 1,357,007,600,000 ms
    MutableDataPoint.ofLongValue(BASE_TIME + 9200000, 50)
  };
  private static final int THOUSAND_SEC_INTERVAL =
      (int)DateTime.parseDuration("1000s");
  private static final int TEN_SEC_INTERVAL =
      (int)DateTime.parseDuration("10s");
  private static final Aggregator AVG = Aggregators.get("avg");
  private static final Aggregator SUM = Aggregators.get("sum");
  private static final TimeZone EST_TIME_ZONE = DateTime.timezones.get("EST");
  //30 minute offset
  final static TimeZone AF = DateTime.timezones.get("Asia/Kabul");
  // 12h offset w/o DST
  final static TimeZone TV = DateTime.timezones.get("Pacific/Funafuti");
  // 12h offset w DST
  final static TimeZone FJ = DateTime.timezones.get("Pacific/Fiji");
  // Tue, 15 Dec 2015 04:02:25.123 UTC
  final static long DST_TS = 1450137600000L;
 
  private SeekableView source;
  private Downsampler downsampler;
  private DownsamplingSpecification specification;

  @Before
  public void before() {
    source = spy(SeekableViewsForTest.fromArray(DATA_POINTS));
  }

  @Test
  public void testDownsampler() {
    specification = new DownsamplingSpecification("1000s-avg");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(5, values.size());
    assertEquals(40, values.get(0), 0.0000001);
    assertEquals(BASE_TIME - 400000L, timestamps_in_millis.get(0).longValue());
    assertEquals(50, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 1600000, timestamps_in_millis.get(1).longValue());
    assertEquals(45, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 3600000L, timestamps_in_millis.get(2).longValue());
    assertEquals(40, values.get(3), 0.0000001);
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(3).longValue());
    assertEquals(50, values.get(4), 0.0000001);
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(4).longValue());
  }
  
  @Test
  public void testDownsamplerDeprecated() {
    downsampler = new Downsampler(source, THOUSAND_SEC_INTERVAL, AVG);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(5, values.size());
    assertEquals(40, values.get(0), 0.0000001);
    assertEquals(BASE_TIME - 400000L, timestamps_in_millis.get(0).longValue());
    assertEquals(50, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 1600000, timestamps_in_millis.get(1).longValue());
    assertEquals(45, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 3600000L, timestamps_in_millis.get(2).longValue());
    assertEquals(40, values.get(3), 0.0000001);
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(3).longValue());
    assertEquals(50, values.get(4), 0.0000001);
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(4).longValue());
  }
  
  @Test
  public void testDownsamplerDeprecated_10seconds() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 0, 1),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 1, 2),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 2, 4),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 3, 8),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 4, 16),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 5, 32),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 6, 64),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 7, 128),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 8, 256),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 9, 512),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 10, 1024)
    }));
    downsampler = new Downsampler(source, 10000, SUM);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(6, values.size());
    assertEquals(3, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 00000L, timestamps_in_millis.get(0).longValue());
    assertEquals(12, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 10000L, timestamps_in_millis.get(1).longValue());
    assertEquals(48, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 20000L, timestamps_in_millis.get(2).longValue());
    assertEquals(192, values.get(3), 0.0000001);
    assertEquals(BASE_TIME + 30000L, timestamps_in_millis.get(3).longValue());
    assertEquals(768, values.get(4), 0.0000001);
    assertEquals(BASE_TIME + 40000L, timestamps_in_millis.get(4).longValue());
    assertEquals(1024, values.get(5), 0.0000001);
    assertEquals(BASE_TIME + 50000L, timestamps_in_millis.get(5).longValue());
  }

  @Test
  public void testDownsampler_10seconds() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 0, 1),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 1, 2),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 2, 4),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 3, 8),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 4, 16),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 5, 32),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 6, 64),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 7, 128),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 8, 256),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 9, 512),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 10, 1024)
    }));
    specification = new DownsamplingSpecification("10s-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(6, values.size());
    assertEquals(3, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 00000L, timestamps_in_millis.get(0).longValue());
    assertEquals(12, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 10000L, timestamps_in_millis.get(1).longValue());
    assertEquals(48, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 20000L, timestamps_in_millis.get(2).longValue());
    assertEquals(192, values.get(3), 0.0000001);
    assertEquals(BASE_TIME + 30000L, timestamps_in_millis.get(3).longValue());
    assertEquals(768, values.get(4), 0.0000001);
    assertEquals(BASE_TIME + 40000L, timestamps_in_millis.get(4).longValue());
    assertEquals(1024, values.get(5), 0.0000001);
    assertEquals(BASE_TIME + 50000L, timestamps_in_millis.get(5).longValue());
  }
  
  @Test
  public void testDownsamplerDeprecated_15seconds() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME + 5000L, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 15000L, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 25000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 35000L, 8),
        MutableDataPoint.ofLongValue(BASE_TIME + 45000L, 16),
        MutableDataPoint.ofLongValue(BASE_TIME + 55000L, 32)
    }));
    downsampler = new Downsampler(source, 15000, SUM);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(4, values.size());
    assertEquals(1, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 00000L, timestamps_in_millis.get(0).longValue());
    assertEquals(6, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 15000L, timestamps_in_millis.get(1).longValue());
    assertEquals(8, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 30000L, timestamps_in_millis.get(2).longValue());
    assertEquals(48, values.get(3), 0.0000001);
    assertEquals(BASE_TIME + 45000L, timestamps_in_millis.get(3).longValue());
  }

  @Test
  public void testDownsampler_15seconds() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME + 5000L, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 15000L, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 25000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 35000L, 8),
        MutableDataPoint.ofLongValue(BASE_TIME + 45000L, 16),
        MutableDataPoint.ofLongValue(BASE_TIME + 55000L, 32)
    }));
    specification = new DownsamplingSpecification("15s-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(4, values.size());
    assertEquals(1, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 00000L, timestamps_in_millis.get(0).longValue());
    assertEquals(6, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 15000L, timestamps_in_millis.get(1).longValue());
    assertEquals(8, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 30000L, timestamps_in_millis.get(2).longValue());
    assertEquals(48, values.get(3), 0.0000001);
    assertEquals(BASE_TIME + 45000L, timestamps_in_millis.get(3).longValue());
  }
  
  @Test
  public void testDownsampler_allFullRange() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME + 5000L, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 15000L, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 25000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 35000L, 8),
        MutableDataPoint.ofLongValue(BASE_TIME + 45000L, 16),
        MutableDataPoint.ofLongValue(BASE_TIME + 55000L, 32)
    }));
    specification = new DownsamplingSpecification("0all-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(1, values.size());
    assertEquals(63, values.get(0), 0.0000001);
    assertEquals(0L, timestamps_in_millis.get(0).longValue());
  }
  
  @Test
  public void testDownsampler_allFilterOnQuery() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME + 5000L, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 15000L, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 25000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 35000L, 8),
        MutableDataPoint.ofLongValue(BASE_TIME + 45000L, 16),
        MutableDataPoint.ofLongValue(BASE_TIME + 55000L, 32)
    }));
    specification = new DownsamplingSpecification("0all-sum");
    downsampler = new Downsampler(source, specification, 
        BASE_TIME + 15000L, BASE_TIME + 45000L);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(1, values.size());
    assertEquals(14, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 15000L, timestamps_in_millis.get(0).longValue());
  }
  
  @Test
  public void testDownsampler_allFilterOnQueryOutOfRangeEarly() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME + 5000L, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 15000L, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 25000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 35000L, 8),
        MutableDataPoint.ofLongValue(BASE_TIME + 45000L, 16),
        MutableDataPoint.ofLongValue(BASE_TIME + 55000L, 32)
    }));
    specification = new DownsamplingSpecification("0all-sum");
    downsampler = new Downsampler(source, specification, 
        BASE_TIME + 65000L, BASE_TIME + 75000L);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(0, values.size());
  }
  
  @Test
  public void testDownsampler_allFilterOnQueryOutOfRangeLate() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME + 5000L, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 15000L, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 25000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 35000L, 8),
        MutableDataPoint.ofLongValue(BASE_TIME + 45000L, 16),
        MutableDataPoint.ofLongValue(BASE_TIME + 55000L, 32)
    }));
    specification = new DownsamplingSpecification("0all-sum");
    downsampler = new Downsampler(source, specification, 
        BASE_TIME - 15000L, BASE_TIME - 5000L);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(0, values.size());
  }
  
  @Test
  public void testDownsampler_calendar() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME + 5000L, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 15000L, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 25000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 35000L, 8),
        MutableDataPoint.ofLongValue(BASE_TIME + 45000L, 16),
        MutableDataPoint.ofLongValue(BASE_TIME + 55000L, 32)
    }));
    specification = new DownsamplingSpecification("1dc-sum");
    specification.setTimezone(DateTime.timezones.get("America/Denver"));
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(1, values.size());
    assertEquals(63, values.get(0), 0.0000001);
    assertEquals(1356937200000L, timestamps_in_millis.get(0).longValue());
  }
  
  @Test
  public void testDownsampler_calendarHour() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 1800000, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 3599000L, 3),
        MutableDataPoint.ofLongValue(BASE_TIME + 3600000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 5400000L, 5),
        MutableDataPoint.ofLongValue(BASE_TIME + 7199000L, 6)
    }));
    specification = new DownsamplingSpecification("1hc-sum");
    specification.setTimezone(TV);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    long ts = BASE_TIME;
    double value = 6;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 3600000;
      value = 15;
    }

    // hour offset by 30m
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1hc-sum");
    specification.setTimezone(AF);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1356996600000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 3600000;
      if (value == 1) {
        value = 9;
      } else {
        value = 11;
      }
    }
    
    // multiple hours
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("4hc-sum");
    specification.setTimezone(AF);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1356996600000L;
    value = 21;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
    }
  }
  
  @Test
  public void testDownsampler_calendarDay() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(DST_TS, 1),
        MutableDataPoint.ofLongValue(DST_TS + 86399000, 2),
        MutableDataPoint.ofLongValue(DST_TS + 126001000L, 3), // falls to the next in FJ
        MutableDataPoint.ofLongValue(DST_TS + 172799000L, 4),
        MutableDataPoint.ofLongValue(DST_TS + 172800000L, 5),
        MutableDataPoint.ofLongValue(DST_TS + 242999000L, 6) // falls within 30m offset
    }));
    
    // control
    specification = new DownsamplingSpecification("1dc-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    long ts = DST_TS;
    double value = 3;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000;
      if (value == 3) {
        value = 7;
      } else if (value == 7) {
        value = 11;
      }
    }

    // 12 hour offset from UTC
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1dc-sum");
    specification.setTimezone(TV);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1450094400000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000;
      if (value == 1) {
        value = 5;
      } else if (value == 5) {
        value = 9;
      } else {
        value = 6;
      }
    }
    
    // 11 hour offset from UTC
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1dc-sum");
    specification.setTimezone(FJ);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1450090800000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000;
      if (value == 1) {
        value = 2;
      } else if (value == 2) {
        value = 12;
      } else {
        value = 6;
      }
    }
    
    // 30m offset
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1dc-sum");
    specification.setTimezone(AF);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1450121400000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000;
      if (value == 1) {
        value = 5;
      } else if (value == 5) {
        value = 15;
      }
    }
    
    // multiple days
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("3dc-sum");
    specification.setTimezone(AF);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1450121400000L;
    value = 21;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
    }
  }
  
  @Test
  public void testDownsampler_calendarWeek() {
    source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(DST_TS, 1), // a Tuesday in UTC land
        MutableDataPoint.ofLongValue(DST_TS + (86400000L * 7), 2),
        MutableDataPoint.ofLongValue(1451129400000L, 3), // falls to the next in FJ
        MutableDataPoint.ofLongValue(DST_TS + (86400000L * 21), 4),
        MutableDataPoint.ofLongValue(1452367799000L, 5) // falls within 30m offset
    });
    // control
    specification = new DownsamplingSpecification("1wc-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    
    long ts = 1449964800000L;
    double value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      if (ts == 1450569600000L) {
        ts = 1451779200000L; // skips a week
      } else {
        ts += 86400000L * 7;
      }
      if (value == 1) {
        value = 5;
      } else {
        value = 9;
      }
    }

    // 12 hour offset from UTC
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1wc-sum");
    specification.setTimezone(TV);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1449921600000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      if (ts == 1450526400000L) {
        ts = 1451736000000L; // skip a week
      } else {
        ts += 86400000L * 7;
      }
      if (value == 1) {
        value = 5;
      } else if (value == 5) {
        value = 4;
      } else {
        value = 5;
      }
    }
    
    // 11 hour offset from UTC
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1wc-sum");
    specification.setTimezone(FJ);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1449918000000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000L * 7;
      value++;
    }
    
    // 30m offset
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1wc-sum");
    specification.setTimezone(AF);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1449948600000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      if (ts == 1449948600000L) {
        ts = 1450553400000L;
      } else {
        ts = 1451763000000L;
      }
      if (value == 1) {
        value = 5;
      } else {
        value = 9;
      }
    }
    
    // multiple weeks
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("2wc-sum");
    specification.setTimezone(AF);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1449948600000L;
    value = 6;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts = 1451158200000L;
      value = 9;
    }
  }
  
  @Test
  public void testDownsampler_calendarMonth() {
    final long dec_1st = 1448928000000L;
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(dec_1st, 1),
        MutableDataPoint.ofLongValue(1451559600000L, 2), // falls to the next in FJ
        MutableDataPoint.ofLongValue(1451606400000L, 3), // jan 1st
        MutableDataPoint.ofLongValue(1454284800000L, 4), // feb 1st
        MutableDataPoint.ofLongValue(1456704000000L, 5), // feb 29th (leap year)
        MutableDataPoint.ofLongValue(1456772400000L, 6)  // falls within 30m offset AF
    }));
    
    // control
    specification = new DownsamplingSpecification("1nc-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    long ts = dec_1st;
    double value = 3;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      if (ts == 1448928000000L) {
        ts = 1451606400000L;
      } else {
        ts = 1454284800000L;
        value = 15;
      }
    }
    
    // 12h offset
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1nc-sum");
    specification.setTimezone(TV);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1448884800000L;
    value = 3;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      if (ts == 1448884800000L) {
        ts = 1451563200000L;
      } else if (ts == 1451563200000L) {
        value = 9;
        ts = 1454241600000L;
      } else {
        ts = 1456747200000L;
        value = 6;
      }
    }
    
    // 11h offset
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1nc-sum");
    specification.setTimezone(FJ);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1448881200000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      if (ts == 1448881200000L) {
        ts = 1451559600000L;
        value = 5;
      } else if (ts == 1451559600000L) {
        ts = 1454241600000L;
        value = 9;
      } else {
        ts = 1456747200000L;
        value = 6;
      }
    }
    
    // 30m offset
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1nc-sum");
    specification.setTimezone(AF);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1448911800000L;
    value = 3;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      if (ts == 1448911800000L) {
        ts = 1451590200000L;
      } else {
        ts = 1454268600000L;
        value = 15;
      }
    }
    
    // multiple months
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("3nc-sum");
    specification.setTimezone(TV);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1443614400000L;
    value = 3;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts = 1451563200000L;
      value = 18;
    }
  }
  
  @Test
  public void testDownsampler_noData() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] { }));
    specification = new DownsamplingSpecification("1d-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    assertFalse(downsampler.hasNext());
  }
  
  @Test
  public void testDownsampler_noDataCalendar() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] { }));
    specification = new DownsamplingSpecification("1mc-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    assertFalse(downsampler.hasNext());
  }
  
  @Test
  public void testDownsampler_1day() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 43200000L, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 86400000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 129600000L, 8)
    }));
    
    downsampler = new Downsampler(source, 86400000, SUM);
    verify(source, never()).next();
    long timestamp = BASE_TIME;
    double value = 3;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(timestamp, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.000001);
      timestamp = 1357084800000L;
      value = 12;
    }
  }

  @Test
  public void testDownsampler_1day_timezone() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(1357016400000L, 1),
        MutableDataPoint.ofLongValue(1357059600000L, 2),
        MutableDataPoint.ofLongValue(1357102800000L, 4),
        MutableDataPoint.ofLongValue(1357146000000L, 8)
    }));
    
    specification = new DownsamplingSpecification("1dc-sum");
    specification.setTimezone(EST_TIME_ZONE);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    
    long timestamp = 1357016400000L;
    double value = 3;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(timestamp, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.000001);
      timestamp = 1357102800000L;
      value = 12;
    }
  }
  
  @Test
  public void testDownsampler_1week() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(1356825600000L, 1),
        MutableDataPoint.ofLongValue(1357128000000L, 2),
        MutableDataPoint.ofLongValue(1357430400000L, 4),
        MutableDataPoint.ofLongValue(1357732800000L, 8)
    }));
    
    specification = new DownsamplingSpecification("1wc-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    long timestamp = 1356825600000L;
    double value = 3;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(timestamp, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.000001);
      timestamp = 1357430400000L;
      value = 12;
    }
  }

  @Test
  public void testDownsampler_1week_timezone() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(1356843600000L, 1),
        MutableDataPoint.ofLongValue(1357146000000L, 2),
        MutableDataPoint.ofLongValue(1357448400000L, 4),
        MutableDataPoint.ofLongValue(1357750800000L, 8)
    }));
    
    specification = new DownsamplingSpecification("1wc-sum");
    specification.setTimezone(EST_TIME_ZONE);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    long timestamp = 1356843600000L;
    double value = 3;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(timestamp, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.000001);
      timestamp = 1357448400000L;
      value = 12;
    }
  }

  @Test
  public void testDownsampler_1month() {
    final int field = DateTime.unitsToCalendarType("n");
    final DataPoint [] data_points = new DataPoint[24];
    Calendar c = DateTime.previousInterval(BASE_TIME, 1, field);
    //long timestamp = DateTime.toStartOfMonth(BASE_TIME, UTC_TIME_ZONE);
    long timestamp = c.getTimeInMillis();
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);

      i += 1;
      c.add(field, 1);
      long startOfNextInterval = c.getTimeInMillis() + 1;
      timestamp = timestamp + (startOfNextInterval - timestamp) / 2;
      value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = startOfNextInterval;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));

    specification = new DownsamplingSpecification("1nc-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    c = DateTime.previousInterval(BASE_TIME, 1, field);
    int j = 0;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals((1 << j++) + (1 << j++), dp.doubleValue(), 0.0000001);
      assertEquals(c.getTimeInMillis(), dp.timestamp());
      c.add(field, 1);
    }
  }
  
  @Test
  public void testDownsampler_1month_alt() {
    /*
    1380600000 -> 2013-10-01T04:00:00Z
    1383278400 -> 2013-11-01T04:00:00Z
    1385874000 -> 2013-12-01T05:00:00Z
    1388552400 -> 2014-01-01T05:00:00Z
    1391230800 -> 2014-02-01T05:00:00Z
    1393650000 -> 2014-03-01T05:00:00Z
    1396324800 -> 2014-04-01T04:00:00Z
    1398916800 -> 2014-05-01T04:00:00Z
    1401595200 -> 2014-06-01T04:00:00Z
    1404187200 -> 2014-07-01T04:00:00Z
    1406865600 -> 2014-08-01T04:00:00Z
    1409544000 -> 2014-09-01T04:00:00Z
    */

    int value = 1;
    final DataPoint [] data_points = new DataPoint[] {
      MutableDataPoint.ofLongValue(1380600000000L, value), 
      MutableDataPoint.ofLongValue(1383278400000L, value), 
      MutableDataPoint.ofLongValue(1385874000000L, value), 
      MutableDataPoint.ofLongValue(1388552400000L, value), 
      MutableDataPoint.ofLongValue(1391230800000L, value), 
      MutableDataPoint.ofLongValue(1393650000000L, value), 
      MutableDataPoint.ofLongValue(1396324800000L, value), 
      MutableDataPoint.ofLongValue(1398916800000L, value), 
      MutableDataPoint.ofLongValue(1401595200000L, value), 
      MutableDataPoint.ofLongValue(1404187200000L, value), 
      MutableDataPoint.ofLongValue(1406865600000L, value), 
      MutableDataPoint.ofLongValue(1409544000000L, value), 
    };
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    
    specification = new DownsamplingSpecification("1dc-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    final int field = DateTime.unitsToCalendarType("n");
    final Calendar c = DateTime.previousInterval(1380585600000L, 1, field);
    long timestamp = c.getTimeInMillis();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(1, dp.doubleValue(), 0.0000001);
      assertEquals(timestamp, dp.timestamp());
      c.add(field, 1);
      timestamp = c.getTimeInMillis();
    }
  }
  
  @Test
  public void testDownsampler_2months() {
    final int field = DateTime.unitsToCalendarType("n");
    final DataPoint [] data_points = new DataPoint[24];
    Calendar c = DateTime.previousInterval(BASE_TIME, 1, field);
    long timestamp = c.getTimeInMillis();
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);

      i += 1;
      c.add(field, 1);
      long startOfNextInterval = c.getTimeInMillis();
      timestamp = timestamp + (startOfNextInterval - timestamp) / 2;
      value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = startOfNextInterval;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    
    specification = new DownsamplingSpecification("2nc-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    int j = 0;
    c = DateTime.previousInterval(BASE_TIME, 1, field);
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      long value = 0;
      for (int k = 0; k < 4; k++) {
        value += (1 << j++);
      }
      assertEquals(value, dp.doubleValue(), 0.0000001);
      assertEquals(c.getTimeInMillis(), dp.timestamp());
      c.add(field, 2);
    }
  }
  
  @Test
  public void testDownsampler_1month_timezone() {
    final int field = DateTime.unitsToCalendarType("n");
    final DataPoint [] data_points = new DataPoint[24];
    Calendar c = DateTime.previousInterval(1357016400000L, 1, field, EST_TIME_ZONE);
    long timestamp = c.getTimeInMillis();
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);

      i += 1;
      c.add(field, 1);
      long startOfNextInterval = c.getTimeInMillis();
      timestamp = timestamp + (startOfNextInterval - timestamp) / 2;
      value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = startOfNextInterval;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    
    specification = new DownsamplingSpecification("1nc-sum");
    specification.setTimezone(EST_TIME_ZONE);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    int j = 0;
    c = DateTime.previousInterval(1357016400000L, 1, field, EST_TIME_ZONE);
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals((1 << j++) + (1 << j++), dp.doubleValue(), 0.0000001);
      assertEquals(c.getTimeInMillis(), dp.timestamp());
      c.add(field, 1);
    }
  }

  @Test
  public void testDownsampler_1year() {
    final int field = DateTime.unitsToCalendarType("y");
    final DataPoint [] data_points = new DataPoint[4];
    Calendar c = DateTime.previousInterval(BASE_TIME, 1, field);
    long timestamp = c.getTimeInMillis();
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);

      i += 1;
      c.add(field, 1);
      long startOfNextInterval = c.getTimeInMillis();
      timestamp = timestamp + (startOfNextInterval - timestamp) / 2;
      value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = startOfNextInterval;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    
    specification = new DownsamplingSpecification("1yc-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    int j = 0;
    c = DateTime.previousInterval(BASE_TIME, 1, field);
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals((1 << j++) + (1 << j++), dp.doubleValue(), 0.0000001);
      assertEquals(c.getTimeInMillis(), dp.timestamp());
      c.add(field, 1);
    }
  }

  @Test
  public void testDownsampler_1year_timezone() {
    final int field = DateTime.unitsToCalendarType("y");
    final DataPoint [] data_points = new DataPoint[4];
    Calendar c = DateTime.previousInterval(1357016400000L, 1, field, 
        EST_TIME_ZONE);
    long timestamp = c.getTimeInMillis();
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);

      i += 1;
      c.add(field, 1);
      long startOfNextInterval = c.getTimeInMillis();
      timestamp = timestamp + (startOfNextInterval - timestamp) / 2;
      value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = startOfNextInterval;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    
    specification = new DownsamplingSpecification("1yc-sum");
    specification.setTimezone(EST_TIME_ZONE);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    int j = 0;
    c = DateTime.previousInterval(1357016400000L, 1, field, EST_TIME_ZONE);
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals((1 << j++) + (1 << j++), dp.doubleValue(), 0.0000001);
      assertEquals(c.getTimeInMillis(), dp.timestamp());
      c.add(field, 1);
    }
  }
  
  @Test
  public void testDownsampler_rollupSum() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable("tsdb-rollup-1h")
        .setPreAggregationTable("tsdb-agg-rollup-1h")
        .setInterval("1h")
        .setRowSpan("1d")
        .build();
    final RollupQuery rollup_query = new RollupQuery(interval, Aggregators.SUM,
        3600000, Aggregators.SUM);
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 0, 1),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 1, 2),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 2, 4),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 3, 8),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 4, 16),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 5, 32),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 6, 64),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 7, 128),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 8, 256),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 9, 512),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 10, 1024)
    }));
    specification = new DownsamplingSpecification("10s-sum");
    downsampler = new Downsampler(source, specification, 0, 0, rollup_query);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(6, values.size());
    assertEquals(3, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 00000L, timestamps_in_millis.get(0).longValue());
    assertEquals(12, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 10000L, timestamps_in_millis.get(1).longValue());
    assertEquals(48, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 20000L, timestamps_in_millis.get(2).longValue());
    assertEquals(192, values.get(3), 0.0000001);
    assertEquals(BASE_TIME + 30000L, timestamps_in_millis.get(3).longValue());
    assertEquals(768, values.get(4), 0.0000001);
    assertEquals(BASE_TIME + 40000L, timestamps_in_millis.get(4).longValue());
    assertEquals(1024, values.get(5), 0.0000001);
    assertEquals(BASE_TIME + 50000L, timestamps_in_millis.get(5).longValue());
  }
  
  @Test
  public void testDownsampler_rollupAvg() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable("tsdb-rollup-1h")
        .setPreAggregationTable("tsdb-agg-rollup-1h")
        .setInterval("1h")
        .setRowSpan("1d")
        .build();
    final RollupQuery rollup_query = new RollupQuery(interval, Aggregators.SUM,
        3600000, Aggregators.AVG);
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 0, 1),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 1, 2),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 2, 4),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 3, 8)
    }));
    specification = new DownsamplingSpecification("10s-avg");
    downsampler = new Downsampler(source, specification, 0, 0, rollup_query);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(2, values.size());
    assertEquals(1.5, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 00000L, timestamps_in_millis.get(0).longValue());
    assertEquals(6, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 10000L, timestamps_in_millis.get(1).longValue());
  }
  
  @Test
  public void testDownsampler_rollupCount() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable("tsdb-rollup-1h")
        .setPreAggregationTable("tsdb-agg-rollup-1h")
        .setInterval("1h")
        .setRowSpan("1d")
        .build();
    final RollupQuery rollup_query = new RollupQuery(interval, Aggregators.SUM,
        3600000, Aggregators.COUNT);
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 0, 1),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 1, 2),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 2, 4),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 3, 8)
    }));
    specification = new DownsamplingSpecification("10s-count");
    downsampler = new Downsampler(source, specification, 0, 0, rollup_query);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(2, values.size());
    assertEquals(2, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 00000L, timestamps_in_millis.get(0).longValue());
    assertEquals(2, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 10000L, timestamps_in_millis.get(1).longValue());
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void testDownsampler_rollupDev() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable("tsdb-rollup-1h")
        .setPreAggregationTable("tsdb-agg-rollup-1h")
        .setInterval("1h")
        .setRowSpan("1d")
        .build();
    final RollupQuery rollup_query = new RollupQuery(interval, Aggregators.SUM,
        3600000, Aggregators.DEV);
    specification = new DownsamplingSpecification("10s-dev");
    downsampler = new Downsampler(source, specification, 0, 0, rollup_query);
    while (downsampler.hasNext()) {
      downsampler.next(); // <-- throws here
    }
  }
  
  @Test(expected = UnsupportedOperationException.class)
  public void testRemove() {
    new Downsampler(source, THOUSAND_SEC_INTERVAL, AVG).remove();
  }

  @Test
  public void testSeek() {
    downsampler = new Downsampler(source, THOUSAND_SEC_INTERVAL, AVG);
    downsampler.seek(BASE_TIME + 3600000L);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(3, values.size());
    assertEquals(45, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 3600000L, timestamps_in_millis.get(0).longValue());
    assertEquals(40, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(1).longValue());
    assertEquals(50, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(2).longValue());
  }

  @Test
  public void testSeek_useCalendar() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(1356998400000L, 1),
        MutableDataPoint.ofLongValue(1388534400000L, 2),
        MutableDataPoint.ofLongValue(1420070400000L, 4),
        MutableDataPoint.ofLongValue(1451606400000L, 8)
    }));
    
    specification = new DownsamplingSpecification("1y-sum");
    specification.setUseCalendar(true);
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    
    downsampler.seek(1420070400000L);
    verify(source, never()).next();
    
    long timestamp = 1420070400000L;
    double value = 4;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(timestamp, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.0000001);
      timestamp = 1451606400000L;
      value = 8;
    }
    
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1yc-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE);
    downsampler.seek(1420070400001L);
    
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(timestamp, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.0000001);
    }
  }
  
  @Test
  public void testSeek_skipPartialInterval() {
    downsampler = new Downsampler(source, THOUSAND_SEC_INTERVAL, AVG);
    downsampler.seek(BASE_TIME + 3800000L);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    // seek timestamp was BASE_TIME + 3800000L or 1,357,002,200,000 ms.
    // The interval that has the timestamp began at 1,357,002,000,000 ms. It
    // had two data points but was abandoned because the requested timestamp
    // was not aligned. The next two intervals at 1,357,003,000,000 and
    // at 1,357,004,000,000 did not have data points. The first interval that
    // had a data point began at 1,357,002,005,000 ms or BASE_TIME + 6600000L.
    assertEquals(2, values.size());
    assertEquals(40, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(0).longValue());
    assertEquals(50, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(1).longValue());
  }

  @Test
  public void testSeek_doubleIteration() {
    downsampler = new Downsampler(source, THOUSAND_SEC_INTERVAL, AVG);
    while (downsampler.hasNext()) {
      downsampler.next();
    }
    downsampler.seek(BASE_TIME + 3600000L);
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(3, values.size());
    assertEquals(45, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 3600000L, timestamps_in_millis.get(0).longValue());
    assertEquals(40, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(1).longValue());
    assertEquals(50, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(2).longValue());
  }

  @Test
  public void testSeek_abandoningIncompleteInterval() {
    source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME + 100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 1100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 2100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 3100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 4100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 5100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 6100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 7100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 8100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 9100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 10100L, 40)
      });
    downsampler = new Downsampler(source, TEN_SEC_INTERVAL, SUM);
    // The seek is aligned by the downsampling window.
    downsampler.seek(BASE_TIME);
    assertTrue("seek(BASE_TIME)", downsampler.hasNext());
    DataPoint first_dp = downsampler.next();
    assertEquals("seek(1356998400000)", BASE_TIME, first_dp.timestamp());
    assertEquals("seek(1356998400000)", 400, first_dp.doubleValue(), 0.0000001);
    // No seeks but the last one is aligned by the downsampling window.
    for (long seek_timestamp = BASE_TIME + 1000L;
         seek_timestamp < BASE_TIME + 10100L; seek_timestamp += 1000) {
      downsampler.seek(seek_timestamp);
      assertTrue("ts = " + seek_timestamp, downsampler.hasNext());
      DataPoint dp = downsampler.next();
      // Timestamp should be greater than or equal to the seek timestamp.
      assertTrue(String.format("%d >= %d", dp.timestamp(), seek_timestamp),
                 dp.timestamp() >= seek_timestamp);
      assertEquals(String.format("seek(%d)", seek_timestamp),
                   BASE_TIME + 10000L, dp.timestamp());
      assertEquals(String.format("seek(%d)", seek_timestamp),
                   40, dp.doubleValue(), 0.0000001);
    }
  }

  @Test
  public void testToString() {
    downsampler = new Downsampler(source, THOUSAND_SEC_INTERVAL, AVG);
    DataPoint dp = downsampler.next();
    assertTrue(downsampler.toString().contains(dp.toString()));
  }
}
