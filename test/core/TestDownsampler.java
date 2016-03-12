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

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import com.google.common.collect.Lists;

import net.opentsdb.utils.DateTime;

import org.junit.Before;
import org.junit.Test;

/** Tests {@link Downsampler}. */
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
  private static final TimeZone UTC_TIME_ZONE = DateTime.timezones.get("UTC");
  private static final TimeZone EST_TIME_ZONE = DateTime.timezones.get("EST");
 
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
    downsampler = new Downsampler(source, specification, 0, 0, TimeZone.getDefault(), false);
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
    downsampler = new Downsampler(source, specification, 0, 0, TimeZone.getDefault(), false);
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
    downsampler = new Downsampler(source, specification, 0, 0, TimeZone.getDefault(), false);
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
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE, TimeZone.getDefault(), false);
    
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
        BASE_TIME + 15000L, BASE_TIME + 45000L, TimeZone.getDefault(), false);
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
        BASE_TIME + 65000L, BASE_TIME + 75000L, TimeZone.getDefault(), false);
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
        BASE_TIME - 15000L, BASE_TIME - 5000L, TimeZone.getDefault(), false);
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
    specification = new DownsamplingSpecification("1d-sum");
    //specification.setTimezone(DateTime.timezones.get("America/Denver"));
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE, DateTime.timezones.get("America/Denver"), true);
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
  public void testDownsampler_noData() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] { }));
    specification = new DownsamplingSpecification("1d-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE, TimeZone.getDefault(), false);
    verify(source, never()).next();
    assertFalse(downsampler.hasNext());
  }
  
  @Test
  public void testDownsampler_noDataCalendar() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] { }));
    specification = new DownsamplingSpecification("1m-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE, 
        UTC_TIME_ZONE, true);
    verify(source, never()).next();
    assertFalse(downsampler.hasNext());
  }
  
  @Test
  public void testDownsampler_1day() {
    final DataPoint [] data_points = new DataPoint[4];
    long timestamp = DateTime.toStartOfDay(BASE_TIME, UTC_TIME_ZONE);
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
  
      i += 1;
      long startOfNextInterval = DateTime.toEndOfDay(timestamp, UTC_TIME_ZONE) + 1;
      timestamp = timestamp + (startOfNextInterval - timestamp) / 2;
      value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = startOfNextInterval;
    }
    
    System.out.println(Arrays.toString(data_points));
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    
    downsampler = new Downsampler(source, Downsampler.ONE_DAY_INTERVAL, SUM);
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
    timestamp = DateTime.toStartOfDay(BASE_TIME, UTC_TIME_ZONE);
    for (int i = 0, j = 0; i < values.size(); i++) {
      assertEquals((1 << j++) + (1 << j++), values.get(i), 0.0000001);
      assertEquals(timestamp, timestamps_in_millis.get(i).longValue());
      timestamp = DateTime.toEndOfDay(timestamp, UTC_TIME_ZONE) + 1;
    }
  }

  @Test
  public void testDownsampler_1day_timezone() {
    final DataPoint [] data_points = new DataPoint[4];
    long timestamp = DateTime.toStartOfDay(BASE_TIME, UTC_TIME_ZONE) - EST_TIME_ZONE.getOffset(BASE_TIME);
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
  
      i += 1;
      long startOfNextInterval = DateTime.toEndOfDay(timestamp, EST_TIME_ZONE) + 1;
      timestamp = timestamp + (startOfNextInterval - timestamp) / 2;
      value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = startOfNextInterval;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    specification = new DownsamplingSpecification("1d-sum");
    downsampler = new Downsampler(source, specification, 0, 0, EST_TIME_ZONE, true);
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
    timestamp = DateTime.toStartOfDay(BASE_TIME, UTC_TIME_ZONE) - EST_TIME_ZONE.getOffset(BASE_TIME);
    for (int i = 0, j = 0; i < values.size(); i++) {
      assertEquals((1 << j++) + (1 << j++), values.get(i), 0.0000001);
      assertEquals(timestamp, timestamps_in_millis.get(i).longValue());
      timestamp = DateTime.toEndOfDay(timestamp, EST_TIME_ZONE) + 1;
    }
  }
  
  @Test
  public void testDownsampler_1week() {
    final DataPoint [] data_points = new DataPoint[4];
    long timestamp = DateTime.toStartOfWeek(BASE_TIME, UTC_TIME_ZONE);
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
  
      i += 1;
      long startOfNextInterval = DateTime.toEndOfWeek(timestamp, UTC_TIME_ZONE) + 1;
      timestamp = timestamp + (startOfNextInterval - timestamp) / 2;
      value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = startOfNextInterval;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    
    specification = new DownsamplingSpecification("1w-sum");
    downsampler = new Downsampler(source, specification, 0, 0, UTC_TIME_ZONE, true);
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
    timestamp = DateTime.toStartOfWeek(BASE_TIME, UTC_TIME_ZONE);
    for (int i = 0, j = 0; i < values.size(); i++) {
      assertEquals((1 << j++) + (1 << j++), values.get(i), 0.0000001);
      assertEquals(timestamp, timestamps_in_millis.get(i).longValue());
      timestamp = DateTime.toEndOfWeek(timestamp, UTC_TIME_ZONE) + 1;
    }
  }

  @Test
  public void testDownsampler_1week_timezone() {
    final DataPoint [] data_points = new DataPoint[4];
    long timestamp = DateTime.toStartOfWeek(BASE_TIME, UTC_TIME_ZONE) - EST_TIME_ZONE.getOffset(BASE_TIME);
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
  
      i += 1;
      long startOfNextInterval = DateTime.toEndOfWeek(timestamp, EST_TIME_ZONE) + 1;
      timestamp = timestamp + (startOfNextInterval - timestamp) / 2;
      value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = startOfNextInterval;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    
    specification = new DownsamplingSpecification("1w-sum");
    downsampler = new Downsampler(source, specification, 0, 0, EST_TIME_ZONE, true);
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
    timestamp = DateTime.toStartOfWeek(BASE_TIME, UTC_TIME_ZONE) - EST_TIME_ZONE.getOffset(BASE_TIME);
    for (int i = 0, j = 0; i < values.size(); i++) {
      assertEquals((1 << j++) + (1 << j++), values.get(i), 0.0000001);
      assertEquals(timestamp, timestamps_in_millis.get(i).longValue());
      timestamp = DateTime.toEndOfWeek(timestamp, EST_TIME_ZONE) + 1;
    }
  }

  @Test
  public void testDownsampler_1month() {
    final DataPoint [] data_points = new DataPoint[24];
    long timestamp = DateTime.toStartOfMonth(BASE_TIME, UTC_TIME_ZONE);
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);

      i += 1;
      long startOfNextInterval = DateTime.toEndOfMonth(timestamp, UTC_TIME_ZONE) + 1;
      timestamp = timestamp + (startOfNextInterval - timestamp) / 2;
      value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = startOfNextInterval;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    
    specification = new DownsamplingSpecification("1n-sum");
    downsampler = new Downsampler(source, specification, 0, 0, UTC_TIME_ZONE, true);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }
    
    assertEquals(12, values.size());
    timestamp = DateTime.toStartOfMonth(BASE_TIME, UTC_TIME_ZONE);
    for (int i = 0, j = 0; i < values.size(); i++) {
      assertEquals((1 << j++) + (1 << j++), values.get(i), 0.0000001);
      assertEquals(timestamp, timestamps_in_millis.get(i).longValue());
      timestamp = DateTime.toEndOfMonth(timestamp, UTC_TIME_ZONE) + 1;
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
    
    specification = new DownsamplingSpecification("1d-sum");
    downsampler = new Downsampler(source, specification, 0, 0, UTC_TIME_ZONE, true);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }
    
    assertEquals(12, values.size());
    long timestamp = DateTime.toStartOfMonth(data_points[0].timestamp(), UTC_TIME_ZONE);
    for (int i = 0; i < values.size(); i++) {
      assertEquals(1, values.get(i), 0.0000001);
      assertEquals(timestamp, timestamps_in_millis.get(i).longValue());
      timestamp = DateTime.toEndOfMonth(timestamp, UTC_TIME_ZONE) + 1;
    }
  }
  
  @Test
  public void testDownsampler_2months() {
    final DataPoint [] data_points = new DataPoint[24];
    long timestamp = DateTime.toStartOfMonth(BASE_TIME, UTC_TIME_ZONE);
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);

      i += 1;
      long startOfNextInterval = DateTime.toEndOfMonth(timestamp, UTC_TIME_ZONE) + 1;
      timestamp = timestamp + (startOfNextInterval - timestamp) / 2;
      value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = startOfNextInterval;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    
    specification = new DownsamplingSpecification("2n-sum");
    downsampler = new Downsampler(source, specification, 0, 0, UTC_TIME_ZONE, true);
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
    timestamp = DateTime.toStartOfMonth(BASE_TIME, UTC_TIME_ZONE);
    for (int i = 0, j = 0; i < values.size(); i++) {
      long value = 0;
      for (int k = 0; k < 4; k++) {
        value += (1 << j++);
      }
      assertEquals(value, values.get(i), 0.0000001);
      assertEquals(timestamp, timestamps_in_millis.get(i).longValue());
      timestamp = DateTime.toEndOfMonth(timestamp, UTC_TIME_ZONE) + 1;
      timestamp = DateTime.toEndOfMonth(timestamp, UTC_TIME_ZONE) + 1;
    }
  }

  @Test
  public void testDownsampler_1month_timezone() {
    final DataPoint [] data_points = new DataPoint[24];
    long timestamp = DateTime.toStartOfMonth(BASE_TIME, UTC_TIME_ZONE) - EST_TIME_ZONE.getOffset(BASE_TIME);
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);

      i += 1;
      long startOfNextInterval = DateTime.toEndOfMonth(timestamp, EST_TIME_ZONE) + 1;
      timestamp = timestamp + (startOfNextInterval - timestamp) / 2;
      value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = startOfNextInterval;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    
    specification = new DownsamplingSpecification("1n-sum");
    downsampler = new Downsampler(source, specification, 0, 0, EST_TIME_ZONE, true);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }
    
    assertEquals(12, values.size());
    timestamp = DateTime.toStartOfMonth(BASE_TIME, UTC_TIME_ZONE) - EST_TIME_ZONE.getOffset(BASE_TIME);
    for (int i = 0, j = 0; i < values.size(); i++) {
      assertEquals((1 << j++) + (1 << j++), values.get(i), 0.0000001);
      assertEquals(timestamp, timestamps_in_millis.get(i).longValue());
      timestamp = DateTime.toEndOfMonth(timestamp, EST_TIME_ZONE) + 1;
    }
  }

  @Test
  public void testDownsampler_1year() {
    final DataPoint [] data_points = new DataPoint[4];
    long timestamp = DateTime.toStartOfYear(BASE_TIME, UTC_TIME_ZONE);
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);

      i += 1;
      long startOfNextInterval = DateTime.toEndOfYear(timestamp, UTC_TIME_ZONE) + 1;
      timestamp = timestamp + (startOfNextInterval - timestamp) / 2;
      value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = startOfNextInterval;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    
    specification = new DownsamplingSpecification("1y-sum");
    downsampler = new Downsampler(source, specification, 0, 0, UTC_TIME_ZONE, true);
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
    timestamp = DateTime.toStartOfYear(BASE_TIME, UTC_TIME_ZONE);
    for (int i = 0, j = 0; i < values.size(); i++) {
      assertEquals((1 << j++) + (1 << j++), values.get(i), 0.0000001);
      assertEquals(timestamp, timestamps_in_millis.get(i).longValue());
      timestamp = DateTime.toEndOfYear(timestamp, UTC_TIME_ZONE) + 1;
    }
  }

  @Test
  public void testDownsampler_1year_timezone() {
    final DataPoint [] data_points = new DataPoint[4];
    long timestamp = DateTime.toStartOfYear(BASE_TIME, UTC_TIME_ZONE) - EST_TIME_ZONE.getOffset(BASE_TIME);
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);

      i += 1;
      long startOfNextInterval = DateTime.toEndOfYear(timestamp, EST_TIME_ZONE) + 1;
      timestamp = timestamp + (startOfNextInterval - timestamp) / 2;
      value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = startOfNextInterval;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    
    specification = new DownsamplingSpecification("1y-sum");
    downsampler = new Downsampler(source, specification, 0, 0, EST_TIME_ZONE, true);
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
    timestamp = DateTime.toStartOfYear(BASE_TIME, UTC_TIME_ZONE) - EST_TIME_ZONE.getOffset(BASE_TIME);
    for (int i = 0, j = 0; i < values.size(); i++) {
      assertEquals((1 << j++) + (1 << j++), values.get(i), 0.0000001);
      assertEquals(timestamp, timestamps_in_millis.get(i).longValue());
      timestamp = DateTime.toEndOfYear(timestamp, EST_TIME_ZONE) + 1;
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
    final DataPoint [] data_points = new DataPoint[4];
    long timestamp = DateTime.toStartOfYear(BASE_TIME, UTC_TIME_ZONE);
    final Calendar c = Calendar.getInstance(UTC_TIME_ZONE);
    c.setTimeInMillis(timestamp);
    for (int i = 0; i < data_points.length; i++) {
      long value = 1 << i;
      data_points[i] = MutableDataPoint.ofLongValue(timestamp, value);
      timestamp = DateTime.toEndOfYear(timestamp, UTC_TIME_ZONE) + 1;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));
    
    c.add(Calendar.YEAR, 2);
    specification = new DownsamplingSpecification("1y-sum");
    downsampler = new Downsampler(source, specification, 0, Long.MAX_VALUE, 
        UTC_TIME_ZONE, true);
    System.out.println("SEEK: " + c.getTimeInMillis());
    downsampler.seek(c.getTimeInMillis());
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
    timestamp = DateTime.toStartOfYear(c.getTimeInMillis(), UTC_TIME_ZONE);
    for (int i = 2; i < values.size(); i++) {
      assertEquals(1 << i, values.get(i), 0.0000001);
      assertEquals(timestamp, timestamps_in_millis.get(i).longValue());
      timestamp = DateTime.toEndOfYear(timestamp, UTC_TIME_ZONE) + 1;
    }
    
    source = spy(SeekableViewsForTest.fromArray(data_points));

    c.add(Calendar.MILLISECOND, 1);
    specification = new DownsamplingSpecification("1y-sum");
    downsampler = new Downsampler(source, specification, 0, 0, UTC_TIME_ZONE, true);
    downsampler.seek(c.getTimeInMillis());
    verify(source, never()).next();
    values = Lists.newArrayList();
    timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }
    
    assertEquals(1, values.size());
    timestamp = DateTime.toStartOfYear(c.getTimeInMillis(), UTC_TIME_ZONE);
    for (int i = 3; i < values.size(); i++) {
      assertEquals(1 << i, values.get(i), 0.0000001);
      assertEquals(timestamp, timestamps_in_millis.get(i).longValue());
      timestamp = DateTime.toEndOfYear(timestamp, UTC_TIME_ZONE) + 1;
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
    System.out.println(downsampler.toString());
    assertTrue(downsampler.toString().contains(dp.toString()));
  }
}
