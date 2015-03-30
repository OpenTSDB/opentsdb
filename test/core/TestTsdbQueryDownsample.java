// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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

import net.opentsdb.utils.DateTime;

import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests downsampling with query.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ Scanner.class })
public class TestTsdbQueryDownsample extends BaseTsdbTest {
  protected TsdbQuery query = null;

  @Before
  public void beforeLocal() throws Exception {
    query = new TsdbQuery(tsdb);
  }

  @Test
  public void downsample() throws Exception {
    int downsampleInterval = (int)DateTime.parseDuration("60s");
    query.downsample(downsampleInterval, Aggregators.SUM);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    assertEquals(60000, TsdbQuery.ForTesting.getDownsampleIntervalMs(query));
    long scanStartTime = 1356998400 - Const.MAX_TIMESPAN * 2 - 60;
    assertEquals(scanStartTime, TsdbQuery.ForTesting.getScanStartTimeSeconds(query));
    long scanEndTime = 1357041600 + Const.MAX_TIMESPAN + 1 + 60;
    assertEquals(scanEndTime, TsdbQuery.ForTesting.getScanEndTimeSeconds(query));
  }

  @Test
  public void downsampleMilliseconds() throws Exception {
    int downsampleInterval = (int)DateTime.parseDuration("60s");
    query.downsample(downsampleInterval, Aggregators.SUM);
    query.setStartTime(1356998400000L);
    query.setEndTime(1357041600000L);
    assertEquals(60000, TsdbQuery.ForTesting.getDownsampleIntervalMs(query));
    long scanStartTime = 1356998400 - Const.MAX_TIMESPAN * 2 - 60;
    assertEquals(scanStartTime, TsdbQuery.ForTesting.getScanStartTimeSeconds(query));
    long scanEndTime = 1357041600 + Const.MAX_TIMESPAN + 1 + 60;
    assertEquals(scanEndTime, TsdbQuery.ForTesting.getScanEndTimeSeconds(query));
  }

  @Test (expected = NullPointerException.class)
  public void downsampleNullAgg() throws Exception {
    query.downsample(60, null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void downsampleInvalidInterval() throws Exception {
    query.downsample(0, Aggregators.SUM);
  }

  @Test
  public void runLongSingleTSDownsample() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60000, Aggregators.AVG);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    // Timeseries in intervals: (1), (2, 3), (4, 5), ... (298, 299), (300)
    int i = 0;
    for (DataPoint dp : dps[0]) {
      // Downsampler outputs just doubles.
      assertFalse(dp.isInteger());
      if (i == 0) {
        // The first time interval has just one value - (1).
        assertEquals(1, dp.doubleValue(), 0.00001);
      } else if (i >= 150) {
        // The last time interval has just one value - (300).
        assertEquals(300, dp.doubleValue(), 0.00001);
      } else {
        // Each interval has two consecutive numbers - (i*2, i*2+1).
        // Takes the average of the values of this interval.
        double value = i * 2 + 0.5;
        assertEquals(value, dp.doubleValue(), 0.00001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 60000);
      ++i;
    }
    // Out of 300 values, the first and the last intervals have one value each,
    // and the 149 intervals in the middle have two values for each.
    assertEquals(151, dps[0].size());
  }

  @Test
  public void runLongSingleTSDownsampleMs() throws Exception {
    storeLongTimeSeriesMs();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(1000, Aggregators.AVG);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    // Timeseries in intervals: (1), (2, 3), (4, 5), ... (298, 299), (300)
    int i = 0;
    for (DataPoint dp : dps[0]) {
      // Downsampler outputs just doubles.
      assertFalse(dp.isInteger());
      if (i == 0) {
        // The first time interval has just one value - (1).
        assertEquals(1, dp.doubleValue(), 0.00001);
      } else if (i >= 150) {
        // The last time interval has just one value - (300).
        assertEquals(300, dp.doubleValue(), 0.00001);
      } else {
        // Each interval has two consecutive numbers - (i*2, i*2+1).
        // Takes the average of the values of this interval.
        double value = i * 2 + 0.5;
        assertEquals(value, dp.doubleValue(), 0.00001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 1000);
      ++i;
    }
    // Out of 300 values, the first and the last intervals have one value each,
    // and the 149 intervals in the middle have two values for each.
    assertEquals(151, dps[0].size());
  }

  @Test
  public void runLongSingleTSDownsampleAndRate() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60000, Aggregators.AVG);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    // Timeseries in intervals: (1), (2, 3), (4, 5), ... (298, 299), (300)
    // After downsampling: 1, 2.5, 4.5, ... 298.5, 300
    long expected_timestamp = 1356998460000L;
    int i = 0;
    for (DataPoint dp : dps[0]) {
      assertFalse(dp.isInteger());
      if (i == 0) {
        // The value of the first interval is one and the next one is 2.5
        // 0.025 = (2.5 - 1) / 60 seconds.
        assertEquals(0.025F, dp.doubleValue(), 0.001);
      } else if (i >= 149) {
        // The value of the last interval is 300 and the previous one is 298.5
        // 0.025 = (300 - 298.5) / 60 seconds.
        assertEquals(0.025F, dp.doubleValue(), 0.00001);
      } else {
        // 0.033 = 2 / 60 seconds where 2 is the difference of the values
        // of two consecutive intervals.
        assertEquals(0.033F, dp.doubleValue(), 0.001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 60000);
      assertEquals(expected_timestamp, dp.timestamp());
      expected_timestamp += 60000;
      ++i;
    }
    assertEquals(150, dps[0].size());
  }

  @Test
  public void runLongSingleTSDownsampleAndRateMs() throws Exception {
    storeLongTimeSeriesMs();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(1000, Aggregators.AVG);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    // Timeseries in intervals: (1), (2, 3), (4, 5), ... (298, 299), (300)
    // After downsampling: 1, 2.5, 4.5, ... 298.5, 300
    int i = 0;
    for (DataPoint dp : dps[0]) {
      assertFalse(dp.isInteger());
      if (i == 0) {
        // The value of the first interval is one and the next one is 2.5
        // 1.5 = (2.5 - 1) / 1.000 seconds.
        assertEquals(1.5F, dp.doubleValue(), 0.001);
      } else if (i >= 149) {
        // The value of the last interval is 300 and the previous one is 298.5
        // 1.5 = (300 - 298.5) / 1.000 seconds.
        assertEquals(1.5, dp.doubleValue(), 0.00001);
      } else {
        // 2 = 2 / 1.000 seconds where 2 is the difference of the values
        // of two consecutive intervals.
        assertEquals(2F, dp.doubleValue(), 0.001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 1000);
      ++i;
    }
    assertEquals(150, dps[0].size());
  }

  @Test
  public void runFloatSingleTSDownsample() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60000, Aggregators.AVG);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    // Timeseries in intervals: (1.25), (1.5, 1.75), (2, 2.25), ...
    // (75.5, 75.75), (76).
    // After downsampling: 1.25, 1.625, 2.125, ... 75.625, 76
    int i = 0;
    for (DataPoint dp : dps[0]) {
      if (i == 0) {
        // The first time interval has just one value - 1.25.
        assertEquals(1.25, dp.doubleValue(), 0.00001);
      } else if (i >= 150) {
        // The last time interval has just one value - 76.
        assertEquals(76D, dp.doubleValue(), 0.00001);
      } else {
        // Each interval has two consecutive numbers - (i*0.5+1, i*0.5+1.25).
        // Takes the average of the values of this interval.
        double value = (i + 2.25) / 2;
        assertEquals(value, dp.doubleValue(), 0.00001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 60000);
      ++i;
    }
    // Out of 300 values, the first and the last intervals have one value each,
    // and the 149 intervals in the middle have two values for each.
    assertEquals(151, dps[0].size());
  }

  @Test
  public void runFloatSingleTSDownsampleMs() throws Exception {
    storeFloatTimeSeriesMs();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(1000, Aggregators.AVG);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    // Timeseries in intervals: (1.25), (1.5, 1.75), (2, 2.25), ...
    // (75.5, 75.75), (76).
    // After downsampling: 1.25, 1.625, 2.125, ... 75.625, 76
    int i = 0;
    for (DataPoint dp : dps[0]) {
      if (i == 0) {
        // The first time interval has just one value.
        assertEquals(1.25, dp.doubleValue(), 0.00001);
      } else if (i >= 150) {
        // The last time interval has just one value.
        assertEquals(76D, dp.doubleValue(), 0.00001);
      } else {
        // Each interval has two consecutive numbers - (i*0.5+1, i*0.5+1.25).
        // Takes the average of the values of this interval.
        double value = (i + 2.25) / 2;
        assertEquals(value, dp.doubleValue(), 0.00001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 1000);
      ++i;
    }
    // Out of 300 values, the first and the last intervals have one value each,
    // and the 149 intervals in the middle have two values for each.
    assertEquals(151, dps[0].size());
  }

  @Test
  public void runFloatSingleTSDownsampleAndRate() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60000, Aggregators.AVG);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    // Timeseries in intervals: (1.25), (1.5, 1.75), (2, 2.25), ...
    // (75.5, 75.75), (76).
    // After downsampling: 1.25, 1.625, 2.125, ... 75.625, 76
    long expected_timestamp = 1356998460000L;
    int i = 0;
    for (DataPoint dp : dps[0]) {
      assertFalse(dp.isInteger());
      if (i == 0) {
        // The value of the first interval is 1.25 and the next one is 1.625
        // 0.00625 = (1.625 - 1.25) / 60 seconds.
        assertEquals(0.00625F, dp.doubleValue(), 0.000001);
      } else if (i >= 149) {
        // The value of the last interval is 76 and the previous one is 75.625
        // 0.00625 = (76 - 75.625) / 60 seconds.
        assertEquals(0.00625F, dp.doubleValue(), 0.000001);
      } else {
        // 0.00833 = 0.5 / 60 seconds where 0.5 is the difference of the values
        // of two consecutive intervals.
        assertEquals(0.00833F, dp.doubleValue(), 0.00001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 60000);
      assertEquals(expected_timestamp, dp.timestamp());
      expected_timestamp += 60000;
      ++i;
    }
    assertEquals(150, dps[0].size());
  }

  @Test
  public void runFloatSingleTSDownsampleAndRateMs() throws Exception {
    storeFloatTimeSeriesMs();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(1000, Aggregators.AVG);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    // Timeseries in intervals: (1.25), (1.5, 1.75), (2, 2.25), ...
    // (75.5, 75.75), (76).
    // After downsampling: 1.25, 1.625, 2.125, ... 75.625, 76
    int i = 0;
    for (DataPoint dp : dps[0]) {
      if (i == 0) {
        // The value of the first interval is 1.25 and the next one is 1.625
        // 0.375 = (1.625 - 1.25) / 1.000 seconds.
        assertEquals(0.375F, dp.doubleValue(), 0.000001);
      } else if (i >= 149) {
        // The value of the last interval is 76 and the previous one is 75.625
        // 0.375 = (76 - 75.625) / 1.000 seconds.
        assertEquals(0.375F, dp.doubleValue(), 0.000001);
      } else {
        // 0.5 = 0.5 / 1.000 seconds where 0.5 is the difference of the values
        // of two consecutive intervals.
        assertEquals(0.5F, dp.doubleValue(), 0.00001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 1000);
      ++i;
    }
    assertEquals(150, dps[0].size());
  }

  @Test
  public void runLongSingleTSDownsampleCount() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60000, Aggregators.COUNT);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    // Timeseries in intervals: (1), (2, 3), (4, 5), ... (298, 299), (300)
    int i = 0;
    for (DataPoint dp : dps[0]) {
      // Downsampler outputs just doubles.
      assertFalse(dp.isInteger());
      if (i == 0 || i == 150) {
        assertEquals(1, dp.doubleValue(), 0.00001);
      } else {
        assertEquals(2, dp.doubleValue(), 0.00001);
      }
      ++i;
    }
    // Out of 300 values, the first and the last intervals have one value each,
    // and the 149 intervals in the middle have two values for each.
    assertEquals(151, dps[0].size());
  }
  
  // this could happen.
  @Test
  public void runFloatSingleTSDownsampleAndRateAndCount() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60000, Aggregators.COUNT);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    // Timeseries in intervals: (1.25), (1.5, 1.75), (2, 2.25), ...
    // (75.5, 75.75), (76).
    // After downsampling: 1.25, 1.625, 2.125, ... 75.625, 76
    long expected_timestamp = 1356998460000L;
    int i = 0;
    for (DataPoint dp : dps[0]) {
      assertFalse(dp.isInteger());
      if (i == 0) {
        // switching from 1 value to 2 on the first point
        assertEquals(0.016666F, dp.doubleValue(), 0.00001);
      } else if (i == 149) {
        // switching from 2 values to 1 on the last point
        assertEquals(-0.016666F, dp.doubleValue(), 0.00001);
      } else {
        // no difference between the value counts for most of these so zero
        assertEquals(0.000F, dp.doubleValue(), 0.00001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 60000);
      assertEquals(expected_timestamp, dp.timestamp());
      expected_timestamp += 60000;
      ++i;
    }
    assertEquals(150, dps[0].size());
  }

}
