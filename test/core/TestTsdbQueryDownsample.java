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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import net.opentsdb.utils.DateTime;

import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.math.DoubleMath;

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
  public void downsampleFullyAligned() {
    testDownsampleScanBounds(
      60000L,
      1356998400L, 1357041600L,

      // The scan start time should be exactly the same as the query start time
      // because it is aligned on both boundaries, timespan and interval.
      1356998400L,

      // However, because the query end time is already aligned on an interval,
      // it should be snapped forward yet another interval and then snapped
      // forward to the next timespan, which is an entire extra hour.
      1357045200L);
  }

  @Test
  public void downsampleUnaligned() {
    final long fifteen_minutes = 60L * 15L;
    final long twelve_hours = 3600L * 12L;
    final long now = 1427415547L; // Thu Mar 26 17:19:07 2015 GMT-7:00 DST

    testDownsampleScanBounds(
      1000L * fifteen_minutes,
      now - twelve_hours, now,

      // Start time should have been snapped back to 1427415300 (5:15a) for the
      // interval, then back to 1427371200 (5:00a) for the timespan.
      1427371200L,

      // End time should have been snapped forward to 1427416200 (5:30p) for
      // the interval, then forward to 1427418000 (6:00p) for the timespan.
      1427418000L);
  }

  @Test
  public void downsampleWeirdly() {
    final long day = 3600L * 24L;
    final long twelve_hours = 3600L * 12L;

    // Thu Mar 26 17:19:07 2015 GMT-7:00 DST
    // Fri, 27 Mar 2015 00:19:07 GMT
    final long now = 1427415547L;

    testDownsampleScanBounds(
      1000L * day,
      now - twelve_hours, now,

      // Start time should be midnight UTC, 26 March.
      1427328000L,

      // End time should be midnight UTC, 28 March.
      1427500800L);
  }

  @Test
  public void downsampleMilliseconds() throws Exception {
    final long start_time = 1356998400000L;
    final long end_time = 1357041600000L;
    final long downsample_interval = DateTime.parseDuration("60s");

    query.downsample(downsample_interval, Aggregators.SUM);
    query.setStartTime(start_time);
    query.setEndTime(end_time);
    assertEquals(60000L, TsdbQuery.ForTesting.getDownsampleIntervalMs(query));

    // The scan start time should be exactly the same as the query start time
    // because it is aligned on both boundaries, timespan and interval.
    assertEquals(start_time / 1000L,
      TsdbQuery.ForTesting.getScanStartTimeSeconds(query));

    // However, because the query end time is already aligned on an interval,
    // it should be snapped forward yet another interval and then snapped
    // forward to the next timespan, which is an entire extra hour.
    assertEquals((end_time + 3600000L) / 1000L,
      TsdbQuery.ForTesting.getScanEndTimeSeconds(query));
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

  /**
   * A helper interface to be used by the filling-test code. 
   */ 
  interface Validator {
    /** @return true if the argument is valid. */
    boolean isValidValue(double value);

    /** @return the fill policy to be used while downsampling. */
    FillPolicy getFillPolicy();

    /** @return true if the argument is the sentinel for empty intervals. */
    boolean isMissingValue(double value);
  }

  // Fill missing intervals with NaNs.
  abstract class NaNValidator implements Validator {
    @Override
    public FillPolicy getFillPolicy() {
      return FillPolicy.NOT_A_NUMBER;
    }

    @Override
    public boolean isMissingValue(final double value) {
      return Double.isNaN(value);
    }
  }

  // Fill missing intervals with zeroes.
  abstract class ZeroValidator implements Validator {
    @Override
    public FillPolicy getFillPolicy() {
      return FillPolicy.ZERO;
    }

    @Override
    public boolean isMissingValue(final double value) {
      return DoubleMath.fuzzyEquals(0.0, value, 0.0001);
    }
  }

  @Test
  public void runSumAvgLongSingleTSDownsampleWNulls() throws Exception {
    storeLongTimeSeriesWithMissingData();

    runTSDownsampleWithMissingData(Aggregators.SUM, Aggregators.AVG,
      // 301.5, 301.5, 301.5, ...
      new NaNValidator() {
        @Override
        public boolean isValidValue(final double value) {
          return DoubleMath.fuzzyEquals(301.5, value, 0.0001);
        }
      });
  }

  @Test
  public void runAvgSumLongSingleTSDownsampleWNulls() throws Exception {
    storeLongTimeSeriesWithMissingData();

    runTSDownsampleWithMissingData(Aggregators.AVG, Aggregators.SUM,
      // 152, 301.5, 155, 301.5, 158, 301.5, ...
      new NaNValidator() {
        private boolean even = false;
        private double even_expected = 149.0;

        @Override
        public boolean isValidValue(final double value) {
          even = !even;
          if (even) {
            even_expected += 3.0;
            return DoubleMath.fuzzyEquals(even_expected, value, 0.0001);
          } else {
            return DoubleMath.fuzzyEquals(301.5, value, 0.0001);
          }
        }
      });
  }

  @Test
  public void runAvgAvgLongSingleTSDownsampleWNulls() throws Exception {
    storeLongTimeSeriesWithMissingData();

    runTSDownsampleWithMissingData(Aggregators.AVG, Aggregators.AVG,
      // 150.75, 150.75, 150.75, ...
      new ZeroValidator() {
        @Override
        public boolean isValidValue(final double value) {
          return DoubleMath.fuzzyEquals(150.75, value, 0.0001);
        }
      });
  }

  @Test
  public void runSumSumLongSingleTSDownsampleWNulls() throws Exception {
    storeLongTimeSeriesWithMissingData();

    runTSDownsampleWithMissingData(Aggregators.SUM, Aggregators.SUM,
      // 304, 603, 310, 603, 316, 603, ...
      new NaNValidator() {
        private double even_expected = 298.0;
        private final double odd_expected = 603.0;
        private boolean even = false;

        @Override
        public boolean isValidValue(final double value) {
          even = !even;
          if (even) {
            even_expected += 6.0;
            return DoubleMath.fuzzyEquals(even_expected, value, 0.0001);
          } else {
            return DoubleMath.fuzzyEquals(odd_expected, value, 0.0001);
          }
        }
      });
  }

  @Test
  public void runMinMinLongSingleTSDownsampleWNulls() throws Exception {
    storeLongTimeSeriesWithMissingData();

    runTSDownsampleWithMissingData(Aggregators.MIN, Aggregators.MIN,
      // 2, 5, 8, ..., 143, 146, 149, 149, 145, 143, 139, 133, 131, ...
      new ZeroValidator() {
        private double even_expected = -4.0;
        private double even_change = 6.0;
        private double odd_expected = -1.0;
        private double odd_change = 6.0;
        private boolean even = false;

        @Override
        public boolean isValidValue(final double value) {
          even = !even;
          if (even) {
            even_expected += even_change;

            // Check for the point at which even terms change.
            if (DoubleMath.fuzzyEquals(even_expected, 152.0, 0.0001)) {
              // After this point, even terms begin decreasing by six.
              even_expected = 149.0;
              even_change = -6.0;
            }

            return DoubleMath.fuzzyEquals(even_expected, value, 0.0001);
          } else {
            odd_expected += odd_change;

            // Check for the point at which odd terms change.
            if (DoubleMath.fuzzyEquals(odd_expected, 155.0, 0.0001)) {
              // After this point, odd terms begin decreasing by six.
              odd_expected = 145.0;
              odd_change = -6.0;
            }

            return DoubleMath.fuzzyEquals(odd_expected, value, 0.0001);
          }
        }
      });
  }

  @Test
  public void runMinSumLongSingleTSDownsampleWNulls() throws Exception {
    storeLongTimeSeriesWithMissingData();

    runTSDownsampleWithMissingData(Aggregators.MIN, Aggregators.SUM,
      // 5, 11, 17, 23, ..., 197, 203, 197, 215, 191, 227, 185, 239, ...,
      // 287, 155, 299, 149, 292, 143, 280, ...
      new NaNValidator() {
        private double even_expected = -7.0;
        private double even_change = 12.0;
        private double odd_expected = -1.0;
        private double odd_change = 12.0;
        private boolean even = false;

        @Override
        public boolean isValidValue(final double value) {
          even = !even;
          if (even) {
            even_expected += even_change;

            // Check for the point at which even terms change.
            if (DoubleMath.fuzzyEquals(even_expected, 209.0, 0.0001)) {
              // After this point, even terms begin decreasing by six.
              even_expected = 197.0;
              even_change = -6.0;
            }

            return DoubleMath.fuzzyEquals(even_expected, value, 0.0001);
          } else {
            odd_expected += odd_change;

            // Check for the point at which odd terms change.
            if (DoubleMath.fuzzyEquals(odd_expected, 311.0, 0.0001)) {
              // After this point, odd terms begin decreasing by twelve.
              odd_expected = 292.0;
              odd_change = -12.0;
            }

            return DoubleMath.fuzzyEquals(odd_expected, value, 0.0001);
          }
        }
      });
  }

  @Test
  public void runSumMinLongSingleTSDownsampleWNulls() throws Exception {
    storeLongTimeSeriesWithMissingData();

    runTSDownsampleWithMissingData(Aggregators.SUM, Aggregators.MIN,
      // 301, 300, 301, 300, ...
      new NaNValidator() {
        private boolean even = false;

        @Override
        public boolean isValidValue(final double value) {
          even = !even;
          return DoubleMath.fuzzyEquals(
            even ? 301.0 : 300.0, value, 0.0001);
        }
      });
  }

  /**
   * Precondition: the time series have been stored.
   */
  public void runTSDownsampleWithMissingData(final Aggregator queryAggregator,
      final Aggregator downsampleAggregator, final Validator validator)
      throws Exception {
    final long start_time = 1356998400L;
    final long end_time = 1357041600L;
    final int ds_interval = 30;
    final long ds_interval_ms = 1000L * ds_interval;
    final String metric = METRIC_STRING;

    final HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(start_time);
    query.setEndTime(end_time);
    query.downsample(ds_interval_ms, downsampleAggregator,
      validator.getFillPolicy());
    query.setTimeSeries(metric, tags, queryAggregator, false);
    final DataPoints[] dps = query.run();

    assertNotNull(dps);
    assertEquals(metric, dps[0].metricName());
    assertFalse(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());

    // For the reasoning behind this calculation, see the following methods:
    //   TsdbQuery#getScanStartTimeSeconds()
    //   TsdbQuery#getScanEndTimeSeconds()

    int i = 0;
    long expected_timestamp_ms = 1000L * start_time;
    for (final DataPoint dp : dps[0]) {
      // Downsampler outputs just doubles.
      assertFalse(dp.isInteger());

      // There should be only one hundred valid values.
      if (i++ < 100) {
        // Check the value.
        assertTrue(validator.isValidValue(dp.doubleValue()));
      } else {
        // Otherwise, the value should be the special missing value.
        assertTrue(validator.isMissingValue(dp.doubleValue()));
      }

      // The timestamp should match our expectation based on the interval.
      assertEquals(expected_timestamp_ms, dp.timestamp());

      // Move to the next expected interval.
      expected_timestamp_ms += ds_interval_ms;
    }

    // Ensure we got the number of points we expected.
    assertEquals((end_time - start_time + 3600L) / ds_interval, dps[0].size());
  }

  /**
   * Helper to test the start and stop times in a query for downsampling
   * @param downsample_interval The downsample interval
   * @param start_time The start of the query
   * @param end_time The end of the query
   * @param expected_start_time What we expect the TSDBQuery class to give us
   * @param expected_end_time What we expect the TSDBQuery class to give us
   */
  private void testDownsampleScanBounds(final long downsample_interval,
      final long start_time, final long end_time,
      final long expected_start_time, final long expected_end_time) {
    query.downsample(downsample_interval, Aggregators.SUM);
    query.setStartTime(start_time);
    query.setEndTime(end_time);

    assertEquals(expected_start_time,
      TsdbQuery.ForTesting.getScanStartTimeSeconds(query));

    assertEquals(expected_end_time,
      TsdbQuery.ForTesting.getScanEndTimeSeconds(query));
  }
}
