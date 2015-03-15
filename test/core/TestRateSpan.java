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
import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;


/**
 * Tests {@link RateSpan}.
 */
public class TestRateSpan {

  private static final DataPoint[] DATA_POINTS = new DataPoint[] {
    MutableDataPoint.ofDoubleValue(1356998400000L, 40.0),
    MutableDataPoint.ofLongValue(1356998400000L + 2000000, 50),
    MutableDataPoint.ofLongValue(1357002000000L, 40),
    MutableDataPoint.ofDoubleValue(1357002000000L + 5000, 50.0),
    MutableDataPoint.ofLongValue(1357005600000L, 40),
    MutableDataPoint.ofDoubleValue(1357005600000L + 2000000, 50.0)
  };

  private static final DataPoint[] RATE_DATA_POINTS = new DataPoint[] {
    MutableDataPoint.ofDoubleValue(1356998400000L, 40.0 / 1356998400),
    MutableDataPoint.ofDoubleValue(1357000400000L, 10.0 / 2000.0),
    // Low quality datapoint.
    MutableDataPoint.ofDoubleValue(1357002000000L,-10.0 / (1357002000L - 1356998400L - 2000)),
    MutableDataPoint.ofDoubleValue(1357002005000L, 10.0 / 5.0),
    MutableDataPoint.ofDoubleValue(1357005600000L,-10.0 / (1357005600L - 1357002005L)),
    MutableDataPoint.ofDoubleValue(1357007600000L, 10.0 / 2000.0)
  };

  private static final DataPoint[] RATES_AFTER_SEEK = new DataPoint[] {
    // The first rate is calculated against the time zero, not the previous
    // data point.
    MutableDataPoint.ofDoubleValue(1357002000000L, 40.0 / 1357002000),
    RATE_DATA_POINTS[3], RATE_DATA_POINTS[4], RATE_DATA_POINTS[5]
  };

  private static final DataPoint[] COUNTER_DATA_POINTS_ONE = new DataPoint[] {
    MutableDataPoint.ofDoubleValue(1426385507000L, 1),
    MutableDataPoint.ofDoubleValue(1426385508000L, 2),
    MutableDataPoint.ofDoubleValue(1426385509000L, 3),
    MutableDataPoint.ofDoubleValue(1426385510000L, 4),
    MutableDataPoint.ofDoubleValue(1426385511000L, 5),
    MutableDataPoint.ofDoubleValue(1426385512000L, 6)
  };

  private static final DataPoint[] COUNTER_DATA_POINTS_TWO_RESET = new DataPoint[] {
    MutableDataPoint.ofDoubleValue(1426385507000L, 2),
    MutableDataPoint.ofDoubleValue(1426385508000L, 4),
    MutableDataPoint.ofDoubleValue(1426385509000L, 6),
    MutableDataPoint.ofDoubleValue(1426385510000L, 8),
    MutableDataPoint.ofDoubleValue(1426385511000L, 3),
    MutableDataPoint.ofDoubleValue(1426385512000L, 5),
    MutableDataPoint.ofDoubleValue(1426385513000L, 7),
    MutableDataPoint.ofDoubleValue(1426385514000L, 9)
  };

  private SeekableView source;
  private RateOptions options;

  @Before
  public void before() {
    source = SeekableViewsForTest.fromArray(DATA_POINTS);
    options = new RateOptions();
  }

  @Test
  public void testRateSpan() {
    RateSpan rate_span = new RateSpan(source, options);
    // The first rate is between the time zero and the first data point.
    assertTrue(rate_span.hasNext());
    DataPoint dp = rate_span.next();
    assertFalse(dp.isInteger());
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(40.0 / 1356998400L, dp.doubleValue(), 0);
    // The second rate comes from the first two data points.
    assertTrue(rate_span.hasNext());
    DataPoint dp2 = rate_span.next();
    assertFalse(dp2.isInteger());
    assertEquals(1356998400000L + 2000000, dp2.timestamp());
    assertEquals(10.0 / 2000.0, dp2.doubleValue(), 0);
  }

  @Test
  public void testNext_iterateAll() {
    RateSpan rate_span = new RateSpan(source, options);
    for (DataPoint rate : RATE_DATA_POINTS) {
      assertTrue(rate_span.hasNext());
      assertTrue(rate_span.hasNext());
      DataPoint dp = rate_span.next();
      String msg = String.format("expected rate = '%s' ", rate);
      assertFalse(msg, dp.isInteger());
      assertEquals(msg, rate.timestamp(), dp.timestamp());
      assertEquals(msg, rate.doubleValue(), dp.doubleValue(), 0.0000001);
    }
    assertFalse(rate_span.hasNext());
    assertFalse(rate_span.hasNext());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRemove() {
    RateSpan rate_span = new RateSpan(source, options);
    rate_span.remove();
  }

  @Test(expected = NoSuchElementException.class)
  public void testNext_noMoreData() {
    source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(1356998400000L, 40)
    });
    RateSpan rate_span = new RateSpan(source, options);
    // The first rate exists.
    assertTrue(rate_span.hasNext());
    rate_span.next();
    // No second rate.
    assertFalse(rate_span.hasNext());
    rate_span.next();
  }

  @Test
  public void testSeek() {
    RateSpan rate_span = new RateSpan(source, options);
    rate_span.seek(1357002000000L);
    for (DataPoint rate : RATES_AFTER_SEEK) {
      assertTrue(rate_span.hasNext());
      assertTrue(rate_span.hasNext());
      DataPoint dp = rate_span.next();
      String msg = String.format("expected rate = '%s' ", rate);
      assertFalse(msg, dp.isInteger());
      assertEquals(msg, rate.timestamp(), dp.timestamp());
      assertEquals(msg, rate.doubleValue(), dp.doubleValue(), 0.0000001);
    }
    assertFalse(rate_span.hasNext());
    assertFalse(rate_span.hasNext());
  }

  @Test(expected = IllegalStateException.class)
  public void testNext_decreasingTimestamps() {
    source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(1357002000000L + 5000, 50),
        MutableDataPoint.ofLongValue(1357002000000L + 4000, 50)
      });
    RateSpan rate_span = new RateSpan(source, options);
    rate_span.next();
  }

  @Test(expected = IllegalStateException.class)
  public void testMoveToNextRate_duplicatedTimestamps() {
    source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(1356998400000L, 40),
        MutableDataPoint.ofLongValue(1356998400000L + 2000000, 50),
        MutableDataPoint.ofLongValue(1356998400000L + 2000000, 50)
    });
    RateSpan rate_span = new RateSpan(source, options);
    rate_span.next();  // Abandons the first rate to test next ones.
    assertTrue(rate_span.hasNext());
    rate_span.next();
  }

  @Test
  public void testCalculateDelta_bigLongValues() {
    source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(1356998400000L, Long.MAX_VALUE - 100),
        MutableDataPoint.ofLongValue(1356998500000L, Long.MAX_VALUE - 20)
    });
    RateSpan rate_span = new RateSpan(source, options);
    rate_span.next();  // Abandons the first rate to test next ones.
    assertTrue(rate_span.hasNext());
    DataPoint dp = rate_span.next();
    assertFalse(dp.isInteger());
    assertEquals(1356998500000L, dp.timestamp());
    assertEquals(0.8, dp.doubleValue(), 0);
    assertFalse(rate_span.hasNext());
  }

  @Test
  public void noCounterNoResetSteadyRate() {
    source = SeekableViewsForTest.fromArray(COUNTER_DATA_POINTS_ONE);
    options = new RateOptions(false);
    RateSpan rate_span = new RateSpan(source, options);
    int count = 0;
    rate_span.next(); // Skip first datapoint.
    while(rate_span.hasNext()) {
      DataPoint rateDp = rate_span.next();
      assertEquals(1, rateDp.doubleValue(), 0);
      count++;
    }
    assertEquals(5, count);
  }

  @Test
  public void counterNoResetSteadyRate() {
    source = SeekableViewsForTest.fromArray(COUNTER_DATA_POINTS_ONE);
    options = new RateOptions(true);
    RateSpan rate_span = new RateSpan(source, options);
    int count = 0;
    rate_span.next(); // Skip first datapoint.
    while(rate_span.hasNext()) {
      DataPoint rateDp = rate_span.next();
      assertEquals(1, rateDp.doubleValue(), 0);
      count++;
    }
    assertEquals(5, count);
  }

  @Test
  public void noCounterResetHasNegativeDatapoint() {
    source = SeekableViewsForTest.fromArray(COUNTER_DATA_POINTS_TWO_RESET);
    options = new RateOptions(false);
    RateSpan rate_span = new RateSpan(source, options);
    int count = 0;
    rate_span.next(); // Throw away first datapoint.
    double[] values = new double[7];
    while(rate_span.hasNext()) {
      DataPoint rateDp = rate_span.next();
      values[count++] = rateDp.doubleValue();
    }
    assertEquals(7, count);
    assertTrue(Arrays.equals(new double[]{2,2,2,-5,2,2,2}, values));
  }

  @Test
  public void CounterResetSkipLowQualityDatapoint() {
    source = SeekableViewsForTest.fromArray(COUNTER_DATA_POINTS_TWO_RESET);
    options = new RateOptions(true);
    RateSpan rate_span = new RateSpan(source, options);
    int count = 0;
    rate_span.next(); // Throw away first datapoint.
    double[] values = new double[6];
    while(rate_span.hasNext()) {
      DataPoint rateDp = rate_span.next();
      values[count++] = rateDp.doubleValue();
    }
    assertEquals(6, count);
    assertTrue(Arrays.equals(new double[]{2,2,2,2,2,2}, values));
  }

}
