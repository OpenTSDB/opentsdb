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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import net.opentsdb.core.Aggregators.Interpolation;
import net.opentsdb.utils.DateTime;
import org.junit.Before;
import org.junit.Test;


/** Tests {@link AggregationIter}. */
public class TestAggregationIter {

  private static final long BASE_TIME = 1356998400000L;
  private static final DataPoint[] DATA_POINTS_1 = new DataPoint[] {
    MutableDataPoint.ofLongValue(BASE_TIME, 40),
    MutableDataPoint.ofLongValue(BASE_TIME + 10000, 50),
    MutableDataPoint.ofLongValue(BASE_TIME + 30000, 70)
  };
  private static final DataPoint[] DATA_POINTS_2 = new DataPoint[] {
    MutableDataPoint.ofLongValue(BASE_TIME + 10000, 37),
    MutableDataPoint.ofLongValue(BASE_TIME + 20000, 48)
  };
  final DataPoint[] DATA_5SEC = new DataPoint[] {
      MutableDataPoint.ofDoubleValue(BASE_TIME + 00000L, 1),
      MutableDataPoint.ofDoubleValue(BASE_TIME + 07000L, 1),
      MutableDataPoint.ofDoubleValue(BASE_TIME + 10000L, 1),
      MutableDataPoint.ofDoubleValue(BASE_TIME + 15000L, 1),
      MutableDataPoint.ofDoubleValue(BASE_TIME + 20000L, 1),
      MutableDataPoint.ofDoubleValue(BASE_TIME + 25000L, 1),
      MutableDataPoint.ofDoubleValue(BASE_TIME + 30000L, 1),
      MutableDataPoint.ofDoubleValue(BASE_TIME + 35000L, 1),
      MutableDataPoint.ofDoubleValue(BASE_TIME + 40000L, 1),
      MutableDataPoint.ofDoubleValue(BASE_TIME + 45000L, 1),
      MutableDataPoint.ofDoubleValue(BASE_TIME + 50000L, 1)
  };
  private static final Aggregator AVG = Aggregators.get("avg");
  private static final Aggregator SUM = Aggregators.get("sum");

  private SeekableView[] iterators;
  private long start_time_ms;
  private long end_time_ms;
  private boolean rate;
  private Aggregator aggregator;
  private Interpolation interpolation;


  @Before
  public void setUp() {
    start_time_ms = 1356998400L * 1000;
    end_time_ms = 1356998500L * 1000;
    rate = false;
    aggregator = Aggregators.SUM;
    interpolation = Interpolation.LERP;
  }

  @Test
  public void testAggregate_singleSpan() {
    iterators = new SeekableView[] {
        SeekableViewsForTest.fromArray(DATA_POINTS_1)
    };
    AggregationIter sgai = AggregationIter.createForTesting(iterators,
        start_time_ms, end_time_ms, aggregator, interpolation,rate);
    // Aggregating a single span should repeat the single span.
    for (DataPoint expected: DATA_POINTS_1) {
      assertTrue(sgai.hasNext());
      DataPoint dp = sgai.next();
      assertEquals(expected.timestamp(), dp.timestamp());
      assertEquals(expected.longValue(), dp.longValue());
    }
    assertFalse(sgai.hasNext());
  }

  @Test
  public void testAggregate_doubleSpans() {
    iterators = new SeekableView[] {
        SeekableViewsForTest.fromArray(DATA_POINTS_1),
        SeekableViewsForTest.fromArray(DATA_POINTS_2),
    };
    AggregationIter sgai = AggregationIter.createForTesting(iterators,
        start_time_ms, end_time_ms, aggregator, interpolation, rate);
    // Checks if all the distinct timestamps of both spans appear and missing
    // data point of one span for a timestamp of one span was interpolated.
    DataPoint[] expected_data_points = new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 10000, 50 + 37),
        // 60 is the interpolated value.
        MutableDataPoint.ofLongValue(BASE_TIME + 20000, 60 + 48),
        MutableDataPoint.ofLongValue(BASE_TIME + 30000, 70)
      };
    for (DataPoint expected: expected_data_points) {
      assertTrue(sgai.hasNext());
      DataPoint dp = sgai.next();
      assertEquals(expected.timestamp(), dp.timestamp());
      assertEquals(expected.longValue(), dp.longValue());
    }
    assertFalse(sgai.hasNext());
  }

  @Test
  public void testAggregate_manySpansWithDownsampling() {
    iterators = new SeekableView[] {
        new Downsampler(SeekableViewsForTest.fromArray(DATA_5SEC), 10000, AVG),
        new Downsampler(SeekableViewsForTest.fromArray(DATA_5SEC), 10000, AVG),
        new Downsampler(SeekableViewsForTest.fromArray(DATA_5SEC), 10000, AVG),
        new Downsampler(SeekableViewsForTest.fromArray(DATA_5SEC), 10000, AVG),
        new Downsampler(SeekableViewsForTest.fromArray(DATA_5SEC), 10000, AVG),
        new Downsampler(SeekableViewsForTest.fromArray(DATA_5SEC), 10000, AVG),
        new Downsampler(SeekableViewsForTest.fromArray(DATA_5SEC), 10000, AVG)
    };
    // Starting at 01 second causes abandoning the data of the first 10 seconds
    // by the first round of ten-second downsampling.
    start_time_ms = BASE_TIME + 1000L;
    end_time_ms = BASE_TIME + 100000;
    AggregationIter sgai = AggregationIter.createForTesting(iterators,
        start_time_ms, end_time_ms, aggregator, interpolation,
        rate);
    DataPoint[] expected_data_points = new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10000L, 7),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 20000L, 7),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30000L, 7),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 40000L, 7),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 50000L, 7),
      };
    for (DataPoint expected: expected_data_points) {
      assertTrue(sgai.hasNext());
      DataPoint dp = sgai.next();
      assertEquals(expected.timestamp(), dp.timestamp());
      assertEquals(expected.doubleValue(), dp.doubleValue(), 0);
    }
    assertFalse(sgai.hasNext());
  }

  @Test
  public void testDownsample_afterAggregation() {
    iterators = new SeekableView[] {
        new Downsampler(SeekableViewsForTest.fromArray(DATA_5SEC), 10000, AVG),
        new Downsampler(SeekableViewsForTest.fromArray(DATA_5SEC), 10000, AVG),
        new Downsampler(SeekableViewsForTest.fromArray(DATA_5SEC), 10000, AVG),
        new Downsampler(SeekableViewsForTest.fromArray(DATA_5SEC), 10000, AVG),
        new Downsampler(SeekableViewsForTest.fromArray(DATA_5SEC), 10000, AVG),
        new Downsampler(SeekableViewsForTest.fromArray(DATA_5SEC), 10000, AVG),
        new Downsampler(SeekableViewsForTest.fromArray(DATA_5SEC), 10000, AVG)
    };
    start_time_ms = BASE_TIME + 01000L;
    end_time_ms = BASE_TIME + 100000;
    AggregationIter sgai = AggregationIter.createForTesting(iterators,
        start_time_ms, end_time_ms, aggregator, interpolation,
        rate);
    Downsampler downsampler = new Downsampler(sgai, 15000, SUM);
    // Tests the case: downsamples by 10 seconds. Then, aggregates across spans.
    // Then, downsample by 15 seconds again.
    DataPoint[] expected_data_points = new DataPoint[] {
        // Aggregator output timestamp: BASE_TIME + 10000
        MutableDataPoint.ofDoubleValue(BASE_TIME + 00000L, 7),
        // Aggregator output timestamp: BASE_TIME + 20000
        MutableDataPoint.ofDoubleValue(BASE_TIME + 15000L, 7),
        // Aggregator output timestamps: BASE_TIME + 30000, BASE_TIME + 40000
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30000L, 14),
        // Aggregator output timestamp: BASE_TIME + 50000
        MutableDataPoint.ofDoubleValue(BASE_TIME + 45000L, 7)
      };
    for (DataPoint expected: expected_data_points) {
      assertTrue(downsampler.hasNext());
      DataPoint dp = downsampler.next();
      assertEquals(expected.timestamp(), dp.timestamp());
      assertEquals(String.format("timestamp = %d", dp.timestamp()),
                   expected.doubleValue(), dp.doubleValue(), 0);
    }
    assertFalse(downsampler.hasNext());
  }

  @Test
  public void testAggregate_seek() {
    SeekableView iterator = spy(SeekableViewsForTest.fromArray(DATA_POINTS_1));
    iterators = new SeekableView[] {
        iterator
    };
    AggregationIter sgai = AggregationIter.createForTesting(iterators,
        start_time_ms, end_time_ms, aggregator, interpolation,rate);
    // The seek method should be called just once at the beginning.
    verify(iterator).seek(start_time_ms);
    for (DataPoint expected: DATA_POINTS_1) {
      assertTrue(sgai.hasNext());
      DataPoint dp = sgai.next();
      assertEquals(expected.timestamp(), dp.timestamp());
      assertEquals(expected.longValue(), dp.longValue());
    }
    assertFalse(sgai.hasNext());
    // The seek method should be called just once at the beginning.
    verify(iterator).seek(start_time_ms);
  }

  @Test
  public void testAggregate_emptySpan() {
    // Empty span should be ignored and no exception should be thrown.
    final DataPoint[] empty_data_points = new DataPoint[] {
    };
    iterators = new SeekableView[] {
        SeekableViewsForTest.fromArray(empty_data_points),
        SeekableViewsForTest.fromArray(DATA_POINTS_1),
    };
    AggregationIter sgai = AggregationIter.createForTesting(iterators,
        BASE_TIME + 00000L, end_time_ms, aggregator, interpolation,
        rate);
    for (DataPoint expected: DATA_POINTS_1) {
      assertTrue(sgai.hasNext());
      DataPoint dp = sgai.next();
      assertEquals(expected.timestamp(), dp.timestamp());
      assertEquals(expected.longValue(), dp.longValue());
    }
    assertFalse(sgai.hasNext());
  }

  private SeekableView[] createSeekableViews(int num_views,
                                             final long start_time_ms,
                                             final long end_time_ms,
                                             final int num_points_per_span) {
    SeekableView[] views = new SeekableView[num_views];
    final long sample_period_ms = 5000;
    final long increment_ms =
        (end_time_ms - start_time_ms) / num_views;
    long current_time = start_time_ms;
    for (int i = 0; i < num_views; ++i) {
      final SeekableView view = SeekableViewsForTest.generator(
          current_time, sample_period_ms, num_points_per_span, true);
      views[i] = new Downsampler(view, (int)DateTime.parseDuration("10s"),
                                 Aggregators.AVG);
      current_time += increment_ms;
    }
    assertEquals(num_views, views.length);
    return views;
  }

  private void testMeasureAggregationLatency(int num_views, double max_secs) {
    // Microbenchmark to measure the performance of AggregationIter.
    iterators = createSeekableViews(num_views, 1356990000000L, 1356993600000L,
                                    100);
    AggregationIter sgai = AggregationIter.createForTesting(iterators,
        1356990000000L, 1356993600000L, aggregator, interpolation,
        rate);
    final long start_time_nano = System.nanoTime();
    long total_data_points = 0;
    long timestamp_checksum = 0;
    double value_checksum = 0;
    while (sgai.hasNext()) {
      DataPoint dp = sgai.next();
      ++total_data_points;
      timestamp_checksum += dp.timestamp();
      value_checksum += dp.doubleValue();
    }
    final long finish_time_nano = System.nanoTime();
    final double elapsed = (finish_time_nano - start_time_nano) / 1000000000.0;
    System.out.println(String.format("%f seconds, %d data points, (%d, %g)" +
                                     "for %d views",
                                     elapsed, total_data_points,
                                     timestamp_checksum, value_checksum,
                                     num_views));
    assertTrue("Too slow, " + elapsed + " > " + max_secs,
        elapsed <= max_secs);
  }

  @Test
  public void testAggregate10000Spans() {
    testMeasureAggregationLatency(10000, 10.0);
  }

  @Test
  public void testAggregate250000Spans() {
    testMeasureAggregationLatency(250000, 10.0);
  }
}
