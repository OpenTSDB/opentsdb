// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests {@link FillingDownsampler}. */
public class TestFillingDownsampler {
  private static final Aggregator SUM = Aggregators.get("sum");

  private static final FillPolicy NAN = FillPolicy.fromString("nan");
  private static final FillPolicy ZERO = FillPolicy.fromString("zero");

  /** Data with gaps: before, during, and after. */
  @Test
  public void testNaNMissingInterval() {
    final long baseTime = 500L;
    final SeekableView source =
      SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  4L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  5L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  7L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L * 12L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L * 15L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L * 24L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L * 25L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L * 26L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L * 27L, 1.),
      });

    final Downsampler downsampler = new FillingDownsampler(source, baseTime,
      baseTime + 36 * 25L, 100L, SUM, NAN);

    step(downsampler, Double.NaN);
    step(downsampler, 3.);
    step(downsampler, Double.NaN);
    step(downsampler, 2.);
    step(downsampler, Double.NaN);
    step(downsampler, Double.NaN);
    step(downsampler, 4.);
    step(downsampler, Double.NaN);
    step(downsampler, Double.NaN);
    assertFalse(downsampler.hasNext());
  }

  @Test
  public void testZeroMissingInterval() {
    final long baseTime = 500L;
    final SeekableView source =
      SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  4L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  5L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  7L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L * 12L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L * 15L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L * 24L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L * 25L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L * 26L, 1.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L * 27L, 1.),
      });

    final Downsampler downsampler = new FillingDownsampler(source, baseTime,
      baseTime + 36 * 25L, 100L, SUM, ZERO);

    step(downsampler, 0.);
    step(downsampler, 3.);
    step(downsampler, 0.);
    step(downsampler, 2.);
    step(downsampler, 0.);
    step(downsampler, 0.);
    step(downsampler, 4.);
    step(downsampler, 0.);
    step(downsampler, 0.);
    assertFalse(downsampler.hasNext());
  }

  /** Contiguous data, i.e., nothing missing. */
  @Test
  public void testWithoutMissingIntervals() {
    final long baseTime = 1000L;
    final SeekableView source =
      SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  0L, 12.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  1L, 11.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  2L, 10.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  3L,  9.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  4L,  8.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  5L,  7.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  6L,  6.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  7L,  5.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  8L,  4.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L *  9L,  3.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L * 10L,  2.),
        MutableDataPoint.ofDoubleValue(baseTime + 25L * 11L,  1.),
      });

    final Downsampler downsampler = new FillingDownsampler(source, baseTime,
      baseTime + 12L * 25L, 100L, SUM, NAN);

    step(downsampler, 42.);
    step(downsampler, 26.);
    step(downsampler, 10.);
    assertFalse(downsampler.hasNext());
  }

  /** Data up to five minutes out of query time bounds. */
  @Test
  public void testWithOutOfBoundsData() {
    final long baseTime = 1425335895000L;
    final SeekableView source =
      SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(baseTime - 60000L * 5L +   320L, 53.),
        MutableDataPoint.ofDoubleValue(baseTime - 60000L * 2L +  8839L, 16.),

        // start query
        MutableDataPoint.ofDoubleValue(baseTime + 60000L * 0L +   849L,  9.),
        MutableDataPoint.ofDoubleValue(baseTime + 60000L * 0L +  3849L,  8.),
        MutableDataPoint.ofDoubleValue(baseTime + 60000L * 0L +  6210L,  7.),
        MutableDataPoint.ofDoubleValue(baseTime + 60000L * 0L + 42216L,  6.),
        MutableDataPoint.ofDoubleValue(baseTime + 60000L * 1L +   167L,  5.),
        MutableDataPoint.ofDoubleValue(baseTime + 60000L * 1L + 28593L,  4.),
        // end query

        MutableDataPoint.ofDoubleValue(baseTime + 60000L * 2L + 30384L, 37.),
        MutableDataPoint.ofDoubleValue(baseTime + 60000L * 4L +  1530L, 86.)
      });

    final Downsampler downsampler = new FillingDownsampler(source, baseTime,
      baseTime + 60000L * 2L, 60000L, SUM, NAN);

    step(downsampler, 30.);
    step(downsampler,  9.);
    assertFalse(downsampler.hasNext());
  }

  private void step(final Downsampler downsampler, final double expected) {
    assertTrue(downsampler.hasNext());
    final DataPoint point = downsampler.next();
    assertNotNull(point);
    assertEquals(expected, point.doubleValue(), 0.01);
  }
}

