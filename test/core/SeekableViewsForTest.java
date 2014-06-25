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

import java.util.NoSuchElementException;
import org.junit.Test;

/** Helper class to mock SeekableView. */
public class SeekableViewsForTest {

  /**
   * Creates a {@link SeekableView} object to iterate the given data points.
   * @param data_points Test data.
   * @return A {@link SeekableView} object
   */
  public static SeekableView fromArray(final DataPoint[] data_points) {
    return new MockSeekableView(data_points);
  }

  /**
   * Creates a {@link SeekableView} that generates a sequence of data points.
   * @param start_time Starting timestamp
   * @param sample_period Average sample period of data points
   * @param num_data_points Total number of data points to generate
   * @param is_integer True to generate a sequence of integer data points.
   * @return A {@link SeekableView} object
   */
  public static SeekableView generator(final long start_time,
                                       final long sample_period,
                                       final int num_data_points,
                                       final boolean is_integer) {
    return new DataPointGenerator(start_time, sample_period, num_data_points,
                                  is_integer);
  }

  /** Iterates an array of data points. */
  private static class MockSeekableView implements SeekableView {

    private final DataPoint[] data_points;
    private int index = 0;

    MockSeekableView(final DataPoint[] data_points) {
      this.data_points = data_points;
    }

    @Override
    public boolean hasNext() {
      return data_points.length > index;
    }

    @Override
    public DataPoint next() {
      if (hasNext()) {
        return data_points[index++];
      }
      throw new NoSuchElementException("no more values");
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long timestamp) {
      for (index = 0; index < data_points.length; ++index) {
        if (data_points[index].timestamp() >= timestamp) {
          break;
        }
      }
    }
  }

  /** Generates a sequence of data points. */
  private static class DataPointGenerator implements SeekableView {

    private final long start_time_ms;
    private final long sample_period_ms;
    private final int num_data_points;
    private final boolean is_integer;
    private final MutableDataPoint current_data = new MutableDataPoint();
    private int current = 0;

    DataPointGenerator(final long start_time_ms, final long sample_period_ms,
                       final int num_data_points, final boolean is_integer) {
      this.start_time_ms = start_time_ms;
      this.sample_period_ms = sample_period_ms;
      this.num_data_points = num_data_points;
      this.is_integer = is_integer;
      rewind();
    }

    @Override
    public boolean hasNext() {
      return current < num_data_points;
    }

    @Override
    public DataPoint next() {
      if (hasNext()) {
        generateData();
        ++current;
        return current_data;
      }
      throw new NoSuchElementException("no more values");
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long timestamp) {
      rewind();
      current = (int)((timestamp -1 - start_time_ms) / sample_period_ms);
      if (current < 0) {
        current = 0;
      }
      while (generateTimestamp() < timestamp) {
        ++current;
      }
    }

    private void rewind() {
      current = 0;
      generateData();
    }

    private void generateData() {
      if (is_integer) {
        current_data.reset(generateTimestamp(), current);
      } else {
        current_data.reset(generateTimestamp(), current);
      }
    }

    private long generateTimestamp() {
      long timestamp = start_time_ms + sample_period_ms * current;
      return timestamp + (((current % 2) == 0) ? -1000 : 1000);
    }
  }

  @Test
  public void testDataPointGenerator() {
    DataPointGenerator dpg = new DataPointGenerator(100000, 10000, 5, true);
    DataPoint[] expected_data_points = new DataPoint[] {
        MutableDataPoint.ofLongValue(99000, 0),
        MutableDataPoint.ofLongValue(111000, 1),
        MutableDataPoint.ofLongValue(119000, 2),
        MutableDataPoint.ofLongValue(131000, 3),
        MutableDataPoint.ofLongValue(139000, 4),
    };
    for (DataPoint expected: expected_data_points) {
      assertTrue(dpg.hasNext());
      DataPoint dp = dpg.next();
      assertEquals(expected.timestamp(), dp.timestamp());
      assertEquals(expected.longValue(), dp.longValue());
    }
    assertFalse(dpg.hasNext());
  }

  @Test
  public void testDataPointGenerator_double() {
    DataPointGenerator dpg = new DataPointGenerator(100000, 10000, 5, false);
    DataPoint[] expected_data_points = new DataPoint[] {
        MutableDataPoint.ofDoubleValue(99000, 0),
        MutableDataPoint.ofDoubleValue(111000, 1),
        MutableDataPoint.ofDoubleValue(119000, 2),
        MutableDataPoint.ofDoubleValue(131000, 3),
        MutableDataPoint.ofDoubleValue(139000, 4),
    };
    for (DataPoint expected: expected_data_points) {
      assertTrue(dpg.hasNext());
      DataPoint dp = dpg.next();
      assertEquals(expected.timestamp(), dp.timestamp());
      assertEquals(expected.doubleValue(), dp.doubleValue(), 0);
    }
    assertFalse(dpg.hasNext());
  }

  @Test
  public void testDataPointGenerator_seek() {
    DataPointGenerator dpg = new DataPointGenerator(100000, 10000, 5, true);
    dpg.seek(119000);
    DataPoint[] expected_data_points = new DataPoint[] {
        MutableDataPoint.ofLongValue(119000, 2),
        MutableDataPoint.ofLongValue(131000, 3),
        MutableDataPoint.ofLongValue(139000, 4),
    };
    for (DataPoint expected: expected_data_points) {
      assertTrue(dpg.hasNext());
      DataPoint dp = dpg.next();
      assertEquals(expected.timestamp(), dp.timestamp());
      assertEquals(expected.longValue(), dp.longValue());
    }
    assertFalse(dpg.hasNext());
  }

  @Test
  public void testDataPointGenerator_seekToFirst() {
    DataPointGenerator dpg = new DataPointGenerator(100000, 10000, 5, true);
    dpg.seek(100000);
    DataPoint[] expected_data_points = new DataPoint[] {
        MutableDataPoint.ofLongValue(111000, 1),
        MutableDataPoint.ofLongValue(119000, 2),
        MutableDataPoint.ofLongValue(131000, 3),
        MutableDataPoint.ofLongValue(139000, 4),
    };
    for (DataPoint expected: expected_data_points) {
      assertTrue(dpg.hasNext());
      DataPoint dp = dpg.next();
      assertEquals(expected.timestamp(), dp.timestamp());
      assertEquals(expected.longValue(), dp.longValue());
    }
    assertFalse(dpg.hasNext());
  }

  @Test
  public void testDataPointGenerator_seekToSecond() {
    DataPointGenerator dpg = new DataPointGenerator(100000, 10000, 5, true);
    dpg.seek(100001);
    DataPoint[] expected_data_points = new DataPoint[] {
        MutableDataPoint.ofLongValue(111000, 1),
        MutableDataPoint.ofLongValue(119000, 2),
        MutableDataPoint.ofLongValue(131000, 3),
        MutableDataPoint.ofLongValue(139000, 4),
    };
    for (DataPoint expected: expected_data_points) {
      assertTrue(dpg.hasNext());
      DataPoint dp = dpg.next();
      assertEquals(expected.timestamp(), dp.timestamp());
      assertEquals(expected.longValue(), dp.longValue());
    }
    assertFalse(dpg.hasNext());
  }
}
