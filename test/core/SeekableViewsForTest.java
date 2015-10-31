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
   * Creates a {@link SeekableView} that generates a sequence of data points
   * where the starting value is 1 and it is incremented by 1 each iteration.
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
    return generator(start_time, sample_period, num_data_points,
                                  is_integer, 0, 1);
  }
  
  /**
   * Creates a {@link SeekableView} that generates a sequence of data points.
   * @param start_time Starting timestamp
   * @param sample_period Average sample period of data points
   * @param num_data_points Total number of data points to generate
   * @param is_integer True to generate a sequence of integer data points.
   * @param starting_value The starting data point value.
   * @param increment How much to increment the values each iteration.
   * @return A {@link SeekableView} object
   */
  public static SeekableView generator(final long start_time,
                                       final long sample_period,
                                       final int num_data_points,
                                       final boolean is_integer,
                                       final double starting_value,
                                       final double increment) {
    return new DataPointGenerator(start_time, sample_period, num_data_points,
     is_integer, starting_value, increment);
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

    private final long sample_period_ms;
    private final int num_data_points;
    private final boolean is_integer;
    private final double increment;
    private final MutableDataPoint current_data = new MutableDataPoint();
    private final MutableDataPoint next_data = new MutableDataPoint();
    private int dps_emitted = 0;
    
    DataPointGenerator(final long start_time_ms, final long sample_period_ms,
        final int num_data_points, final boolean is_integer, 
        final double starting_value, final double increment) {
      this.sample_period_ms = sample_period_ms;
      this.num_data_points = num_data_points;
      this.is_integer = is_integer;
      this.increment = increment;
      if (is_integer) {
        next_data.reset(start_time_ms, (long)starting_value);
      } else {
        next_data.reset(start_time_ms, starting_value);
      }
    }

    @Override
    public boolean hasNext() {
      return dps_emitted < num_data_points;
    }

    @Override
    public DataPoint next() {
      if (hasNext()) {
        current_data.reset(next_data);
        advance();
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
      while (next_data.timestamp() < timestamp && dps_emitted < num_data_points) {
        advance();
      }
    }
    
    private void advance() {
      if (is_integer) {
        next_data.reset(next_data.timestamp() + sample_period_ms, 
            next_data.longValue() + (long)increment);
      } else {
        next_data.reset(next_data.timestamp() + sample_period_ms, 
            next_data.doubleValue() + increment);
      }
      dps_emitted++;
    }
  }

  @Test
  public void testDataPointGenerator() {
    SeekableView dpg = generator(100000, 10000, 5, true);
    DataPoint[] expected_data_points = new DataPoint[] {
        MutableDataPoint.ofLongValue(100000, 0),
        MutableDataPoint.ofLongValue(110000, 1),
        MutableDataPoint.ofLongValue(120000, 2),
        MutableDataPoint.ofLongValue(130000, 3),
        MutableDataPoint.ofLongValue(140000, 4),
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
    SeekableView dpg = generator(100000, 10000, 5, false);
    DataPoint[] expected_data_points = new DataPoint[] {
        MutableDataPoint.ofDoubleValue(100000, 0),
        MutableDataPoint.ofDoubleValue(110000, 1),
        MutableDataPoint.ofDoubleValue(120000, 2),
        MutableDataPoint.ofDoubleValue(130000, 3),
        MutableDataPoint.ofDoubleValue(140000, 4),
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
    SeekableView dpg = generator(100000, 10000, 5, true);
    dpg.seek(119000);
    DataPoint[] expected_data_points = new DataPoint[] {
        MutableDataPoint.ofLongValue(120000, 2),
        MutableDataPoint.ofLongValue(130000, 3),
        MutableDataPoint.ofLongValue(140000, 4),
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
    SeekableView dpg = generator(100000, 10000, 5, true);
    dpg.seek(100000);
    DataPoint[] expected_data_points = new DataPoint[] {
        MutableDataPoint.ofLongValue(100000, 0),
        MutableDataPoint.ofLongValue(110000, 1),
        MutableDataPoint.ofLongValue(120000, 2),
        MutableDataPoint.ofLongValue(130000, 3),
        MutableDataPoint.ofLongValue(140000, 4),
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
    SeekableView dpg = generator(100000, 10000, 5, true);
    dpg.seek(100001);
    DataPoint[] expected_data_points = new DataPoint[] {
        MutableDataPoint.ofLongValue(110000, 1),
        MutableDataPoint.ofLongValue(120000, 2),
        MutableDataPoint.ofLongValue(130000, 3),
        MutableDataPoint.ofLongValue(140000, 4),
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
