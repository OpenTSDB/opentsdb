// This file is part of OpenTSDB.
// Copyright (C) 2016-2017  The OpenTSDB Authors.
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

import org.hbase.async.Bytes;
import org.junit.Ignore;
import org.junit.Test;

/** Helper class to mock HistogramSeekableView. */
@Ignore
public class HistogramSeekableViewForTest {

  /**
   * Creates a {@link HistogramSeekableView} object to iterate the given data 
   * points.
   * @param data_points Test data.
   * @return A {@link HistogramSeekableView} object
   */
  public static HistogramSeekableView fromArray(
      final HistogramDataPoint[] data_points) {
    return new MockHistogramSeekableView(data_points);
  }

  /**
   * Creates a {@link HistogramSeekableView} that generates a sequence of data 
   * points.
   * @param start_time Starting timestamp
   * @param sample_period Average sample period of data points
   * @param num_data_points Total number of data points to generate
   * @return A {@link HistogramSeekableView} object
   */
  public static HistogramSeekableView generator(final long start_time,
                                       final long sample_period,
                                       final int num_data_points) {
    return new DataPointGenerator(start_time, sample_period, num_data_points);
  }

  /** Iterates an array of data points. */
  public static class MockHistogramSeekableView implements HistogramSeekableView {

    private final HistogramDataPoint[] data_points;
    private int index = 0;

    MockHistogramSeekableView(final HistogramDataPoint[] data_points2) {
      this.data_points = data_points2;
    }

    @Override
    public boolean hasNext() {
      return data_points.length > index;
    }

    @Override
    public HistogramDataPoint next() {
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
  
    public void resetIndex() {
      index = 0;
    }
  }

  /** Generates a sequence of data points. */
  private static class DataPointGenerator implements HistogramSeekableView {

    private final long start_time_ms;
    private final long sample_period_ms;
    private final int num_data_points;
    private final LongHistogramDataPointForTest current_data = 
        new LongHistogramDataPointForTest(100L, Bytes.fromLong(0L));
    private int current = 0;

    DataPointGenerator(final long start_time_ms, final long sample_period_ms,
                       final int num_data_points) {
      this.start_time_ms = start_time_ms;
      this.sample_period_ms = sample_period_ms;
      this.num_data_points = num_data_points;
      rewind();
    }

    @Override
    public boolean hasNext() {
      return current < num_data_points;
    }

    @Override
    public HistogramDataPoint next() {
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
      current_data.setTimeStamp(generateTimestamp());
      current_data.setRawData(Bytes.fromLong(current));
    }

    private long generateTimestamp() {
      long timestamp = start_time_ms + sample_period_ms * current;
      return timestamp + (((current % 2) == 0) ? -1000 : 1000);
    }
  }

  @Test
  public void testDataPointGenerator() {
    DataPointGenerator hdpg = new DataPointGenerator(100000, 10000, 5);
    HistogramDataPoint[] expected_data_points = new HistogramDataPoint[] {
        new LongHistogramDataPointForTest(99000, Bytes.fromLong(0L)),
        new LongHistogramDataPointForTest(111000, Bytes.fromLong(1L)),
        new LongHistogramDataPointForTest(119000, Bytes.fromLong(2L)),
        new LongHistogramDataPointForTest(131000, Bytes.fromLong(3L)),
        new LongHistogramDataPointForTest(139000, Bytes.fromLong(4L)),
    };
    for (HistogramDataPoint expected: expected_data_points) {
      assertTrue(hdpg.hasNext());
      HistogramDataPoint dp = hdpg.next();
      assertEquals(expected.timestamp(), dp.timestamp());
      assertEquals(Bytes.getLong(expected.getRawData()), 
          Bytes.getLong(dp.getRawData()));
    }
    assertFalse(hdpg.hasNext());
  }

  @Test
  public void testDataPointGenerator_seek() {
    DataPointGenerator hdpg = new DataPointGenerator(100000, 10000, 5);
    hdpg.seek(119000);
    HistogramDataPoint[] expected_data_points = new HistogramDataPoint[] {
        new LongHistogramDataPointForTest(119000, Bytes.fromLong(2L)),
        new LongHistogramDataPointForTest(131000, Bytes.fromLong(3L)),
        new LongHistogramDataPointForTest(139000, Bytes.fromLong(4L)),
    };
    for (HistogramDataPoint expected: expected_data_points) {
      assertTrue(hdpg.hasNext());
      HistogramDataPoint hdp = hdpg.next();
      assertEquals(expected.timestamp(), hdp.timestamp());
      assertEquals(Bytes.getLong(expected.getRawData()), 
          Bytes.getLong(hdp.getRawData()));
    }
    assertFalse(hdpg.hasNext());
  }

  @Test
  public void testDataPointGenerator_seekToFirst() {
    DataPointGenerator hdpg = new DataPointGenerator(100000, 10000, 5);
    hdpg.seek(100000);
    HistogramDataPoint[] expected_data_points = new HistogramDataPoint[] {
        new LongHistogramDataPointForTest(111000, Bytes.fromLong(1L)),
        new LongHistogramDataPointForTest(119000, Bytes.fromLong(2L)),
        new LongHistogramDataPointForTest(131000, Bytes.fromLong(3L)),
        new LongHistogramDataPointForTest(139000, Bytes.fromLong(4L)),
    };
    for (HistogramDataPoint expected: expected_data_points) {
      assertTrue(hdpg.hasNext());
      HistogramDataPoint hdp = hdpg.next();
      assertEquals(expected.timestamp(), hdp.timestamp());
      assertEquals(Bytes.getLong(expected.getRawData()), 
          Bytes.getLong(hdp.getRawData()));
    }
    assertFalse(hdpg.hasNext());
  }

  @Test
  public void testDataPointGenerator_seekToSecond() {
    DataPointGenerator hdpg = new DataPointGenerator(100000, 10000, 5);
    hdpg.seek(100001);
    HistogramDataPoint[] expected_data_points = new HistogramDataPoint[] {
        new LongHistogramDataPointForTest(111000, Bytes.fromLong(1)),
        new LongHistogramDataPointForTest(119000, Bytes.fromLong(2L)),
        new LongHistogramDataPointForTest(131000, Bytes.fromLong(3L)),
        new LongHistogramDataPointForTest(139000, Bytes.fromLong(4L)),
    };
    for (HistogramDataPoint expected: expected_data_points) {
      assertTrue(hdpg.hasNext());
      HistogramDataPoint hdp = hdpg.next();
      assertEquals(expected.timestamp(), hdp.timestamp());
      assertEquals(Bytes.getLong(expected.getRawData()), 
          Bytes.getLong(hdp.getRawData()));
    }
    assertFalse(hdpg.hasNext());
  }
}
