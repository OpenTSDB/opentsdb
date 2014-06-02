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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.List;

import com.google.common.collect.Lists;

import net.opentsdb.utils.DateTime;

import org.junit.Before;
import org.junit.Test;


/** Tests {@link Downsampler}. */
public class TestDownsampler {

  private static final long BASE_TIME = 1356998400000L;
  private static final DataPoint[] DATA_POINTS = new DataPoint[] {
    MutableDataPoint.ofLongValue(BASE_TIME, 40),
    MutableDataPoint.ofLongValue(BASE_TIME + 2000000, 50),
    MutableDataPoint.ofLongValue(BASE_TIME + 3600000, 40),
    MutableDataPoint.ofLongValue(BASE_TIME + 3605000, 50),
    MutableDataPoint.ofLongValue(BASE_TIME + 7200000, 40),
    MutableDataPoint.ofLongValue(BASE_TIME + 9200000, 50)
  };
  private static final int THOUSAND_SEC_INTERVAL =
      (int)DateTime.parseDuration("1000s");
  private static final int TEN_SEC_INTERVAL =
      (int)DateTime.parseDuration("10s");
  private static final Aggregator AVG = Aggregators.get("avg");
  private static final Aggregator SUM = Aggregators.get("sum");

  private SeekableView source;
  private Downsampler downsampler;

  @Before
  public void setup() {
    source = spy(SeekableViewsForTest.fromArray(DATA_POINTS));
  }

  @Test
  public void testDownsampler() {
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
    assertEquals(40, values.get(0).longValue());
    assertEquals(BASE_TIME - 400000L, timestamps_in_millis.get(0).longValue());
    assertEquals(50, values.get(1).longValue());
    assertEquals(BASE_TIME + 1600000, timestamps_in_millis.get(1).longValue());
    assertEquals(45, values.get(2).longValue());
    assertEquals(BASE_TIME + 3600000L, timestamps_in_millis.get(2).longValue());
    assertEquals(40, values.get(3).longValue());
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(3).longValue());
    assertEquals(50, values.get(4).longValue());
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(4).longValue());
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
    assertEquals(3, values.get(0).longValue());
    assertEquals(BASE_TIME + 00000L, timestamps_in_millis.get(0).longValue());
    assertEquals(12, values.get(1).longValue());
    assertEquals(BASE_TIME + 10000L, timestamps_in_millis.get(1).longValue());
    assertEquals(48, values.get(2).longValue());
    assertEquals(BASE_TIME + 20000L, timestamps_in_millis.get(2).longValue());
    assertEquals(192, values.get(3).longValue());
    assertEquals(BASE_TIME + 30000L, timestamps_in_millis.get(3).longValue());
    assertEquals(768, values.get(4).longValue());
    assertEquals(BASE_TIME + 40000L, timestamps_in_millis.get(4).longValue());
    assertEquals(1024, values.get(5).longValue());
    assertEquals(BASE_TIME + 50000L, timestamps_in_millis.get(5).longValue());
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
    assertEquals(1, values.get(0).longValue());
    assertEquals(BASE_TIME + 00000L, timestamps_in_millis.get(0).longValue());
    assertEquals(6, values.get(1).longValue());
    assertEquals(BASE_TIME + 15000L, timestamps_in_millis.get(1).longValue());
    assertEquals(8, values.get(2).longValue());
    assertEquals(BASE_TIME + 30000L, timestamps_in_millis.get(2).longValue());
    assertEquals(48, values.get(3).longValue());
    assertEquals(BASE_TIME + 45000L, timestamps_in_millis.get(3).longValue());
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
    assertEquals(45, values.get(0).longValue());
    assertEquals(BASE_TIME + 3600000L, timestamps_in_millis.get(0).longValue());
    assertEquals(40, values.get(1).longValue());
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(1).longValue());
    assertEquals(50, values.get(2).longValue());
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(2).longValue());
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
    assertEquals(45, values.get(0).longValue());
    assertEquals(BASE_TIME + 3600000L, timestamps_in_millis.get(0).longValue());
    assertEquals(40, values.get(1).longValue());
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(1).longValue());
    assertEquals(50, values.get(2).longValue());
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
    assertEquals("seek(1356998400000)", 400, first_dp.doubleValue(), 0);
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
                   40, dp.doubleValue(), 0);
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
