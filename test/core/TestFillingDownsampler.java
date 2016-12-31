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

import net.opentsdb.core.SeekableViewsForTest.MockSeekableView;
import net.opentsdb.utils.DateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

import java.util.TimeZone;

/** Tests {@link FillingDownsampler}. */
public class TestFillingDownsampler {
  private static final long BASE_TIME = 1356998400000L;
  //30 minute offset
  final static TimeZone AF = DateTime.timezones.get("Asia/Kabul");
  // 12h offset w/o DST
  final static TimeZone TV = DateTime.timezones.get("Pacific/Funafuti");
  // 12h offset w DST
  final static TimeZone FJ = DateTime.timezones.get("Pacific/Fiji");
  // Tue, 15 Dec 2015 04:02:25.123 UTC
  final static long DST_TS = 1450137600000L;
 
  private SeekableView source;
  private Downsampler downsampler;
  private DownsamplingSpecification specification;
  
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

    specification = new DownsamplingSpecification("100ms-sum-nan");
    final Downsampler downsampler = new FillingDownsampler(source, baseTime,
      baseTime + 36 * 25L, specification, 0, 0);
    
    long timestamp = baseTime;
    step(downsampler, timestamp, Double.NaN);
    step(downsampler, timestamp += 100, 3.);
    step(downsampler, timestamp += 100, Double.NaN);
    step(downsampler, timestamp += 100, 2.);
    step(downsampler, timestamp += 100, Double.NaN);
    step(downsampler, timestamp += 100, Double.NaN);
    step(downsampler, timestamp += 100, 4.);
    step(downsampler, timestamp += 100, Double.NaN);
    step(downsampler, timestamp += 100, Double.NaN);
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
    
    specification = new DownsamplingSpecification("100ms-sum-zero");
    final Downsampler downsampler = new FillingDownsampler(source, baseTime,
      baseTime + 36 * 25L, specification, 0, 0);
    
    long timestamp = baseTime;
    step(downsampler, timestamp, 0.);
    step(downsampler, timestamp += 100, 3.);
    step(downsampler, timestamp += 100, 0.);
    step(downsampler, timestamp += 100, 2.);
    step(downsampler, timestamp += 100, 0.);
    step(downsampler, timestamp += 100, 0.);
    step(downsampler, timestamp += 100, 4.);
    step(downsampler, timestamp += 100, 0.);
    step(downsampler, timestamp += 100, 0.);
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

    specification = new DownsamplingSpecification("100ms-sum-nan");
    final Downsampler downsampler = new FillingDownsampler(source, baseTime,
      baseTime + 12L * 25L, specification, 0, 0);

    long timestamp = baseTime;
    step(downsampler, timestamp, 42.);
    step(downsampler, timestamp += 100, 26.);
    step(downsampler, timestamp += 100, 10.);
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
    
    specification = new DownsamplingSpecification("1m-sum-nan");
    final Downsampler downsampler = new FillingDownsampler(source, baseTime,
      baseTime + 60000L * 2L, specification, 0, 0);
    
    long timestamp = 1425335880000L;
    step(downsampler, timestamp, 30.);
    step(downsampler, timestamp += 60000, 9.);
    assertFalse(downsampler.hasNext());
  }
  
  @Test
  public void testWithOutOfBoundsDataEarly() {
    final long baseTime = 1425335895000L;
    final SeekableView source =
      SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(baseTime - 60000L * 5L +   320L, 53.),
        MutableDataPoint.ofDoubleValue(baseTime - 60000L * 2L +  8839L, 16.)
      });

    specification = new DownsamplingSpecification("1m-sum-nan");
    final Downsampler downsampler = new FillingDownsampler(source, baseTime,
      baseTime + 60000L * 2L, specification, 0, 0);
    
    long timestamp = 1425335880000L;
    step(downsampler, timestamp, Double.NaN);
    step(downsampler, timestamp += 60000, Double.NaN);
    assertFalse(downsampler.hasNext());
  }
  
  @Test
  public void testWithOutOfBoundsDataLate() {
    final long baseTime = 1425335895000L;
    final SeekableView source =
      SeekableViewsForTest.fromArray(new DataPoint[] {
          MutableDataPoint.ofDoubleValue(baseTime + 60000L * 2L + 30384L, 37.),
          MutableDataPoint.ofDoubleValue(baseTime + 60000L * 4L +  1530L, 86.)
      });

    specification = new DownsamplingSpecification("1m-sum-nan");
    final Downsampler downsampler = new FillingDownsampler(source, baseTime,
      baseTime + 60000L * 2L, specification, 0, 0);
    
    long timestamp = 1425335880000L;
    step(downsampler, timestamp, Double.NaN);
    step(downsampler, timestamp += 60000, Double.NaN);
    assertFalse(downsampler.hasNext());
  }

  @Test
  public void testDownsampler_allFullRange() {
    final SeekableView source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME + 5000L, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 15000L, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 25000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 35000L, 8),
        MutableDataPoint.ofLongValue(BASE_TIME + 45000L, 16),
        MutableDataPoint.ofLongValue(BASE_TIME + 55000L, 32)
    });
    
    specification = new DownsamplingSpecification("0all-sum-nan");
    final Downsampler downsampler = new FillingDownsampler(source, 
        BASE_TIME + 5000L,BASE_TIME + 55000L, specification, 0, 
        Long.MAX_VALUE);
    
    step(downsampler, 0, 63);
    assertFalse(downsampler.hasNext());
  }
  
  @Test
  public void testDownsampler_allFilterOnQuery() {
    final SeekableView source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME + 5000L, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 15000L, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 25000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 35000L, 8),
        MutableDataPoint.ofLongValue(BASE_TIME + 45000L, 16),
        MutableDataPoint.ofLongValue(BASE_TIME + 55000L, 32)
    });
    
    specification = new DownsamplingSpecification("0all-sum-nan");
    final Downsampler downsampler = new FillingDownsampler(source, 
        BASE_TIME + 5000L,BASE_TIME + 55000L, specification, 
        BASE_TIME + 15000L, BASE_TIME + 45000L);
    
    step(downsampler, BASE_TIME + 15000L, 14);
    assertFalse(downsampler.hasNext());
  }
  
  @Test
  public void testDownsampler_allFilterOnQueryOutOfRangeEarly() {
    final SeekableView source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME + 5000L, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 15000L, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 25000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 35000L, 8),
        MutableDataPoint.ofLongValue(BASE_TIME + 45000L, 16),
        MutableDataPoint.ofLongValue(BASE_TIME + 55000L, 32)
    });
    
    specification = new DownsamplingSpecification("0all-sum-nan");
    final Downsampler downsampler = new FillingDownsampler(source, 
        BASE_TIME + 5000L,BASE_TIME + 55000L, specification, 
        BASE_TIME + 65000L, BASE_TIME + 75000L);
    
    assertFalse(downsampler.hasNext());
  }
  
  @Test
  public void testDownsampler_allFilterOnQueryOutOfRangeLate() {
    final SeekableView source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME + 5000L, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 15000L, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 25000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 35000L, 8),
        MutableDataPoint.ofLongValue(BASE_TIME + 45000L, 16),
        MutableDataPoint.ofLongValue(BASE_TIME + 55000L, 32)
    });
    
    specification = new DownsamplingSpecification("0all-sum-nan");
    final Downsampler downsampler = new FillingDownsampler(source, 
        BASE_TIME + 5000L,BASE_TIME + 55000L, specification, 
        BASE_TIME - 15000L, BASE_TIME - 5000L);
    
    assertFalse(downsampler.hasNext());
  }
  
  @Test
  public void testDownsampler_calendarHour() {
    source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 1800000, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 3599000L, 3),
        MutableDataPoint.ofLongValue(BASE_TIME + 3600000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 5400000L, 5),
        MutableDataPoint.ofLongValue(BASE_TIME + 7199000L, 6)
    });
    specification = new DownsamplingSpecification("1hc-sum-nan");
    specification.setTimezone(TV);
    downsampler = new FillingDownsampler(source, 
        BASE_TIME, BASE_TIME + (3600000 * 3), specification, 0, Long.MAX_VALUE);

    long ts = BASE_TIME;
    double value = 6;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 3600000;
      if (value == 6) {
        value = 15;
      } else {
        value = Double.NaN;
      }
    }

    // hour offset by 30m
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1hc-sum-nan");
    specification.setTimezone(AF);
    downsampler = new FillingDownsampler(source, 1356996600000L, 
        1356996600000L + (3600000 * 4), specification, 0, Long.MAX_VALUE);

    ts = 1356996600000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 3600000;
      if (value == 1) {
        value = 9;
      } else if (value == 9) {
        value = 11;
      } else {
        value = Double.NaN;
      }
    }
    
    // multiple hours
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("4hc-sum-nan");
    specification.setTimezone(AF);
    downsampler = new FillingDownsampler(source, 1356996600000L, 
        1356996600000L + (3600000 * 8), specification, 0, Long.MAX_VALUE);

    ts = 1356996600000L;
    value = 21;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts = 1357011000000L;
      value = Double.NaN;
    }
  }
  
  @Test
  public void testDownsampler_calendarDay() {
    source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(DST_TS, 1),
        MutableDataPoint.ofLongValue(DST_TS + 86399000, 2),
        MutableDataPoint.ofLongValue(DST_TS + 126001000L, 3), // falls to the next in FJ
        MutableDataPoint.ofLongValue(DST_TS + 172799000L, 4),
        MutableDataPoint.ofLongValue(DST_TS + 172800000L, 5),
        MutableDataPoint.ofLongValue(DST_TS + 242999000L, 6) // falls within 30m offset
    });
    
    // control
    specification = new DownsamplingSpecification("1d-sum-nan");
    downsampler = new FillingDownsampler(source, DST_TS, 
        DST_TS + (86400000 * 4), specification, 0, Long.MAX_VALUE);

    long ts = DST_TS;
    double value = 3;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000;
      if (value == 3) {
        value = 7;
      } else if (value == 7) {
        value = 11;
      } else {
        value = Double.NaN;
      }
    }

    // 12 hour offset from UTC
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1dc-sum-nan");
    specification.setTimezone(TV);
    downsampler = new FillingDownsampler(source, 1450094400000L - 86400000, 
        DST_TS + (86400000 * 5), specification, 0, Long.MAX_VALUE);

    ts = 1450094400000L - 86400000; // make sure we front-fill too
    value = Double.NaN;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000;
      if (Double.isNaN(value)) {
        value = 1;
      } else if (value == 1) {
        value = 5;
      } else if (value == 5) {
        value = 9;
      } else if (value == 9) {
        value = 6;
      } else {
        value = Double.NaN;
      }
    }
    
    // 11 hour offset from UTC
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1dc-sum-nan");
    specification.setTimezone(FJ);
    downsampler = new FillingDownsampler(source, 1450094400000L, 
        DST_TS + (86400000 * 5), specification, 0, Long.MAX_VALUE);

    ts = 1450090800000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000;
      if (value == 1) {
        value = 2;
      } else if (value == 2) {
        value = 12;
      } else if (value == 12) {
        value = 6;
      } else {
        value = Double.NaN;
      }
    }
    
    // 30m offset
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1dc-sum-nan");
    specification.setTimezone(AF);
    downsampler = new FillingDownsampler(source, 1450121400000L, 
        DST_TS + (86400000 * 4), specification, 0, Long.MAX_VALUE);

    ts = 1450121400000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000;
      if (value == 1) {
        value = 5;
      } else if (value == 5) {
        value = 15;
      } else {
        value = Double.NaN;
      }
    }
    
    // multiple days
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("3dc-sum-nan");
    specification.setTimezone(AF);
    downsampler = new FillingDownsampler(source, 1450121400000L, 
        DST_TS + (86400000 * 6), specification, 0, Long.MAX_VALUE);

    ts = 1450121400000L;
    value = 21;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000L * 3;
      value = Double.NaN;
    }
  }
  
  @Test
  public void testDownsampler_calendarWeek() {
    source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(DST_TS, 1), // a Tuesday in UTC land
        MutableDataPoint.ofLongValue(DST_TS + (86400000L * 7), 2),
        MutableDataPoint.ofLongValue(1451129400000L, 3), // falls to the next in FJ
        MutableDataPoint.ofLongValue(DST_TS + (86400000L * 21), 4),
        MutableDataPoint.ofLongValue(1452367799000L, 5) // falls within 30m offset
    });
    // control
    specification = new DownsamplingSpecification("1wc-sum-nan");
    downsampler = new FillingDownsampler(source, 1449964800000L, 
        DST_TS + (86400000L * 35), specification, 0, Long.MAX_VALUE);
    
    long ts = 1449964800000L;
    double value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000L * 7;
      if (value == 1) {
        value = 5;
      } else if (value == 5) {
        value = Double.NaN;
      } else if (Double.isNaN(value)) {
        value = 9;
      } else {
        value = Double.NaN;
      }
    }
    
    // 12 hour offset from UTC
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1wc-sum-nan");
    specification.setTimezone(TV);
    downsampler = new FillingDownsampler(source, 1449964800000L, 
        DST_TS + (86400000L * 35), specification, 0, Long.MAX_VALUE);

    ts = 1449921600000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000L * 7;
      if (value == 1) {
        value = 5;
      } else if (value == 5) {
        value = Double.NaN;
      } else if (Double.isNaN(value)) {
        value = 4;
      } else {
        value = 5;
      }
    }
    
    // 11 hour offset from UTC
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1wc-sum-nan");
    specification.setTimezone(FJ);
    downsampler = new FillingDownsampler(source, 1449964800000L, 
        DST_TS + (86400000L * 35), specification, 0, Long.MAX_VALUE);

    ts = 1449918000000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000L * 7;
      value++;
    }
    
    // 30m offset
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1wc-sum-nan");
    specification.setTimezone(AF);
    downsampler = new FillingDownsampler(source, 1449964800000L, 
        DST_TS + (86400000L * 35), specification, 0, Long.MAX_VALUE);

    ts = 1449948600000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000L * 7;
      if (value == 1) {
        value = 5;
      } else if (value == 5) {
        value = Double.NaN;
      } else if (Double.isNaN(value)) {
        value = 9;
      } else {
        value = Double.NaN;
      }
    }
    
    // multiple weeks
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("2wc-sum-nan");
    specification.setTimezone(AF);
    downsampler = new FillingDownsampler(source, 1449964800000L, 
        DST_TS + (86400000L * 35), specification, 0, Long.MAX_VALUE);

    ts = 1449948600000L;
    value = 6;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 86400000L * 14;
      if (value == 6) {
        value = 9;
      } else {
        value = Double.NaN;
      }
    }
  }
  
  @Test
  public void testDownsampler_calendarMonth() {
    final long dec_1st = 1448928000000L;
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(dec_1st, 1),
        MutableDataPoint.ofLongValue(1451559600000L, 2), // falls to the next in FJ
        MutableDataPoint.ofLongValue(1451606400000L, 3), // jan 1st
        MutableDataPoint.ofLongValue(1454284800000L, 4), // feb 1st
        MutableDataPoint.ofLongValue(1456704000000L, 5), // feb 29th (leap year)
        MutableDataPoint.ofLongValue(1456772400000L, 6)  // falls within 30m offset AF
    }));
    
    // control
    specification = new DownsamplingSpecification("1n-sum-nan");
    downsampler = new FillingDownsampler(source, dec_1st, 
        dec_1st + (2592000000L * 5), specification, 0, Long.MAX_VALUE);

    long ts = dec_1st;
    double value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 2592000000L;
      if (value == 1) {
        value = 5;
      } else if (value == 5) {
        value = 4;
      } else if (value == 4) {
        value = 11;
      } else {
        value = Double.NaN;
      }
    }
    
    // 12h offset
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1nc-sum-nan");
    specification.setTimezone(TV);
    downsampler = new FillingDownsampler(source, dec_1st, 
        dec_1st + (2592000000L * 6), specification, 0, Long.MAX_VALUE);

    ts = 1448884800000L;
    value = 3;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      if (ts == 1448884800000L) {
        ts = 1451563200000L;
      } else if (ts == 1451563200000L) {
        ts = 1454241600000L;
        value = 9;
      } else if (ts == 1454241600000L) {
        ts = 1456747200000L;
        value = 6;
      } else {
        ts = 1459425600000L;
        value = Double.NaN;
      }
    }
    
    // 11h offset
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1nc-sum-nan");
    specification.setTimezone(FJ);
    downsampler = new FillingDownsampler(source, dec_1st, 
        dec_1st + (2592000000L * 6), specification, 0, Long.MAX_VALUE);

    ts = 1448881200000L;
    value = 1;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      if (ts == 1448881200000L) {
        ts = 1451559600000L;
        value = 5;
      } else if (ts == 1451559600000L) {
        ts = 1454241600000L;
        value = 9;
      } else if (ts == 1454241600000L) {
        ts = 1456747200000L;
        value = 6;
      } else {
        ts = 1459425600000L;
        value = Double.NaN;
      }
    }
    
    // 30m offset
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("1nc-sum-nan");
    specification.setTimezone(AF);
    downsampler = new FillingDownsampler(source, dec_1st, 
        dec_1st + (2592000000L * 5), specification, 0, Long.MAX_VALUE);

    ts = 1448911800000L;
    value = 3;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      if (ts == 1448911800000L) {
        ts = 1451590200000L;
      } else if (ts == 1451590200000L) {
        ts = 1454268600000L;
        value = 15;
      } else {
        ts = 1456774200000L;
        value = Double.NaN;
      }
    }
    
    // multiple months
    ((MockSeekableView)source).resetIndex();
    specification = new DownsamplingSpecification("3nc-sum-nan");
    specification.setTimezone(TV);
    downsampler = new FillingDownsampler(source, dec_1st, 
        dec_1st + (2592000000L * 9), specification, 0, Long.MAX_VALUE);

    ts = 1443614400000L;
    value = 3;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      if (ts == 1443614400000L) {
        ts = 1451563200000L;
        value = 18;
      } else {
        ts = 1459425600000L;
        value = Double.NaN;
      }
    }
  }
  
  @Test
  public void testDownsampler_calendarSkipSomePoints() {
    source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 1800000, 2),
        // skip an hour
        MutableDataPoint.ofLongValue(BASE_TIME + 7200000, 6)
    });
    specification = new DownsamplingSpecification("1hc-sum-nan");
    specification.setTimezone(TV);
    downsampler = new FillingDownsampler(source, 1356998400000L, 1357009200000L, 
        specification, 0, Long.MAX_VALUE);

    long ts = BASE_TIME;
    double value = 3;
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.doubleValue(), 0.001);
      ts += 3600000;
      if (value == 3) {
        value = Double.NaN;
      } else {
        value = 6;
      }
    }
  }
  
  @Test
  public void testDownsampler_noData() {
    final SeekableView source = 
        SeekableViewsForTest.fromArray(new DataPoint[] { });
    specification = new DownsamplingSpecification("1m-sum-nan");
    final Downsampler downsampler = new FillingDownsampler(source, BASE_TIME,
        BASE_TIME + 60000L * 2L, specification, 0, 0);
    
    long timestamp = 1356998400000L;
    step(downsampler, timestamp, Double.NaN);
    step(downsampler, timestamp += 60000, Double.NaN);
    assertFalse(downsampler.hasNext());
  }
  
  @Test
  public void testDownsampler_noDataCalendar() {
    final SeekableView source = 
        SeekableViewsForTest.fromArray(new DataPoint[] { });
    specification = new DownsamplingSpecification("1mc-sum-nan");
    final Downsampler downsampler = new FillingDownsampler(source, BASE_TIME,
        BASE_TIME + 60000L * 2L, specification, 0, 0);
    
    long timestamp = 1356998400000L;
    step(downsampler, timestamp, Double.NaN);
    step(downsampler, timestamp += 60000, Double.NaN);
    assertFalse(downsampler.hasNext());
  }
  
  private void step(final Downsampler downsampler, final long expected_timestamp, 
      final double expected_value) {
    assertTrue(downsampler.hasNext());
    final DataPoint point = downsampler.next();
    assertEquals(expected_timestamp, point.timestamp());
    assertEquals(expected_value, point.doubleValue(), 0.01);
  }
}

