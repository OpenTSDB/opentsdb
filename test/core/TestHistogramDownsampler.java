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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import com.google.common.collect.Lists;

import net.opentsdb.uid.UniqueId;
import net.opentsdb.core.HistogramSeekableViewForTest.MockHistogramSeekableView;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
// "Classloader hell"... It's real. Tell PowerMock to ignore these classes
// because they fiddle with the class loader. We don't test them anyway.
@PowerMockIgnore({ "javax.management.*", "javax.xml.*", "ch.qos.*", 
  "org.slf4j.*", "com.sum.*", "org.xml.*" })
@PrepareForTest({ HistogramSpan.class, HistogramRowSeq.class, TSDB.class, 
  UniqueId.class, KeyValue.class, Config.class,
    RowKey.class })
public class TestHistogramDownsampler {
  private TSDB tsdb = mock(TSDB.class);
  private Config config = mock(Config.class);
  private UniqueId metrics = mock(UniqueId.class);
  
  private static final long BASE_TIME = 1356998400000L;
  
  private static final HistogramDataPoint[] HIST_DATA_POINTS = 
      new HistogramDataPoint[] {
    // timestamp = 1,356,998,400,000 ms
    new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 40L), BASE_TIME),
    // timestamp = 1,357,000,400,000 ms
    new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 50L), BASE_TIME + 2000000),
    // timestamp = 1,357,002,000,000 ms
    new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 40L), BASE_TIME + 3600000),
    // timestamp = 1,357,002,005,000 ms
    new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 50L), BASE_TIME + 3605000),
    // timestamp = 1,357,005,600,000 ms
    new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 40L), BASE_TIME + 7200000),
    // timestamp = 1,357,007,600,000 ms
    new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 50L), BASE_TIME + 9200000) 
  };
  
  public static final byte[] KEY = 
      new byte[] { 0, 0, 0, 0, 1, 0x50, (byte)0xE2, 0x27, 0, 0, 0, 0, 1, 0, 0, 0, 2 };
  
  // 30 minute offset
  final static TimeZone AF = DateTime.timezones.get("Asia/Kabul");

  // 12h offset w/o DST
  final static TimeZone TV = DateTime.timezones.get("Pacific/Funafuti");

  // 12h offset w DST
  final static TimeZone FJ = DateTime.timezones.get("Pacific/Fiji");

  // Tue, 15 Dec 2015 04:02:25.123 UTC
  final static long DST_TS = 1450137600000L;

  private HistogramSeekableView source;
  private HistogramDownsampler downsampler;
  private DownsamplingSpecification specification;

  @Before
  public void before() {
    // Inject the attributes we need into the "tsdb" object.
    Whitebox.setInternalState(tsdb, "metrics", metrics);
    Whitebox.setInternalState(tsdb, "config", config);
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.metrics.width()).thenReturn((short)4);
    
    source = spy(HistogramSeekableViewForTest.fromArray(HIST_DATA_POINTS));
  }

  @Test
  public void testDownsampler() {
    specification = new DownsamplingSpecification("1000s-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, 0);
    verify(source, never()).next();
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
    }

    assertEquals(5, values.size());
    assertEquals(40L, values.get(0).longValue());
    assertEquals(BASE_TIME - 400000L, timestamps_in_millis.get(0).longValue());
    assertEquals(50, values.get(1).longValue());
    assertEquals(BASE_TIME + 1600000, timestamps_in_millis.get(1).longValue());
    assertEquals(90, values.get(2).longValue());
    assertEquals(BASE_TIME + 3600000L, timestamps_in_millis.get(2).longValue());
    assertEquals(40, values.get(3).longValue());
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(3).longValue());
    assertEquals(50, values.get(4).longValue());
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(4).longValue());
  }

  @Test
  public void testDownsampler_10seconds() {
    source = spy(HistogramSeekableViewForTest
        .fromArray(new HistogramDataPoint[] { 
          new SimpleHistogramDataPointAdapter(
              new LongHistogramDataPointForTest(0, 1L), BASE_TIME + 5000L * 0),
          new SimpleHistogramDataPointAdapter(
              new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 5000L * 1),
          new SimpleHistogramDataPointAdapter(
              new LongHistogramDataPointForTest(0, 4L), BASE_TIME + 5000L * 2),
          new SimpleHistogramDataPointAdapter(
              new LongHistogramDataPointForTest(0, 8L), BASE_TIME + 5000L * 3),
          new SimpleHistogramDataPointAdapter(
              new LongHistogramDataPointForTest(0, 16L), BASE_TIME + 5000L * 4),
          new SimpleHistogramDataPointAdapter(
              new LongHistogramDataPointForTest(0, 32L), BASE_TIME + 5000L * 5),
          new SimpleHistogramDataPointAdapter(
              new LongHistogramDataPointForTest(0, 64L), BASE_TIME + 5000L * 6),
          new SimpleHistogramDataPointAdapter(
              new LongHistogramDataPointForTest(0, 128L), BASE_TIME + 5000L * 7),
          new SimpleHistogramDataPointAdapter(
              new LongHistogramDataPointForTest(0, 256L), BASE_TIME + 5000L * 8),
          new SimpleHistogramDataPointAdapter(
              new LongHistogramDataPointForTest(0, 512L), BASE_TIME + 5000L * 9),
          new SimpleHistogramDataPointAdapter(
              new LongHistogramDataPointForTest(0, 1024L), BASE_TIME + 5000L * 10) 
    }));

    specification = new DownsamplingSpecification("10s-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, 0);
    verify(source, never()).next();
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
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
    source = spy(HistogramSeekableViewForTest
        .fromArray(new HistogramDataPoint[] { 
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 1L), BASE_TIME + 5000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 15000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 4L), BASE_TIME + 25000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 8L), BASE_TIME + 35000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 16L), BASE_TIME + 45000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 32L), BASE_TIME + 55000L) }));
    specification = new DownsamplingSpecification("15s-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, 0);
    verify(source, never()).next();
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
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

  @Test
  public void testDownsampler_allFullRange() {
    source = spy(HistogramSeekableViewForTest
        .fromArray(new HistogramDataPoint[] { 
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 1L), BASE_TIME + 5000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 15000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 4L), BASE_TIME + 25000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 8L), BASE_TIME + 35000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 16L), BASE_TIME + 45000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 32L), BASE_TIME + 55000L) }));
    specification = new DownsamplingSpecification("0all-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
    }

    assertEquals(1, values.size());
    assertEquals(63, values.get(0).longValue());
    assertEquals(0L, timestamps_in_millis.get(0).longValue());
  }

  @Test
  public void testDownsampler_allFilterOnQuery() {
    source = spy(HistogramSeekableViewForTest
        .fromArray(new HistogramDataPoint[] { 
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 1L), BASE_TIME + 5000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 15000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 4L), BASE_TIME + 25000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 8L), BASE_TIME + 35000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 16L), BASE_TIME + 45000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 32L), BASE_TIME + 55000L) }));
    specification = new DownsamplingSpecification("0all-sum");
    downsampler = new HistogramDownsampler(source, specification, 
        BASE_TIME + 15000L, BASE_TIME + 45000L);
    verify(source, never()).next();
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
    }

    assertEquals(1, values.size());
    assertEquals(14, values.get(0).longValue());
    assertEquals(BASE_TIME + 15000L, timestamps_in_millis.get(0).longValue());
  }

  @Test
  public void testDownsampler_allFilterOnQueryOutOfRangeEarly() {
    source = spy(HistogramSeekableViewForTest
        .fromArray(new HistogramDataPoint[] { 
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 1L), BASE_TIME + 5000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 15000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 4L), BASE_TIME + 25000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 8L), BASE_TIME + 35000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 16L), BASE_TIME + 45000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 32L), BASE_TIME + 55000L) }));
    specification = new DownsamplingSpecification("0all-sum");
    downsampler = new HistogramDownsampler(source, specification, 
        BASE_TIME + 65000L, BASE_TIME + 75000L);
    verify(source, never()).next();
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
    }

    assertEquals(0, values.size());
  }

  @Test
  public void testDownsampler_allFilterOnQueryOutOfRangeLate() {
    source = spy(HistogramSeekableViewForTest
        .fromArray(new HistogramDataPoint[] { 
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 1L), BASE_TIME + 5000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 15000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 4L), BASE_TIME + 25000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 8L), BASE_TIME + 35000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 16L), BASE_TIME + 45000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 32L), BASE_TIME + 55000L) }));
    specification = new DownsamplingSpecification("0all-sum");
    downsampler = new HistogramDownsampler(source, specification, 
        BASE_TIME - 15000L, BASE_TIME - 5000L);
    verify(source, never()).next();
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
    }

    assertEquals(0, values.size());
  }

  @Test
  public void testDownsampler_calendarHour() {
    source = spy(HistogramSeekableViewForTest
        .fromArray(new HistogramDataPoint[] { 
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 1L), BASE_TIME),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 1800000),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 3L), BASE_TIME + 3599000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 4L), BASE_TIME + 3600000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 5L), BASE_TIME + 5400000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 6L), BASE_TIME + 7199000L) }));
    specification = new DownsamplingSpecification("1hc-sum");
    specification.setTimezone(TV);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    long ts = BASE_TIME;
    long value = 6;
    while (downsampler.hasNext()) {
      HistogramDataPoint dp = downsampler.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, Bytes.getLong(dp.getRawData(false)));
      ts += 3600000;
      value = 15;
    }

    // hour offset by 30m
    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("1hc-sum");
    specification.setTimezone(AF);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1356996600000L;
    value = 1;
    while (downsampler.hasNext()) {
      HistogramDataPoint dp = downsampler.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, Bytes.getLong(dp.getRawData(false)));
      ts += 3600000;
      if (value == 1) {
        value = 9;
      } else {
        value = 11;
      }
    }

    // multiple hours
    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("4hc-sum");
    specification.setTimezone(AF);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1356996600000L;
    value = 21;
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      assertEquals(ts, hdp.timestamp());
      assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
    }
  }

  @Test
  public void testDownsampler_calendarDay() {
    // UTC
    source = spy(HistogramSeekableViewForTest
        .fromArray(new HistogramDataPoint[] { 
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 1L), DST_TS),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 2L), DST_TS + 86399000),
            // falls to the next in FJ
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 3L), DST_TS + 126001000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 4L), DST_TS + 172799000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 5L), DST_TS + 172800000L),
            // falls within 30m offset
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 6L), DST_TS + 242999000L) }));

    // control
    specification = new DownsamplingSpecification("1d-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    long ts = DST_TS;
    long value = 3;
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      assertEquals(ts, hdp.timestamp());
      assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
      ts += 86400000;
      if (value == 3) {
        value = 7;
      } else if (value == 7) {
        value = 11;
      }
    }
    

    // 12 hour offset from UTC
    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("1dc-sum");
    specification.setTimezone(TV);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1450094400000L;
    value = 1;
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      assertEquals(ts, hdp.timestamp());
      assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
      ts += 86400000;
      if (value == 1) {
        value = 5;
      } else if (value == 5) {
        value = 9;
      } else {
        value = 6;
      }
    }
    

    // 11 hour offset from UTC
    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("1dc-sum");
    specification.setTimezone(FJ);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1450090800000L;
    value = 1;
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      assertEquals(ts, hdp.timestamp());
      assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
      ts += 86400000;
      if (value == 1) {
        value = 2;
      } else if (value == 2) {
        value = 12;
      } else {
        value = 6;
      }
    }
    

    // 30m offset
    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("1dc-sum");
    specification.setTimezone(AF);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1450121400000L;
    value = 1;
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      assertEquals(ts, hdp.timestamp());
      assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
      ts += 86400000;
      if (value == 1) {
        value = 5;
      } else if (value == 5) {
        value = 15;
      }
    }

    // multiple days
    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("3dc-sum");
    specification.setTimezone(AF);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1450121400000L;
    value = 21;
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      assertEquals(ts, hdp.timestamp());
      assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
    }
  }

  @Test
  public void testDownsampler_calendarWeek() {
    source = HistogramSeekableViewForTest
        .fromArray(new HistogramDataPoint[] { 
            // a Tuesday in UTC land
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 1L), DST_TS),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 2L), DST_TS + (86400000L * 7)),
            // falls to the next in FJ
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 3L), 1451129400000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 4L), DST_TS + (86400000L * 21)),
            // falls within 30m offset
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 5L), 1452367799000L)
    });
    // control
    specification = new DownsamplingSpecification("1wc-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    long ts = 1449964800000L;
    long value = 1;
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      assertEquals(ts, hdp.timestamp());
      assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
      if (ts == 1450569600000L) {
        ts = 1451779200000L; // skips a week
      } else {
        ts += 86400000L * 7;
      }
      if (value == 1) {
        value = 5;
      } else {
        value = 9;
      }
    }

    // 12 hour offset from UTC
    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("1wc-sum");
    specification.setTimezone(TV);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1449921600000L;
    value = 1;
    while (downsampler.hasNext()) {
      HistogramDataPoint dp = downsampler.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, Bytes.getLong(dp.getRawData(false)));
      if (ts == 1450526400000L) {
        ts = 1451736000000L; // skip a week
      } else {
        ts += 86400000L * 7;
      }
      if (value == 1) {
        value = 5;
      } else if (value == 5) {
        value = 4;
      } else {
        value = 5;
      }
    }

    // 11 hour offset from UTC
    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("1wc-sum");
    specification.setTimezone(FJ);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1449918000000L;
    value = 1;
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      assertEquals(ts, hdp.timestamp());
      assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
      ts += 86400000L * 7;
      value++;
    }

    // 30m offset
    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("1wc-sum");
    specification.setTimezone(AF);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1449948600000L;
    value = 1;
    while (downsampler.hasNext()) {
      HistogramDataPoint dp = downsampler.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, Bytes.getLong(dp.getRawData(false)));
      if (ts == 1449948600000L) {
        ts = 1450553400000L;
      } else {
        ts = 1451763000000L;
      }
      if (value == 1) {
        value = 5;
      } else {
        value = 9;
      }
    }

    // multiple weeks
    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("2wc-sum");
    specification.setTimezone(AF);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1449948600000L;
    value = 6;
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      assertEquals(ts, hdp.timestamp());
      assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
      ts = 1451158200000L;
      value = 9;
    }
  }

  @Test
  public void testDownsampler_calendarMonth() {
    final long dec_1st = 1448928000000L;
    source = spy(HistogramSeekableViewForTest
        .fromArray(new HistogramDataPoint[] { 
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 1L), dec_1st),
            // falls to the next in FJ
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 2L), 1451559600000L), 
            // jan 1st
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 3L), 1451606400000L), 
            // feb 1st
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 4L), 1454284800000L),
            // feb 29th (leap year)
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 5L), 1456704000000L), 
            // falls within 30m offset AT
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 6L), 1456772400000L)
    }));

    // control
    specification = new DownsamplingSpecification("1nc-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    long ts = dec_1st;
    long value = 3;
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      assertEquals(ts, hdp.timestamp());
      assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
      if (ts == 1448928000000L) {
        ts = 1451606400000L;
      } else {
        ts = 1454284800000L;
        value = 15;
      }
    }

    // 12h offset
    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("1nc-sum");
    specification.setTimezone(TV);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1448884800000L;
    value = 3;
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      assertEquals(ts, hdp.timestamp());
      assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
      if (ts == 1448884800000L) {
        ts = 1451563200000L;
      } else if (ts == 1451563200000L) {
        value = 9;
        ts = 1454241600000L;
      } else {
        ts = 1456747200000L;
        value = 6;
      }
    }

    // 11h offset
    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("1nc-sum");
    specification.setTimezone(FJ);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1448881200000L;
    value = 1;
    while (downsampler.hasNext()) {
      HistogramDataPoint dp = downsampler.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, Bytes.getLong(dp.getRawData(false)));
      if (ts == 1448881200000L) {
        ts = 1451559600000L;
        value = 5;
      } else if (ts == 1451559600000L) {
        ts = 1454241600000L;
        value = 9;
      } else {
        ts = 1456747200000L;
        value = 6;
      }
    }

    // 30m offset
    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("1nc-sum");
    specification.setTimezone(AF);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1448911800000L;
    value = 3;
    while (downsampler.hasNext()) {
      HistogramDataPoint dp = downsampler.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, Bytes.getLong(dp.getRawData(false)));
      if (ts == 1448911800000L) {
        ts = 1451590200000L;
      } else {
        ts = 1454268600000L;
        value = 15;
      }
    }

    // multiple months
    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("3nc-sum");
    specification.setTimezone(TV);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    ts = 1443614400000L;
    value = 3;
    while (downsampler.hasNext()) {
      HistogramDataPoint dp = downsampler.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, Bytes.getLong(dp.getRawData(false)));
      ts = 1451563200000L;
      value = 18;
    }
  }

  @Test
  public void testDownsampler_calendarSkipSomePoints() {
    source = spy(HistogramSeekableViewForTest
        .fromArray(new HistogramDataPoint[] { 
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 1L), BASE_TIME),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 1800000),
            // skip an hour
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 6L), BASE_TIME + 7200000) }));
    specification = new DownsamplingSpecification("1hc-sum");
    specification.setTimezone(TV);
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    long ts = BASE_TIME;
    long value = 3;
    while (downsampler.hasNext()) {
      HistogramDataPoint dp = downsampler.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, Bytes.getLong(dp.getRawData(false)));
      ts = 1357005600000L;
      value = 6;
    }
  }

  @Test
  public void testDownsampler_noData() {
    source = spy(HistogramSeekableViewForTest.fromArray(new HistogramDataPoint[] {}));
    specification = new DownsamplingSpecification("1d-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    assertFalse(downsampler.hasNext());
  }

  @Test
  public void testDownsampler_noDataCalendar() {
    source = spy(HistogramSeekableViewForTest.fromArray(new HistogramDataPoint[] {}));
    specification = new DownsamplingSpecification("1mc-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);
    verify(source, never()).next();
    assertFalse(downsampler.hasNext());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRemove() {
    specification = new DownsamplingSpecification("1d-sum");
    new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE).remove();
  }

  @Test
  public void testSeek() {
    specification = new DownsamplingSpecification("1000s-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, 0);
    downsampler.seek(BASE_TIME + 3600000L);
    verify(source, never()).next();
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      HistogramDataPoint dp = downsampler.next();
      values.add(Bytes.getLong(dp.getRawData(false)));
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(3, values.size());
    assertEquals(90, values.get(0).longValue());
    assertEquals(BASE_TIME + 3600000L, timestamps_in_millis.get(0).longValue());
    assertEquals(40, values.get(1).longValue());
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(1).longValue());
    assertEquals(50, values.get(2).longValue());
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(2).longValue());
  }

  @Test
  public void testSeek_skipPartialInterval() {
    specification = new DownsamplingSpecification("1000s-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, 0);
    downsampler.seek(BASE_TIME + 3800000L);
    verify(source, never()).next();
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      HistogramDataPoint dp = downsampler.next();
      values.add(Bytes.getLong(dp.getRawData(false)));
      timestamps_in_millis.add(dp.timestamp());
    }

    // seek timestamp was BASE_TIME + 3800000L or 1,357,002,200,000 ms.
    // The interval that has the timestamp began at 1,357,002,000,000 ms. It
    // had two data points but was abandoned because the requested timestamp
    // was not aligned. The next two intervals at 1,357,003,000,000 and
    // at 1,357,004,000,000 did not have data points. The first interval that
    // had a data point began at 1,357,002,005,000 ms or BASE_TIME + 6600000L.
    assertEquals(2, values.size());
    assertEquals(40, values.get(0).longValue());
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(0).longValue());
    assertEquals(50, values.get(1).longValue());
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(1).longValue());
  }

  @Test
  public void testSeek_doubleIteration() {
    specification = new DownsamplingSpecification("1000s-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, 0);
    while (downsampler.hasNext()) {
      downsampler.next();
    }
    downsampler.seek(BASE_TIME + 3600000L);
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
    }

    assertEquals(3, values.size());
    assertEquals(90, values.get(0).longValue());
    assertEquals(BASE_TIME + 3600000L, timestamps_in_millis.get(0).longValue());
    assertEquals(40, values.get(1).longValue());
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(1).longValue());
    assertEquals(50, values.get(2).longValue());
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(2).longValue());
  }

  @Test
  public void testSeek_abandoningIncompleteInterval() {
    source = HistogramSeekableViewForTest
        .fromArray(new HistogramDataPoint[] { 
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 40L), BASE_TIME + 100L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 40L), BASE_TIME + 1100L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 40L), BASE_TIME + 2100L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 40L), BASE_TIME + 3100L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 40L), BASE_TIME + 4100L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 40L), BASE_TIME + 5100L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 40L), BASE_TIME + 6100L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 40L), BASE_TIME + 7100L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 40L), BASE_TIME + 8100L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 40L), BASE_TIME + 9100L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 40L), BASE_TIME + 10100L) });
    specification = new DownsamplingSpecification("10s-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, 0);
    // The seek is aligned by the downsampling window.
    downsampler.seek(BASE_TIME);
    assertTrue("seek(BASE_TIME)", downsampler.hasNext());
    HistogramDataPoint first_dp = downsampler.next();
    assertEquals("seek(1356998400000)", BASE_TIME, first_dp.timestamp());
    assertEquals("seek(1356998400000)", 400, Bytes.getLong(first_dp.getRawData(false)));

    // No seeks but the last one is aligned by the downsampling window.
    for (long seek_timestamp = BASE_TIME + 1000L; seek_timestamp < BASE_TIME 
        + 10100L; seek_timestamp += 1000) {
      downsampler.seek(seek_timestamp);
      assertTrue("ts = " + seek_timestamp, downsampler.hasNext());
      HistogramDataPoint dp = downsampler.next();
      // Timestamp should be greater than or equal to the seek timestamp.
      assertTrue(String.format("%d >= %d", dp.timestamp(), seek_timestamp), 
          dp.timestamp() >= seek_timestamp);
      assertEquals(String.format("seek(%d)", seek_timestamp), 
          BASE_TIME + 10000L, dp.timestamp());
      assertEquals(String.format("seek(%d)", seek_timestamp), 
          40, Bytes.getLong(dp.getRawData(false)));
    }
  }

  @Test
  public void testSeek_useCalendar() {
    source = spy(HistogramSeekableViewForTest
        .fromArray(new HistogramDataPoint[] { 
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 1L), 1356998400000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 2L), 1388534400000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 4L), 1420070400000L),
            new SimpleHistogramDataPointAdapter(
                new LongHistogramDataPointForTest(0, 8L), 1451606400000L) }));

    specification = new DownsamplingSpecification("1yc-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, Long.MAX_VALUE);

    downsampler.seek(1420070400000L);
    verify(source, never()).next();

    long timestamp = 1420070400000L;
    long value = 4;
    while (downsampler.hasNext()) {
      HistogramDataPoint dp = downsampler.next();
      System.out.println(dp.timestamp() + " " + Bytes.getLong(dp.getRawData(false)));
      assertEquals(timestamp, dp.timestamp());
      assertEquals(value, Bytes.getLong(dp.getRawData(false)));
      timestamp = 1451606400000L;
      value = 8;
    }

    ((MockHistogramSeekableView) source).resetIndex();
    specification = new DownsamplingSpecification("1yc-sum");
    downsampler = new HistogramDownsampler(source, specification, 0, 0);
    downsampler.seek(1420070400001L);

    while (downsampler.hasNext()) {
      HistogramDataPoint dp = downsampler.next();
      assertEquals(timestamp, dp.timestamp());
      assertEquals(value, Bytes.getLong(dp.getRawData(false)));
    }
  }

  @Test
  public void testHistogramSpanDownSampler() {
    List<HistogramDataPoint> row = Arrays.asList(HIST_DATA_POINTS);
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);
    
    // check the data points using iterator
    List<Long> it_values = Lists.newArrayList();
    HistogramSpan.Iterator it = hspan.spanIterator();
    while (it.hasNext()) {
      HistogramDataPoint hdp = it.next();
      it_values.add(Bytes.getLong(hdp.getRawData(false)));
    }
    assertEquals(6, it_values.size());
    assertEquals(40L, it_values.get(0).longValue());
    assertEquals(50L, it_values.get(1).longValue());
    assertEquals(40L, it_values.get(2).longValue());
    assertEquals(50L, it_values.get(3).longValue());
    assertEquals(40L, it_values.get(4).longValue());
    assertEquals(50L, it_values.get(5).longValue());
    
    
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    specification = new DownsamplingSpecification("1000s-sum");
    downsampler = hspan.downsampler(0, 0, specification, false, 0, 0);
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
    }

    assertEquals(5, values.size());
    assertEquals(40L, values.get(0).longValue());
    assertEquals(BASE_TIME - 400000L, timestamps_in_millis.get(0).longValue());
    assertEquals(50, values.get(1).longValue());
    assertEquals(BASE_TIME + 1600000, timestamps_in_millis.get(1).longValue());
    assertEquals(90, values.get(2).longValue());
    assertEquals(BASE_TIME + 3600000L, timestamps_in_millis.get(2).longValue());
    assertEquals(40, values.get(3).longValue());
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(3).longValue());
    assertEquals(50, values.get(4).longValue());
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(4).longValue());
  }
  
  @Test
  public void testHistogramSpanDownSampler_10seconds() {
    List<HistogramDataPoint> row = Arrays.asList(new HistogramDataPoint[] {
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 1L), BASE_TIME + 5000L * 0),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 5000L * 1),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 4L), BASE_TIME + 5000L * 2),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 8L), BASE_TIME + 5000L * 3),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 16L), BASE_TIME + 5000L * 4),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 32L), BASE_TIME + 5000L * 5),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 64L), BASE_TIME + 5000L * 6),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 128L), BASE_TIME + 5000L * 7),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 256L), BASE_TIME + 5000L * 8),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 512L), BASE_TIME + 5000L * 9),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 1024L), BASE_TIME + 5000L * 10) });
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);
   
    // downsample iterator the span
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    specification = new DownsamplingSpecification("10s-sum");
    downsampler = hspan.downsampler(0, 0, specification, false, 0, 0);
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
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
  public void testHistogramSpanDownSampler_15seconds() {
    List<HistogramDataPoint> row = Arrays.asList(new HistogramDataPoint[] { 
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 1L), BASE_TIME + 5000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 15000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 4L), BASE_TIME + 25000L),
        new SimpleHistogramDataPointAdapter(    
            new LongHistogramDataPointForTest(0, 8L), BASE_TIME + 35000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 16L), BASE_TIME + 45000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 32L), BASE_TIME + 55000L) });
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);
   
    // downsample iterator the span
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    specification = new DownsamplingSpecification("15s-sum");
    downsampler = hspan.downsampler(0, 0, specification, false, 0, 0);
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
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
  
  @Test
  public void testHistogramSpanDownsampler_allFullRange() {
    List<HistogramDataPoint> row = Arrays.asList(new HistogramDataPoint[] {
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 1L), BASE_TIME + 5000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 15000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 4L), BASE_TIME + 25000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 8L), BASE_TIME + 35000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 16L), BASE_TIME + 45000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 32L), BASE_TIME + 55000L) });
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);
    
    specification = new DownsamplingSpecification("0all-sum");
    downsampler = hspan.downsampler(0, 0, specification, false, 0, Long.MAX_VALUE);
    
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
    }

    assertEquals(1, values.size());
    assertEquals(63, values.get(0).longValue());
    assertEquals(0L, timestamps_in_millis.get(0).longValue());
  }
  
  @Test
  public void testHistogramSpanDownsampler_allFilterOnQuery() {
    List<HistogramDataPoint> row = Arrays.asList(new HistogramDataPoint[] { 
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 1L), BASE_TIME + 5000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 15000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 4L), BASE_TIME + 25000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 8L), BASE_TIME + 35000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 16L), BASE_TIME + 45000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 32L), BASE_TIME + 55000L) });
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);
    specification = new DownsamplingSpecification("0all-sum");
    downsampler = hspan.downsampler(0, 0, specification, false, BASE_TIME + 15000L, BASE_TIME + 45000L);

    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
    }

    assertEquals(1, values.size());
    assertEquals(14, values.get(0).longValue());
    assertEquals(BASE_TIME + 15000L, timestamps_in_millis.get(0).longValue());
  }
  
  @Test
  public void testHistogramSpanDownsampler_allFilterOnQueryOutOfRangeEarly() {
    List<HistogramDataPoint> row = Arrays.asList(new HistogramDataPoint[] { 
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 1L), BASE_TIME + 5000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 15000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 4L), BASE_TIME + 25000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 8L), BASE_TIME + 35000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 16L), BASE_TIME + 45000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 32L), BASE_TIME + 55000L) });
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);
    
    specification = new DownsamplingSpecification("0all-sum");
    downsampler = hspan.downsampler(0, 0, specification, false, BASE_TIME + 65000L, BASE_TIME + 75000L);
   
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
    }

    assertEquals(0, values.size());
  }
  
  @Test
  public void testHistogramSpanDownsampler_allFilterOnQueryOutOfRangeLate() {
    List<HistogramDataPoint> row = Arrays.asList(new HistogramDataPoint[] {
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 1L), BASE_TIME + 5000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 15000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 4L), BASE_TIME + 25000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 8L), BASE_TIME + 35000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 16L), BASE_TIME + 45000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 32L), BASE_TIME + 55000L) });
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);
    
    specification = new DownsamplingSpecification("0all-sum");
    downsampler = hspan.downsampler(0, 0, specification, false, BASE_TIME - 15000L, BASE_TIME - 5000L);
    
    List<Long> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      HistogramDataPoint hdp = downsampler.next();
      values.add(Bytes.getLong(hdp.getRawData(false)));
      timestamps_in_millis.add(hdp.timestamp());
    }

    assertEquals(0, values.size());
  }
  
  @Test
  public void testHistogramSpanDownsampler_calendarHour() {
    List<HistogramDataPoint> row = Arrays.asList(new HistogramDataPoint[] {
        new SimpleHistogramDataPointAdapter(
          new LongHistogramDataPointForTest(0, 1L), BASE_TIME),
        new SimpleHistogramDataPointAdapter(
          new LongHistogramDataPointForTest(0, 2L), BASE_TIME + 1800000),
        new SimpleHistogramDataPointAdapter(
          new LongHistogramDataPointForTest(0, 3L), BASE_TIME + 3599000L),
        new SimpleHistogramDataPointAdapter(
          new LongHistogramDataPointForTest(0, 4L), BASE_TIME + 3600000L),
        new SimpleHistogramDataPointAdapter(
          new LongHistogramDataPointForTest(0, 5L), BASE_TIME + 5400000L),
        new SimpleHistogramDataPointAdapter(
          new LongHistogramDataPointForTest(0, 6L), BASE_TIME + 7199000L) });
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);
    
    {
      specification = new DownsamplingSpecification("1hc-sum");
      specification.setTimezone(TV);
      downsampler = hspan.downsampler(0, 0, specification, false, 0, Long.MAX_VALUE);
      
      long ts = BASE_TIME;
      long value = 6;
      while (downsampler.hasNext()) {
        HistogramDataPoint dp = downsampler.next();
        assertEquals(ts, dp.timestamp());
        assertEquals(value, Bytes.getLong(dp.getRawData(false)));
        ts += 3600000;
        value = 15;
      }
    }

    // hour offset by 30m
    {
      specification = new DownsamplingSpecification("1hc-sum");
      specification.setTimezone(AF);
      downsampler = hspan.downsampler(0, 0, specification, false, 0, Long.MAX_VALUE);

      long ts = 1356996600000L;
      long value = 1;
      while (downsampler.hasNext()) {
        HistogramDataPoint dp = downsampler.next();
        assertEquals(ts, dp.timestamp());
        assertEquals(value, Bytes.getLong(dp.getRawData(false)));
        ts += 3600000;
        if (value == 1) {
          value = 9;
        } else {
          value = 11;
        }
      }

    }
    
    // multiple hours
    {
      specification = new DownsamplingSpecification("4hc-sum");
      specification.setTimezone(AF);
      downsampler = hspan.downsampler(0, 0, specification, false, 0, Long.MAX_VALUE);

      long ts = 1356996600000L;
      long value = 21;
      while (downsampler.hasNext()) {
        HistogramDataPoint hdp = downsampler.next();
        assertEquals(ts, hdp.timestamp());
        assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
      }
    }
  }
  
  @Test
  public void testHistogramSpanDownsampler_calendarDay() {
    // UTC
    List<HistogramDataPoint> row = Arrays.asList(new HistogramDataPoint[] { 
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 1L), DST_TS),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 2L), DST_TS + 86399000),
            // falls to the next in FJ
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 3L), DST_TS + 126001000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 4L), DST_TS + 172799000L),
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 5L), DST_TS + 172800000L),
            // falls within 30m offset
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 6L), DST_TS + 242999000L) });

    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);
    
    // control
    {
      specification = new DownsamplingSpecification("1d-sum");
      downsampler = hspan.downsampler(0, 0, specification, false, 0, Long.MAX_VALUE);

      long ts = DST_TS;
      long value = 3;
      while (downsampler.hasNext()) {
        HistogramDataPoint hdp = downsampler.next();
        assertEquals(ts, hdp.timestamp());
        assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
        ts += 86400000;
        if (value == 3) {
          value = 7;
        } else if (value == 7) {
          value = 11;
        }
      }
    }

    // 12 hour offset from UTC
    {
      specification = new DownsamplingSpecification("1dc-sum");
      specification.setTimezone(TV);
      downsampler = hspan.downsampler(0, 0, specification, false, 0, Long.MAX_VALUE);

      long ts = 1450094400000L;
      long value = 1;
      while (downsampler.hasNext()) {
        HistogramDataPoint hdp = downsampler.next();
        assertEquals(ts, hdp.timestamp());
        assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
        ts += 86400000;
        if (value == 1) {
          value = 5;
        } else if (value == 5) {
          value = 9;
        } else {
          value = 6;
        }
      }
    }
    
    // 11 hour offset from UTC
    {
      specification = new DownsamplingSpecification("1dc-sum");
      specification.setTimezone(FJ);
      downsampler = hspan.downsampler(0, 0, specification, false, 0, Long.MAX_VALUE);

      long ts = 1450090800000L;
      long value = 1;
      while (downsampler.hasNext()) {
        HistogramDataPoint hdp = downsampler.next();
        assertEquals(ts, hdp.timestamp());
        assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
        ts += 86400000;
        if (value == 1) {
          value = 2;
        } else if (value == 2) {
          value = 12;
        } else {
          value = 6;
        }
      }
    }
    
    {
      // 30m offset
      specification = new DownsamplingSpecification("1dc-sum");
      specification.setTimezone(AF);
      downsampler = hspan.downsampler(0, 0, specification, false, 0, Long.MAX_VALUE);

      long ts = 1450121400000L;
      long value = 1;
      while (downsampler.hasNext()) {
        HistogramDataPoint hdp = downsampler.next();
        assertEquals(ts, hdp.timestamp());
        assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
        ts += 86400000;
        if (value == 1) {
          value = 5;
        } else if (value == 5) {
          value = 15;
        }
      }
    }
    
    {
      // multiple days
      specification = new DownsamplingSpecification("3dc-sum");
      specification.setTimezone(AF);
      downsampler = hspan.downsampler(0, 0, specification, false, 0, Long.MAX_VALUE);

      long ts = 1450121400000L;
      long value = 21;
      while (downsampler.hasNext()) {
        HistogramDataPoint hdp = downsampler.next();
        assertEquals(ts, hdp.timestamp());
        assertEquals(value, Bytes.getLong(hdp.getRawData(false)));
      }
    }
  }
}
