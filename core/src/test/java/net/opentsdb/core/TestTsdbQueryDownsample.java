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

import java.util.HashMap;

import com.typesafe.config.Config;
import dagger.ObjectGraph;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TsdbStore;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.uid.UniqueIdType;

import javax.inject.Inject;

import static org.junit.Assert.*;

/**
 * Tests downsampling with query.
 */
public class TestTsdbQueryDownsample {
  private static final byte[] SYS_CPU_USER_ID = new byte[]{0, 0, 1};
  private static final byte[] SYS_CPU_NICE_ID = new byte[]{0, 0, 2};
  private static final byte[] HOST_ID = new byte[]{0, 0, 1};
  private static final byte[] WEB01_ID = new byte[]{0, 0, 1};
  private static final byte[] WEB02_ID = new byte[]{0, 0, 2};

  private QueryBuilder builder;

  @Inject Config config;
  @Inject TsdbStore store;
  @Inject UniqueIdClient idClient;
  @Inject DataPointsClient dataPointsClient;

  @Before
  public void before() throws Exception {
    ObjectGraph.create(new TestModuleMemoryStore()).inject(this);

    builder = new QueryBuilder(idClient, config);

    store.allocateUID("sys.cpu.user", SYS_CPU_USER_ID, UniqueIdType.METRIC);
    store.allocateUID("sys.cpu.nice", SYS_CPU_NICE_ID, UniqueIdType.METRIC);

    store.allocateUID("host", HOST_ID, UniqueIdType.TAGK);

    store.allocateUID("web01", WEB01_ID, UniqueIdType.TAGV);
    store.allocateUID("web02", WEB02_ID, UniqueIdType.TAGV);
  }

  @Test
  public void runLongSingleTSDownsample() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    HashMap<String, String> tags = new HashMap<>(1);
    tags.put("host", "web01");

    builder.withStartAndEndTime(1356998400, 1357041600)
           .downsample(60000, Aggregators.SUM)
           .withMetric("sys.cpu.user")
           .withTags(tags);

    final Query query = builder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    final DataPoints[] dps = dataPointsClient.executeQuery(query).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

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

    HashMap<String, String> tags = new HashMap<>(1);
    tags.put("host", "web01");

    builder.withStartAndEndTime(1356998400, 1357041600)
            .downsample(1000, Aggregators.SUM)
            .withMetric("sys.cpu.user")
            .withTags(tags);

    final Query query = builder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    final DataPoints[] dps = dataPointsClient.executeQuery(query).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

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

    HashMap<String, String> tags = new HashMap<>(1);
    tags.put("host", "web01");

    builder.withStartAndEndTime(1356998400, 1357041600)
            .downsample(60000, Aggregators.SUM)
            .withMetric("sys.cpu.user")
            .withTags(tags)
            .shouldCalculateRate(true);

    final Query query = builder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    final DataPoints[] dps = dataPointsClient.executeQuery(query).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

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

    HashMap<String, String> tags = new HashMap<>(1);
    tags.put("host", "web01");

    builder.withStartAndEndTime(1356998400, 1357041600)
            .downsample(1000, Aggregators.SUM)
            .withMetric("sys.cpu.user")
            .withTags(tags)
            .shouldCalculateRate(true);

    final Query query = builder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    final DataPoints[] dps = dataPointsClient.executeQuery(query).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

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

    HashMap<String, String> tags = new HashMap<>(1);
    tags.put("host", "web01");

    builder.withStartAndEndTime(1356998400, 1357041600)
            .downsample(60000, Aggregators.SUM)
            .withMetric("sys.cpu.user")
            .withTags(tags)
            .shouldCalculateRate(false);

    final Query query = builder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    final DataPoints[] dps = dataPointsClient.executeQuery(query).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

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

    HashMap<String, String> tags = new HashMap<>(1);
    tags.put("host", "web01");

    builder.withStartAndEndTime(1356998400, 1357041600)
            .downsample(1000, Aggregators.SUM)
            .withMetric("sys.cpu.user")
            .withTags(tags)
            .shouldCalculateRate(false);

    final Query query = builder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    final DataPoints[] dps = dataPointsClient.executeQuery(query).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

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

    HashMap<String, String> tags = new HashMap<>(1);
    tags.put("host", "web01");

    builder.withStartAndEndTime(1356998400, 1357041600)
            .downsample(60000, Aggregators.SUM)
            .withMetric("sys.cpu.user")
            .withTags(tags)
            .shouldCalculateRate(true);

    final Query query = builder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    final DataPoints[] dps = dataPointsClient.executeQuery(query).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

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

    HashMap<String, String> tags = new HashMap<>(1);
    tags.put("host", "web01");

    builder.withStartAndEndTime(1356998400, 1357041600)
            .downsample(1000, Aggregators.SUM)
            .withMetric("sys.cpu.user")
            .withTags(tags)
            .shouldCalculateRate(true);

    final Query query = builder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    final DataPoints[] dps = dataPointsClient.executeQuery(query).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

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

  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private void storeLongTimeSeriesSeconds(final boolean two_metrics,
      final boolean offset) throws Exception {
    storeLongTimeSeriesSecondsWithBasetime(1356998400L, two_metrics, offset);
  }

  private void storeLongTimeSeriesSecondsWithBasetime(final long baseTimestamp,
      final boolean two_metrics, final boolean offset) throws Exception {
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<>(1);
    tags.put("host", "web01");
    long timestamp = baseTimestamp;
    for (int i = 1; i <= 300; i++) {
      dataPointsClient.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      if (two_metrics) {
        dataPointsClient.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      }
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = baseTimestamp + (offset ? 15 : 0);
    for (int i = 300; i > 0; i--) {
      dataPointsClient.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      if (two_metrics) {
        dataPointsClient.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      }
    }
  }

  private void storeLongTimeSeriesMs() throws Exception {
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400000L;
    for (int i = 1; i <= 300; i++) {
      dataPointsClient.addPoint("sys.cpu.user", timestamp += 500, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      dataPointsClient.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998400000L;
    for (int i = 300; i > 0; i--) {
      dataPointsClient.addPoint("sys.cpu.user", timestamp += 500, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      dataPointsClient.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }
  }

  private void storeFloatTimeSeriesSeconds(final boolean two_metrics,
      final boolean offset) throws Exception {
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      dataPointsClient.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      if (two_metrics) {
        dataPointsClient.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      }
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = offset ? 1356998415 : 1356998400;
    for (float i = 75F; i > 0; i -= 0.25F) {
      dataPointsClient.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      if (two_metrics) {
        dataPointsClient.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      }
    }
  }

  private void storeFloatTimeSeriesMs() throws Exception {
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400000L;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      dataPointsClient.addPoint("sys.cpu.user", timestamp += 500, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      dataPointsClient.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998400000L;
    for (float i = 75F; i > 0; i -= 0.25F) {
      dataPointsClient.addPoint("sys.cpu.user", timestamp += 500, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      dataPointsClient.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }
  }
}
