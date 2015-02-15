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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import dagger.ObjectGraph;
import net.opentsdb.TestModule;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.uid.UniqueIdType;

import javax.inject.Inject;

import static org.junit.Assert.*;

/**
 * Massive test class that is used to test all facets of querying for data. 
 * Since data is fetched using the TsdbQuery class, it makes sense to put all
 * of the unit tests here that deal with actual data. This includes:
 * - queries
 * - aggregations
 * - rate conversion
 * - downsampling
 * - compactions (read and write);
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
        "ch.qos.*", "org.slf4j.*",
        "com.sum.*", "org.xml.*"})
@PrepareForTest({KeyValue.class, Scanner.class, Query.class})
public final class DataPointsClientExecuteQueryTest {
  public static final int START_TIME_1 = 1356998400;
  public static final int END_TIME_1 = 1357041600;
  public static final long START_TIME_1L = 1356998400L;
  public static final long START_TIME_1000L = 1356998400000L;
  public static final long START_TIME_1250L = 1356998400250L;
  public static final long START_TIME_1500L = 1356998400500L;
  public static final long END_TIME_1L = 1357041600L;
  public static final String HOST = "host";
  public static final String WEB_01 = "web01";
  public static final String METRIC_1 = "sys.cpu.user";
  public static final String METRIC_2 = "sys.cpu.nice";
  public static final String WEB_02 = "web02";
  public static final String WILDCARD = "*";
  public static final String ENABLE_COMPACTIONS = "enable_compactions";
  public static final String E22700000001000001 = "00000150E22700000001000001";
  public static final String E23510000001000001 = "00000150E23510000001000001";
  public static final String E24320000001000001 = "00000150E24320000001000001";
  public static final int END_TIME_2 = 1357001900;
  public static final String E22700000001000002 = "00000150E22700000001000002";
  public static final String E23510000001000002 = "00000150E23510000001000002";
  public static final String E24320000001000002 = "00000150E24320000001000002";
  public static final String TSUID_1 = "000001000001000001";
  public static final int START_TIME_3 = 1356998490;
  public static final String DESCRIPTION = "Hello World!";
  public static final int START_TIME_4 = 1357002090;
  public static final String TSUID_2 = "000001000001000002";
  public static final String TSUID_3 = "000001000001000005";
  public static final int TIMESTAMP_5 = 1356998415;
  public static final long TIMESTAMP_1L = 1356998430000L;
  public static final long TIMESTAMP_4L = 1357007400000L;
  public static final long TIMESTAMP_2L = TIMESTAMP_4L;
  public static final long TIMESTAMP_3L = 1356998550000L;
  private Config config;
  private QueryBuilder queryBuilder = null;

  @Inject TSDB tsdb;
  @Inject MemoryStore tsdb_store;

  private static final byte[] SYS_CPU_USER_ID = new byte[]{0, 0, 1};
  private static final byte[] SYS_CPU_NICE_ID = new byte[]{0, 0, 2};
  private static final byte[] HOST_ID = new byte[]{0, 0, 1};
  private static final byte[] WEB01_ID = new byte[]{0, 0, 1};
  private static final byte[] WEB02_ID = new byte[]{0, 0, 2};

  @Before
  public void before() throws Exception {
    config = new Config(false);
    config.setFixDuplicates(true);  // TODO(jat): test both ways

    ObjectGraph.create(new TestModuleMemoryStore(config)).inject(this);

    queryBuilder = new QueryBuilder(tsdb);

    tsdb_store.allocateUID(METRIC_1, SYS_CPU_USER_ID, UniqueIdType.METRIC);
    tsdb_store.allocateUID(METRIC_2, SYS_CPU_NICE_ID, UniqueIdType.METRIC);

    tsdb_store.allocateUID(HOST, HOST_ID, UniqueIdType.TAGK);

    tsdb_store.allocateUID(WEB_01, WEB01_ID, UniqueIdType.TAGV);
    tsdb_store.allocateUID(WEB_02, WEB02_ID, UniqueIdType.TAGV);
  }

  @Test
  public void runLongSingleTS() throws Exception {
    storeLongTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);

    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);



    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].aggregatedSize());
  }

  @Test
  public void runLongSingleTSMs() throws Exception {
    storeLongTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);

    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].aggregatedSize());
  }

  @Test
  public void runLongSingleTSNoData() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertEquals(0, dps.length);
  }

  @Test
  public void runLongTwoAggSum() throws Exception {
    storeLongTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>();
    queryBuilder.withStartAndEndTime(START_TIME_1L,END_TIME_1L);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    for (DataPoint dp : dps[0]) {
      assertEquals(301, dp.longValue());
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runLongTwoAggSumMs() throws Exception {
    storeLongTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>();
    queryBuilder.withStartAndEndTime(START_TIME_1L,END_TIME_1L);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    for (DataPoint dp : dps[0]) {
      assertEquals(301, dp.longValue());
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runLongTwoGroup() throws Exception {
    storeLongTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WILDCARD);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertEquals(2, dps.length);

    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    assertArrayEquals(SYS_CPU_USER_ID, dps[1].metric());
    assertTrue(dps[1].aggregatedTags().isEmpty());
    assertNull(dps[1].getAnnotations());
    assertArrayEquals(WEB02_ID, dps[1].tags().get(HOST_ID));

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].size());

    value = 300;
    for (DataPoint dp : dps[1]) {
      assertEquals(value, dp.longValue());
      value--;
    }
    assertEquals(300, dps[1].size());
  }

  @Test
  public void runLongSingleTSRate() throws Exception {
    storeLongTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(true);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    for (DataPoint dp : dps[0]) {
      assertEquals(0.033F, dp.doubleValue(), 0.001);
    }
    assertEquals(299, dps[0].size());
  }

  @Test
  public void runLongSingleTSRateMs() throws Exception {
    storeLongTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(true);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    for (DataPoint dp : dps[0]) {
      assertEquals(2.0F, dp.doubleValue(), 0.001);
    }
    assertEquals(299, dps[0].size());
  }

  @Test
  public void runLongSingleTSCompacted() throws Exception {
    storeLongCompactions();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].size());
  }

  // Can't run this one since the TreeMap will order the compacted row AFTER
  // the other data points. A full MockBase implementation would allow this
//  @Test
//  public void runLongSingleTSCompactedAndNonCompacted() throws Exception {
//    storeLongCompactions();
//    HashMap<String, String> tags = new HashMap<String, String>(1);
//    tags.put(HOST, WEB_01);
//
//    long timestamp = 1357007460;
//    for (int i = 301; i <= 310; i++) {
//      tsdb.addPoint(METRIC_1, timestamp += 30, i, tags).joinUninterruptibly();
//    }
//    storage.dumpToSystemOut(false);
//    queryBuilder.withStartTime(START_TIME_1);
//    queryBuilder.setEndTime(END_TIME_1);
//          queryBuilder.withMetric(METRIC_1);
//  queryBuilder.withTags(tags);
//  queryBuilder.withAggregator(Aggregators.SUM);
//          queryBuilder.shouldCalculateRate(false);
//    final DataPoints[] dps = tsdb.executeQuery(queryBuilder.createQuery().joinUninterruptibly()).joinUninterruptibly();
//    assertNotNull(dps);
//
//    int value = 1;
//    for (DataPoint dp : dps[0]) {
//      assertEquals(value, dp.longValue());
//      value++;
//    }
//    assertEquals(310, dps[0].size());
//  }

  @Test
  public void runFloatSingleTS() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    double value = 1.25D;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0.001);
      value += 0.25D;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runFloatSingleTSMs() throws Exception {
    storeFloatTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    double value = 1.25D;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0.001);
      value += 0.25D;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runFloatTwoAggSum() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>();
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    for (DataPoint dp : dps[0]) {
      assertEquals(76.25, dp.doubleValue(), 0.00001);
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runFloatTwoAggSumMs() throws Exception {
    storeFloatTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>();
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    for (DataPoint dp : dps[0]) {
      assertEquals(76.25, dp.doubleValue(), 0.00001);
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runFloatTwoGroup() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WILDCARD);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertEquals(2, dps.length);

    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    assertArrayEquals(SYS_CPU_USER_ID, dps[1].metric());
    assertTrue(dps[1].aggregatedTags().isEmpty());
    assertNull(dps[1].getAnnotations());
    assertArrayEquals(WEB02_ID, dps[1].tags().get(HOST_ID));

    double value = 1.25D;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0.0001);
      value += 0.25D;
    }
    assertEquals(300, dps[0].size());

    value = 75D;
    for (DataPoint dp : dps[1]) {
      assertEquals(value, dp.doubleValue(), 0.0001);
      value -= 0.25d;
    }
    assertEquals(300, dps[1].size());
  }

  @Test
  public void runFloatSingleTSRate() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(true);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    for (DataPoint dp : dps[0]) {
      assertEquals(0.00833F, dp.doubleValue(), 0.00001);
    }
    assertEquals(299, dps[0].size());
  }

  @Test
  public void runFloatSingleTSRateMs() throws Exception {
    storeFloatTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(true);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    for (DataPoint dp : dps[0]) {
      assertEquals(0.5F, dp.doubleValue(), 0.00001);
    }
    assertEquals(299, dps[0].size());
  }

  @Test
  public void runFloatSingleTSCompacted() throws Exception {
    storeFloatCompactions();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    double value = 1.25D;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0.001);
      value += 0.25D;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMixedSingleTS() throws Exception {
    storeMixedTimeSeriesSeconds();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.AVG);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    double float_value = 1.25D;
    int int_value = 76;
    // due to aggregation, the only int that will be returned will be the very
    // last value of 76 since the agg will convert every point in between to a
    // double
    for (DataPoint dp : dps[0]) {
      if (dp.isInteger()) {
        assertEquals(int_value, dp.longValue());
        int_value++;
        float_value = int_value;
      } else {
        assertEquals(float_value, dp.doubleValue(), 0.001);
        float_value += 0.25D;
      }
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMixedSingleTSMsAndS() throws Exception {
    storeMixedTimeSeriesMsAndS();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.AVG);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    double float_value = 1.25D;
    int int_value = 76;
    // due to aggregation, the only int that will be returned will be the very
    // last value of 76 since the agg will convert every point in between to a
    // double
    for (DataPoint dp : dps[0]) {
      if (dp.isInteger()) {
        assertEquals(int_value, dp.longValue());
        int_value++;
        float_value = int_value;
      } else {
        assertEquals(float_value, dp.doubleValue(), 0.001);
        float_value += 0.25D;
      }
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMixedSingleTSPostCompaction() throws Exception {
    storeMixedTimeSeriesSeconds();

    final Field compact = Config.class.getDeclaredField(ENABLE_COMPACTIONS);
    compact.setAccessible(true);
    compact.set(config, true);

    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.AVG);
    queryBuilder.shouldCalculateRate(false);
    assertNotNull(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    // this should only compact the rows for the time series that we fetched and
    // leave the others alone
    assertEquals(1, tsdb_store.numColumns(
            MockBase.stringToBytes(E22700000001000001)));
    assertEquals(1, tsdb_store.numColumns(
            MockBase.stringToBytes(E23510000001000001)));
    assertEquals(1, tsdb_store.numColumns(
            MockBase.stringToBytes(E24320000001000001)));

    // run it again to verify the compacted data uncompacts properly
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    double float_value = 1.25D;
    int int_value = 76;
    // due to aggregation, the only int that will be returned will be the very
    // last value of 76 since the agg will convert every point in between to a
    // double
    for (DataPoint dp : dps[0]) {
      if (dp.isInteger()) {
        assertEquals(int_value, dp.longValue());
        int_value++;
        float_value = int_value;
      } else {
        assertEquals(float_value, dp.doubleValue(), 0.001);
        float_value += 0.25D;
      }
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMixedSingleTSCompacted() throws Exception {
    storeMixedCompactions();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.AVG);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    double float_value = 1.25D;
    int int_value = 76;
    // due to aggregation, the only int that will be returned will be the very
    // last value of 76 since the agg will convert every point in between to a
    // double
    for (DataPoint dp : dps[0]) {
      if (dp.isInteger()) {
        assertEquals(int_value, dp.longValue());
        int_value++;
        float_value = int_value;
      } else {
        assertEquals(float_value, dp.doubleValue(), 0.001);
        float_value += 0.25D;
      }
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runEndTime() throws Exception {
    storeLongTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1, END_TIME_2);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(236, dps[0].size());
  }

  @Test
  public void runCompactPostQuery() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    final Field compact = Config.class.getDeclaredField(ENABLE_COMPACTIONS);
    compact.setAccessible(true);
    compact.set(config, true);

    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartTime(START_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    assertNotNull(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    // this should only compact the rows for the time series that we fetched and
    // leave the others alone
    assertEquals(1, tsdb_store.numColumns(
            MockBase.stringToBytes(E22700000001000001)));
    assertEquals(119, tsdb_store.numColumns(
            MockBase.stringToBytes(E22700000001000002)));
    assertEquals(1, tsdb_store.numColumns(
            MockBase.stringToBytes(E23510000001000001)));
    assertEquals(120, tsdb_store.numColumns(
            MockBase.stringToBytes(E23510000001000002)));
    assertEquals(1, tsdb_store.numColumns(
            MockBase.stringToBytes(E24320000001000001)));
    assertEquals(61, tsdb_store.numColumns(
            MockBase.stringToBytes(E24320000001000002)));

    // run it again to verify the compacted data uncompacts properly
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].size());
  }

  @Test (expected = IllegalStateException.class)

  public void runStartNotSet() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void runFloatAndIntSameTS() throws Exception {
    // if a row has an integer and a float for the same timestamp, there will be
    // two different qualifiers that will resolve to the same offset. This no
    // longer tosses an exception, and keeps the last value
    storeLongTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    tsdb.getDataPointsClient().addPoint(METRIC_1, 1356998430, 42.5F, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(true);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    // TODO: further validate the result
  }

  @Test
  public void runWithAnnotation() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    final Annotation note = new Annotation();
    note.setTSUID(TSUID_1);
    note.setStartTime(START_TIME_3);
    note.setDescription(DESCRIPTION);
    tsdb.getMetaClient().syncToStorage(note, false).joinUninterruptibly(MockBase
            .DEFAULT_TIMEOUT);

    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);

    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertEquals(1, dps[0].getAnnotations().size());
    assertEquals(DESCRIPTION, dps[0].getAnnotations().get(0).getDescription());

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runWithAnnotationPostCompact() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    final Annotation note = new Annotation();
    note.setTSUID(TSUID_1);
    note.setStartTime(START_TIME_3);
    note.setDescription(DESCRIPTION);
    tsdb.getMetaClient().syncToStorage(note, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    final Field compact = Config.class.getDeclaredField(ENABLE_COMPACTIONS);
    compact.setAccessible(true);
    compact.set(config, true);

    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    assertNotNull(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    // this should only compact the rows for the time series that we fetched and
    // leave the others alone
    assertEquals(2, tsdb_store.numColumns(
            MockBase.stringToBytes(E22700000001000001)));
    assertEquals(119, tsdb_store.numColumns(
            MockBase.stringToBytes(E22700000001000002)));
    assertEquals(1, tsdb_store.numColumns(
            MockBase.stringToBytes(E23510000001000001)));
    assertEquals(120, tsdb_store.numColumns(
            MockBase.stringToBytes(E23510000001000002)));
    assertEquals(1, tsdb_store.numColumns(
            MockBase.stringToBytes(E24320000001000001)));
    assertEquals(61, tsdb_store.numColumns(
            MockBase.stringToBytes(E24320000001000002)));

    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertEquals(1, dps[0].getAnnotations().size());
    assertEquals(DESCRIPTION, dps[0].getAnnotations().get(0).getDescription());

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runWithOnlyAnnotation() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    // verifies that we can pickup an annotation stored all by it's lonesome
    // in a row without any data
    tsdb_store.flushRow(MockBase.stringToBytes(E23510000001000001));
    final Annotation note = new Annotation();
    note.setTSUID(TSUID_1);
    note.setStartTime(START_TIME_4);
    note.setDescription(DESCRIPTION);
    tsdb.getMetaClient().syncToStorage(note, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);

    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertEquals(1, dps[0].getAnnotations().size());
    assertEquals(DESCRIPTION, dps[0].getAnnotations().get(0).getDescription());

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
      // account for the jump
      if (value == 120) {
        value = 240;
      }
    }
    assertEquals(180, dps[0].size());
  }

  @Test
  public void runWithSingleAnnotation() throws Exception {
    // verifies that we can pickup an annotation stored all by it's lonesome
    // in a row without any data
    tsdb_store.flushRow(MockBase.stringToBytes(E23510000001000001));
    final Annotation note = new Annotation();
    note.setTSUID(TSUID_1);
    note.setStartTime(START_TIME_4);
    note.setDescription(DESCRIPTION);
    tsdb.getMetaClient().syncToStorage(note, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);

    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertEquals(1, dps[0].getAnnotations().size());
    assertEquals(DESCRIPTION, dps[0].getAnnotations().get(0).getDescription());

    assertEquals(0, dps[0].size());
  }

  @Test
  public void runSingleDataPoint() throws Exception {
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    long timestamp = 1356998410;
    tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add(TSUID_1);
    queryBuilder.withTSUIDS(tsuids);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate( false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertEquals(1, dps.length);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));
    assertEquals(42, dps[0].longValue(0));
  }

  @Test
  public void runSingleDataPointWithAnnotation() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    long timestamp = 1356998410;
    tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    tsdb_store.flushRow(MockBase.stringToBytes(E23510000001000001));
    final Annotation note = new Annotation();
    note.setTSUID(TSUID_1);
    note.setStartTime(START_TIME_4);
    note.setDescription(DESCRIPTION);
    tsdb.getMetaClient().syncToStorage(note, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add(TSUID_1);
    queryBuilder.withTSUIDS(tsuids);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate( false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertEquals(1, dps.length);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));
    assertEquals(42, dps[0].longValue(0));
    assertEquals(1, dps[0].getAnnotations().size());
    assertEquals(DESCRIPTION, dps[0].getAnnotations().get(0).getDescription());
  }

  @Test
  public void runTSUIDQuery() throws Exception {
    storeLongTimeSeriesSeconds(true, false);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add(TSUID_1);
    queryBuilder.withTSUIDS(tsuids);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate( false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].aggregatedSize());
  }

  @Test
  public void runTSUIDsAggSum() throws Exception {
    storeLongTimeSeriesSeconds(true, false);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add(TSUID_1);
    tsuids.add(TSUID_2);
    queryBuilder.withTSUIDS(tsuids);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate( false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    for (DataPoint dp : dps[0]) {
      assertEquals(301, dp.longValue());
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runTSUIDQueryNoData() throws Exception {
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add(TSUID_1);
    queryBuilder.withTSUIDS(tsuids);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate( false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertEquals(0, dps.length);
  }

  @Test
  public void runTSUIDQueryNoDataForTSUID() throws Exception {
    // this doesn't throw an exception since the UIDs are only looked for when
    // the queryBuilder completes.
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add(TSUID_3);
    queryBuilder.withTSUIDS(tsuids);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate( false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertEquals(0, dps.length);
  }

  @Test (expected = NoSuchUniqueId.class)

  public void runTSUIDQueryNSU() throws Exception {
    storeLongTimeSeriesSeconds(true, false);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add(TSUID_1);
    queryBuilder.withTSUIDS(tsuids);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate( false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    dps[0].metric();
  }

  @Test
  public void runRateCounterDefault() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    long timestamp = START_TIME_1;
    tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, Long.MAX_VALUE - 55, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, Long.MAX_VALUE - 25, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, 5, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    RateOptions ro = new RateOptions(true, Long.MAX_VALUE, 0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(true, ro);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    for (DataPoint dp : dps[0]) {
      assertEquals(1.0, dp.doubleValue(), 0.001);
    }
    assertEquals(2, dps[0].size());
  }

  @Test
  public void runRateCounterDefaultNoOp() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    long timestamp = START_TIME_1;
    tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, 30, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, 60, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, 90, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    RateOptions ro = new RateOptions(true, Long.MAX_VALUE, 0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(true, ro);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    for (DataPoint dp : dps[0]) {
      assertEquals(1.0, dp.doubleValue(), 0.001);
    }
    assertEquals(2, dps[0].size());
  }

  @Test
  public void runRateCounterMaxSet() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    long timestamp = START_TIME_1;
    tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, 45, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, 75, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, 5, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    RateOptions ro = new RateOptions(true, 100, 0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(true, ro);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    for (DataPoint dp : dps[0]) {
      assertEquals(1.0, dp.doubleValue(), 0.001);
    }
    assertEquals(2, dps[0].size());
  }

  @Test
  public void runRateCounterAnomally() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    long timestamp = START_TIME_1;
    tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, 45, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, 75, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, 25, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    RateOptions ro = new RateOptions(true, 10000, 35);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(true, ro);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertEquals(1.0, dps[0].doubleValue(0), 0.001);
    assertEquals(0, dps[0].doubleValue(1), 0.001);
    assertEquals(2, dps[0].size());
  }

  @Test
  public void runMultiCompact() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(1L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(2L);

    // 2nd compaction
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(3L);
    final byte[] qual4 = { 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(4L);

    // 3rd compaction
    final byte[] qual5 = { 0x00, 0x57 };
    final byte[] val5 = Bytes.fromLong(5L);
    final byte[] qual6 = { 0x00, 0x67 };
    final byte[] val6 = Bytes.fromLong(6L);

    final byte[] KEY = { 0, 0, 1, 0x50, (byte) 0xE2,
            0x27, 0x00, 0, 0, 1, 0, 0, 1 };

    tsdb_store.addColumn(KEY,
            MockBase.concatByteArrays(qual1, qual2),
            MockBase.concatByteArrays(val1, val2, new byte[]{0}));
    tsdb_store.addColumn(KEY,
            MockBase.concatByteArrays(qual3, qual4),
            MockBase.concatByteArrays(val3, val4, new byte[]{0}));
    tsdb_store.addColumn(KEY,
            MockBase.concatByteArrays(qual5, qual6),
            MockBase.concatByteArrays(val5, val6, new byte[]{0}));

    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);

    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(6, dps[0].aggregatedSize());
  }

  @Test
  public void runMultiCompactAndSingles() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(1L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(2L);

    // 2nd compaction
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(3L);
    final byte[] qual4 = { 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(4L);

    // 3rd compaction
    final byte[] qual5 = { 0x00, 0x57 };
    final byte[] val5 = Bytes.fromLong(5L);
    final byte[] qual6 = { 0x00, 0x67 };
    final byte[] val6 = Bytes.fromLong(6L);

    final byte[] KEY = { 0, 0, 1, 0x50, (byte) 0xE2,
            0x27, 0x00, 0, 0, 1, 0, 0, 1 };

    tsdb_store.addColumn(KEY,
            MockBase.concatByteArrays(qual1, qual2),
            MockBase.concatByteArrays(val1, val2, new byte[]{0}));
    tsdb_store.addColumn(KEY, qual3, val3);
    tsdb_store.addColumn(KEY, qual4, val4);
    tsdb_store.addColumn(KEY,
            MockBase.concatByteArrays(qual5, qual6),
            MockBase.concatByteArrays(val5, val6, new byte[]{0}));

    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);

    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertTrue(dps[0].aggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertArrayEquals(WEB01_ID, dps[0].tags().get(HOST_ID));

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(6, dps[0].aggregatedSize());
  }

  @Test
  public void runInterpolationSeconds() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    long timestamp = START_TIME_1;
    for (int i = 1; i <= 300; i++) {
      tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }

    tags.clear();
    tags.put(HOST, WEB_02);
    timestamp = TIMESTAMP_5;
    for (int i = 300; i > 0; i--) {
      tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }

    tags.clear();
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long v = 1;
    long ts = TIMESTAMP_1L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.longValue());

      if (dp.timestamp() == TIMESTAMP_2L) {
        v = 1;
      } else if (v == 1 || v == 302) {
        v = 301;
      } else {
        v = 302;
      }
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runInterpolationMs() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    long timestamp = START_TIME_1000L;
    for (int i = 1; i <= 300; i++) {
      tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 500, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }

    tags.clear();
    tags.put(HOST, WEB_02);
    timestamp = START_TIME_1250L;
    for (int i = 300; i > 0; i--) {
      tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 500, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }

    tags.clear();
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long v = 1;
    long ts = START_TIME_1500L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 250;
      assertEquals(v, dp.longValue());

      if (dp.timestamp() == TIMESTAMP_3L) {
        v = 1;
      } else if (v == 1 || v == 302) {
        v = 301;
      } else {
        v = 302;
      }
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runInterpolationMsDownsampled() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    // ts = START_TIME_1500, v = 1
    // ts = 1356998401000, v = 2
    // ts = 1356998401500, v = 3
    // ts = 1356998402000, v = 4
    // ts = 1356998402500, v = 5
    // ...
    // ts = 1356998449000, v = 98
    // ts = 1356998449500, v = 99
    // ts = 1356998450000, v = 100
    // ts = 1356998455000, v = 101
    // ts = 1356998460000, v = 102
    // ...
    // ts = 1356998550000, v = 120
    long timestamp = START_TIME_1000L;
    for (int i = 1; i <= 120; i++) {
      timestamp += i <= 100 ? 500 : 5000;
      tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }

    // ts = START_TIME_1750, v = 300
    // ts = 1356998401250, v = 299
    // ts = 1356998401750, v = 298
    // ts = 1356998402250, v = 297
    // ts = 1356998402750, v = 296
    // ...
    // ts = 1356998549250, v = 3
    // ts = 1356998549750, v = 2
    // ts = 1356998550250, v = 1
    tags.clear();
    tags.put(HOST, WEB_02);
    timestamp = START_TIME_1250L;
    for (int i = 300; i > 0; i--) {
      tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 500, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }

    tags.clear();
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.SUM);
    queryBuilder.shouldCalculateRate(false);
    queryBuilder.downsample(1000, Aggregators.SUM);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    // TS1 in intervals = (1), (2,3), (4,5) ... (98,99), 100, (), (), (), (),
    //                    (101), ... (120);
    // TS2 in intervals = (300), (299,298), (297,296), ... (203, 202) ...
    //                    (3,2), (1);
    // TS1 downsample = 1, 5, 9, ... 197, 100, _, _, _, _, 101, ... 120
    // TS1 interpolation = 1, 5, ... 197, 100, 100.2, 100.4, 100.6, 100.8, 101,
    //                     ... 119.6, 119.8, 120
    // TS2 downsample = 300, 597, 593, ... 405, 401, ... 5, 1
    // TS1 + TS2 = 301, 602, 602, ... 501, 497.2, ... 124.8, 121
    int i = 0;
    long ts = START_TIME_1000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 1000;
      if (i == 0) {
        assertEquals(301, dp.doubleValue(), 0.0000001);
      } else if (i < 50) {
        // TS1 = i * 2 + i * 2 + 1
        // TS2 = (300 - i * 2 + 1) + (300 - i * 2);
        // TS1 + TS2 = 602
        assertEquals(602, dp.doubleValue(), 0.0000001);
      } else {
        // TS1 = 100 + (i - 50) * 0.2
        // TS2 = (300 - i * 2 + 1) + (300 - i * 2);
        // TS1 + TS2 = 701 + (i - 50) * 0.2 - i * 4
        double value = 701 + (i - 50) * 0.2 - i * 4;
        assertEquals(value, dp.doubleValue(), 0.0000001);
      }
      ++i;
    }
    assertEquals(151, dps[0].size());
  }

  //---------------------- //
  // Aggregator unit tests //
  // --------------------- //

  @Test
  public void runZimSum() throws Exception {
    storeLongTimeSeriesSeconds(false, false);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.ZIMSUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long ts = TIMESTAMP_1L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(301, dp.longValue());
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runZimSumFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.ZIMSUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long ts = TIMESTAMP_1L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(76.25, dp.doubleValue(), 0.001);
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runZimSumOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.ZIMSUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long v1 = 1;
    long v2 = 300;
    long ts = TIMESTAMP_1L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;

      if (counter % 2 == 0) {
        assertEquals(v1, dp.longValue());
        v1++;
      } else {
        assertEquals(v2, dp.longValue());
        v2--;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runZimSumFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.ZIMSUM);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    double v1 = 1.25;
    double v2 = 75.0;
    long ts = TIMESTAMP_1L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      if (counter % 2 == 0) {
        assertEquals(v1, dp.doubleValue(), 0.001);
        v1 += 0.25;
      } else {
        assertEquals(v2, dp.doubleValue(), 0.001);
        v2 -= 0.25;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runMin() throws Exception {
    storeLongTimeSeriesSeconds(false, false);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MIN);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long v = 1;
    long ts = TIMESTAMP_1L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());

      if (decrement) {
        v--;
      } else {
        v++;
      }

      if (v == 151){
        v = 150;
        decrement = true;
      }
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMinFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MIN);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    double v = 1.25;
    long ts = TIMESTAMP_1L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.0001);

      if (decrement) {
        v -= .25;
      } else {
        v += .25;
      }

      if (v > 38){
        v = 38.0;
        decrement = true;
      }
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMinOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MIN);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long v = 1;
    long ts = TIMESTAMP_1L;
    int counter = 0;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.longValue());
      if (counter % 2 != 0) {
        if (decrement) {
          v--;
        } else {
          v++;
        }
      } else if (v == 151){
        v = 150;
        decrement = true;
        counter--; // hack since the hump is 150 150 151 150 150
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runMinFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MIN);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    double v = 1.25;
    long ts = TIMESTAMP_1L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.doubleValue(), 0.001);
      if (decrement) {
        v -= 0.125;
      } else {
        v += 0.125;
      }

      if (v > 38.125){
        v = 38.125;
        decrement = true;
      }
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runMax() throws Exception {
    storeLongTimeSeriesSeconds(false, false);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MAX);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long v = 300;
    long ts = TIMESTAMP_1L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());

      if (decrement) {
        v--;
      } else {
        v++;
      }

      if (v == 150){
        v = 151;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMaxFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MAX);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    double v = 75.0;
    long ts = TIMESTAMP_1L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.001);

      if (decrement) {
        v -= .25;
      } else {
        v += .25;
      }

      if (v < 38.25){
        v = 38.25;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMaxOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MAX);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long v = 1;
    long ts = TIMESTAMP_1L;
    int counter = 0;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.longValue());
      if (v == 1) {
        v = 300;
      } else if (dp.timestamp() == TIMESTAMP_4L) {
        v = 1;
      } else if (counter % 2 == 0) {
        if (decrement) {
          v--;
        } else {
          v++;
        }
      }

      if (v == 150){
        v = 151;
        decrement = false;
        counter--; // hack since the hump is 151 151 151
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runMaxFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MAX);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    double v = 1.25;
    long ts = TIMESTAMP_1L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.doubleValue(), .0001);
      if (v == 1.25) {
        v = 75.0;
      } else if (dp.timestamp() == TIMESTAMP_4L) {
        v = 0.25;
      } else {
        if (decrement) {
          v -= .125;
        } else {
          v += .125;
        }

        if (v < 38.25){
          v = 38.25;
          decrement = false;
        }
      }
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runAvg() throws Exception {
    storeLongTimeSeriesSeconds(false, false);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.AVG);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long ts = TIMESTAMP_1L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(150, dp.longValue());
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runAvgFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.AVG);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long ts = TIMESTAMP_1L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(38.125, dp.doubleValue(), 0.001);
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runAvgOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.AVG);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long v = 1;
    long ts = TIMESTAMP_1L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.longValue());
      if (v == 1) {
        v = 150;
      } else if (dp.timestamp() == TIMESTAMP_4L) {
        v = 1;
      } else if (v == 150) {
        v = 151;
      } else {
        v = 150;
      }
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runAvgFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.AVG);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    double v = 1.25;
    long ts = TIMESTAMP_1L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.doubleValue(), 0.0001);
      if (v == 1.25) {
        v = 38.1875;
      } else if (dp.timestamp() == TIMESTAMP_4L) {
        v = .25;
      }
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runDev() throws Exception {
    storeLongTimeSeriesSeconds(false, false);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.DEV);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long v = 149;
    long ts = TIMESTAMP_1L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());

      if (decrement) {
        v--;
      } else {
        v++;
      }

      if (v < 0){
        v = 0;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runDevFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.DEV);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    double v = 36.875;
    long ts = TIMESTAMP_1L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.001);

      if (decrement) {
        v -= 0.25;
      } else {
        v += 0.25;
      }

      if (v < 0.125){
        v = 0.125;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runDevOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.DEV);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long v = 0;
    long ts = TIMESTAMP_1L;
    int counter = 0;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.longValue());
      if (dp.timestamp() == TIMESTAMP_1L) {
        v = 149;
      } else if (dp.timestamp() == TIMESTAMP_4L) {
        v = 0;
      } else if (counter % 2 == 0) {
        if (decrement) {
          v--;
        } else {
          v++;
        }
        if (v < 0) {
          v = 0;
          decrement = false;
          counter++;
        }
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runDevFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.DEV);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    double v = 0;
    long ts = TIMESTAMP_1L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.doubleValue(), 0.0001);
      if (dp.timestamp() == TIMESTAMP_1L) {
        v = 36.8125;
      } else if (dp.timestamp() == TIMESTAMP_4L) {
        v = 0;
      } else {
        if (decrement) {
          v -= 0.125;
        } else {
          v += 0.125;
        }
        if (v < 0.0625) {
          v = 0.0625;
          decrement = false;
        }
      }
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runMimMin() throws Exception {
    storeLongTimeSeriesSeconds(false, false);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MIMMIN);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long v = 1;
    long ts = TIMESTAMP_1L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());

      if (decrement) {
        v--;
      } else {
        v++;
      }

      if (v == 151){
        v = 150;
        decrement = true;
      }
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMimMinOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MIMMIN);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long v1 = 1;
    long v2 = 300;
    long ts = TIMESTAMP_1L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;

      if (counter % 2 == 0) {
        assertEquals(v1, dp.longValue());
        v1++;
      } else {
        assertEquals(v2, dp.longValue());
        v2--;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runMimMinFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MIMMIN);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    double v = 1.25;
    long ts = TIMESTAMP_1L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.0001);

      if (decrement) {
        v -= .25;
      } else {
        v += .25;
      }

      if (v > 38){
        v = 38.0;
        decrement = true;
      }
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMimMinFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MIMMIN);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    double v1 = 1.25;
    double v2 = 75.0;
    long ts = TIMESTAMP_1L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      if (counter % 2 == 0) {
        assertEquals(v1, dp.doubleValue(), 0.001);
        v1 += 0.25;
      } else {
        assertEquals(v2, dp.doubleValue(), 0.001);
        v2 -= 0.25;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runMimMax() throws Exception {
    storeLongTimeSeriesSeconds(false, false);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MIMMAX);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long v = 300;
    long ts = TIMESTAMP_1L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());

      if (decrement) {
        v--;
      } else {
        v++;
      }

      if (v == 150){
        v = 151;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMimMaxFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MIMMAX);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    double v = 75.0;
    long ts = TIMESTAMP_1L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.001);

      if (decrement) {
        v -= .25;
      } else {
        v += .25;
      }

      if (v < 38.25){
        v = 38.25;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMimMaxOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MIMMAX);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    long v1 = 1;
    long v2 = 300;
    long ts = TIMESTAMP_1L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;

      if (counter % 2 == 0) {
        assertEquals(v1, dp.longValue());
        v1++;
      } else {
        assertEquals(v2, dp.longValue());
        v2--;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runMimMaxFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);

    HashMap<String, String> tags = new HashMap<String, String>(0);
    queryBuilder.withStartAndEndTime(START_TIME_1,END_TIME_1);
    queryBuilder.withMetric(METRIC_1);
    queryBuilder.withTags(tags);
    queryBuilder.withAggregator(Aggregators.MIMMAX);
    queryBuilder.shouldCalculateRate(false);
    final DataPoints[] dps = tsdb.getDataPointsClient().executeQuery(queryBuilder.createQuery().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(dps);
    assertArrayEquals(SYS_CPU_USER_ID, dps[0].metric());
    assertArrayEquals(HOST_ID, dps[0].aggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].tags().isEmpty());

    double v1 = 1.25;
    double v2 = 75.0;
    long ts = TIMESTAMP_1L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      if (counter % 2 == 0) {
        assertEquals(v1, dp.doubleValue(), 0.001);
        v1 += 0.25;
      } else {
        assertEquals(v2, dp.doubleValue(), 0.001);
        v2 -= 0.25;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }

  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private void storeLongTimeSeriesSeconds(final boolean two_metrics,
                                          final boolean offset) throws Exception {
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    long timestamp = START_TIME_1;
    for (int i = 1; i <= 300; i++) {
      tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      if (two_metrics) {
        tsdb.getDataPointsClient().addPoint(METRIC_2, timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      }
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put(HOST, WEB_02);
    timestamp = offset ? TIMESTAMP_5 : START_TIME_1;
    for (int i = 300; i > 0; i--) {
      tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      if (two_metrics) {
        tsdb.getDataPointsClient().addPoint(METRIC_2, timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      }
    }
  }

  private void storeLongTimeSeriesMs() throws Exception {
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    long timestamp = START_TIME_1000L;
    for (int i = 1; i <= 300; i++) {
      tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 500, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      tsdb.getDataPointsClient().addPoint(METRIC_2, timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put(HOST, WEB_02);
    timestamp = START_TIME_1000L;
    for (int i = 300; i > 0; i--) {
      tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 500, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      tsdb.getDataPointsClient().addPoint(METRIC_2, timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }
  }

  private void storeFloatTimeSeriesSeconds(final boolean two_metrics,
                                           final boolean offset) throws Exception {
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    long timestamp = START_TIME_1;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      if (two_metrics) {
        tsdb.getDataPointsClient().addPoint(METRIC_2, timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      }
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put(HOST, WEB_02);
    timestamp = offset ? TIMESTAMP_5 : START_TIME_1;
    for (float i = 75F; i > 0; i -= 0.25F) {
      tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      if (two_metrics) {
        tsdb.getDataPointsClient().addPoint(METRIC_2, timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      }
    }
  }

  private void storeFloatTimeSeriesMs() throws Exception {
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    long timestamp = START_TIME_1000L;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 500, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      tsdb.getDataPointsClient().addPoint(METRIC_2, timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put(HOST, WEB_02);
    timestamp = START_TIME_1000L;
    for (float i = 75F; i > 0; i -= 0.25F) {
      tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 500, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      tsdb.getDataPointsClient().addPoint(METRIC_2, timestamp, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }
  }

  private void storeMixedTimeSeriesSeconds() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    long timestamp = START_TIME_1;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      if (i % 2 == 0) {
        tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, (long) i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      } else {
        tsdb.getDataPointsClient().addPoint(METRIC_1, timestamp += 30, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      }
    }
  }

  // dumps ints, floats, seconds and ms
  private void storeMixedTimeSeriesMsAndS() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(HOST, WEB_01);
    long timestamp = START_TIME_1000L;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      long ts = timestamp += 500;
      if (ts % 1000 == 0) {
        ts /= 1000;
      }
      if (i % 2 == 0) {
        tsdb.getDataPointsClient().addPoint(METRIC_1, ts, (long) i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      } else {
        tsdb.getDataPointsClient().addPoint(METRIC_1, ts, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
      }
    }
  }

  private void storeLongCompactions() throws Exception {
    long base_timestamp = START_TIME_1;
    long value = 1;
    byte[] qualifier = new byte[119 * 2];
    long timestamp = 1356998430;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column =
              Bytes.fromShort((short)(offset << Const.FLAG_BITS | 0x7));
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
    }

    byte[] column_qualifier = new byte[119 * 8];
    for (int index = 0; index < column_qualifier.length; index += 8) {
      System.arraycopy(Bytes.fromLong(value), 0, column_qualifier, index, 8);
      value++;
    }
    tsdb_store.addColumn(MockBase.stringToBytes(E22700000001000001),
            qualifier, column_qualifier);

    base_timestamp = 1357002000;
    qualifier = new byte[120 * 2];
    timestamp = 1357002000;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column =
              Bytes.fromShort((short)(offset << Const.FLAG_BITS | 0x7));
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
    }

    column_qualifier = new byte[120 * 8];
    for (int index = 0; index < column_qualifier.length; index += 8) {
      System.arraycopy(Bytes.fromLong(value), 0, column_qualifier, index, 8);
      value++;
    }
    tsdb_store.addColumn(MockBase.stringToBytes(E23510000001000001),
            qualifier, column_qualifier);

    base_timestamp = 1357005600;
    qualifier = new byte[61 * 2];
    timestamp = 1357005600;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column =
              Bytes.fromShort((short)(offset << Const.FLAG_BITS | 0x7));
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
    }

    column_qualifier = new byte[61 * 8];
    for (int index = 0; index < column_qualifier.length; index += 8) {
      System.arraycopy(Bytes.fromLong(value), 0, column_qualifier, index, 8);
      value++;
    }
    tsdb_store.addColumn(MockBase.stringToBytes(E24320000001000001),
            qualifier, column_qualifier);
  }

  private void storeFloatCompactions() throws Exception {
    long base_timestamp = START_TIME_1;
    float value = 1.25F;
    byte[] qualifier = new byte[119 * 2];
    long timestamp = 1356998430;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column =
              Bytes.fromShort((short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
    }

    byte[] column_qualifier = new byte[119 * 4];
    for (int index = 0; index < column_qualifier.length; index += 4) {
      System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0,
              column_qualifier, index, 4);
      value += 0.25F;
    }
    tsdb_store.addColumn(MockBase.stringToBytes(E22700000001000001),
            qualifier, column_qualifier);

    base_timestamp = 1357002000;
    qualifier = new byte[120 * 2];
    timestamp = 1357002000;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column =
              Bytes.fromShort((short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
    }

    column_qualifier = new byte[120 * 4];
    for (int index = 0; index < column_qualifier.length; index += 4) {
      System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0,
              column_qualifier, index, 4);
      value += 0.25F;
    }
    tsdb_store.addColumn(MockBase.stringToBytes(E23510000001000001),
            qualifier, column_qualifier);

    base_timestamp = 1357005600;
    qualifier = new byte[61 * 2];
    timestamp = 1357005600;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column =
              Bytes.fromShort((short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
    }

    column_qualifier = new byte[61 * 4];
    for (int index = 0; index < column_qualifier.length; index += 4) {
      System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0,
              column_qualifier, index, 4);
      value += 0.25F;
    }
    tsdb_store.addColumn(MockBase.stringToBytes(E24320000001000001),
            qualifier, column_qualifier);
  }

  private void storeMixedCompactions() throws Exception {
    long base_timestamp = START_TIME_1;
    float q_counter = 1.25F;
    byte[] qualifier = new byte[119 * 2];
    long timestamp = 1356998430;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column;
      if (q_counter % 1 == 0) {
        column = Bytes.fromShort((short)(offset << Const.FLAG_BITS | 0x7));
      } else {
        column = Bytes.fromShort(
                (short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
      }
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
      q_counter += 0.25F;
    }

    float value = 1.25F;
    int num = 119;
    byte[] column_qualifier = new byte[((num / 4) * 8) + ((num - (num / 4)) * 4)];
    int idx = 0;
    while (idx < column_qualifier.length) {
      if (value % 1 == 0) {
        System.arraycopy(Bytes.fromLong((long)value), 0, column_qualifier, idx, 8);
        idx += 8;
      } else {
        System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0,
                column_qualifier, idx, 4);
        idx += 4;
      }
      value += 0.25F;
    }
    tsdb_store.addColumn(MockBase.stringToBytes(E22700000001000001),
            qualifier, column_qualifier);

    base_timestamp = 1357002000;
    qualifier = new byte[120 * 2];
    timestamp = 1357002000;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column;
      if (q_counter % 1 == 0) {
        column = Bytes.fromShort((short)(offset << Const.FLAG_BITS | 0x7));
      } else {
        column = Bytes.fromShort(
                (short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
      }
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
      q_counter += 0.25F;
    }

    num = 120;
    column_qualifier = new byte[((num / 4) * 8) + ((num - (num / 4)) * 4)];
    idx = 0;
    while (idx < column_qualifier.length) {
      if (value % 1 == 0) {
        System.arraycopy(Bytes.fromLong((long)value), 0, column_qualifier, idx, 8);
        idx += 8;
      } else {
        System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0,
                column_qualifier, idx, 4);
        idx += 4;
      }
      value += 0.25F;
    }
    tsdb_store.addColumn(MockBase.stringToBytes(E23510000001000001),
            qualifier, column_qualifier);

    base_timestamp = 1357005600;
    qualifier = new byte[61 * 2];
    timestamp = 1357005600;
    for (int index = 0; index < qualifier.length; index += 2) {
      final int offset = (int) (timestamp - base_timestamp);
      final byte[] column;
      if (q_counter % 1 == 0) {
        column = Bytes.fromShort((short)(offset << Const.FLAG_BITS | 0x7));
      } else {
        column = Bytes.fromShort(
                (short)(offset << Const.FLAG_BITS | Const.FLAG_FLOAT | 0x3));
      }
      System.arraycopy(column, 0, qualifier, index, 2);
      timestamp += 30;
      q_counter += 0.25F;
    }

    num = 61;
    column_qualifier =
            new byte[(((num / 4) + 1) * 8) + ((num - ((num / 4) + 1)) * 4)];
    idx = 0;
    while (idx < column_qualifier.length) {
      if (value % 1 == 0) {
        System.arraycopy(Bytes.fromLong((long)value), 0, column_qualifier, idx, 8);
        idx += 8;
      } else {
        System.arraycopy(Bytes.fromInt(Float.floatToRawIntBits(value)), 0,
                column_qualifier, idx, 4);
        idx += 4;
      }
      value += 0.25F;
    }
    tsdb_store.addColumn(MockBase.stringToBytes(E24320000001000001),
            qualifier, column_qualifier);
  }
}
