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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import org.hbase.async.Bytes;
import org.hbase.async.FilterList;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.MockBase.MockScanner;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.utils.Config;

/**
 * An integration test class that makes sure our query path is up to snuff.
 * This class should have tests for different data point types, rates,
 * compactions, etc. Other files can cover salting, aggregation and downsampling.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ Scanner.class })
public class TestTsdbQueryQueries extends BaseTsdbTest {
  protected TsdbQuery query = null;

  @Before
  public void beforeLocal() throws Exception {
    query = new TsdbQuery(tsdb);
  }

  @Test
  public void runLongSingleTS() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    int value = 1;
    long timestamp = 1356998430000L;
    verify(tag_values, times(1)).getNameAsync(TAGV_BYTES);
    verify(tag_values, never()).getNameAsync(TAGV_B_BYTES);
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      value++;
      timestamp += 30000;
    }
    assertEquals(300, dps[0].aggregatedSize());
  }

  @Test
  public void runLongSingleTSMs() throws Exception {
    storeLongTimeSeriesMs();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    int value = 1;
    long timestamp = 1356998400500L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      value++;
      timestamp += 500;
    }
    assertEquals(300, dps[0].aggregatedSize());
  }

  @Test
  public void runLongSingleTSNoData() throws Exception {
    setDataPointStorage();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals(0, dps.length);
  }

  @Test
  public void runLongTwoAggSum() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    tags.clear();
    query.setStartTime(1356998400L);
    query.setEndTime(1357041600L);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);

    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(301, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      timestamp += 30000;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runLongTwoAggSumMs() throws Exception {
    storeLongTimeSeriesMs();

    tags.clear();
    query.setStartTime(1356998400L);
    query.setEndTime(1357041600L);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);

    long timestamp = 1356998400500L;
    for (DataPoint dp : dps[0]) {
      assertEquals(301, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      timestamp += 500;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runLongTwoGroup() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    tags.clear();
    tags.put(TAGK_STRING , "*");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);
    assertMeta(dps, 1, false);
    assertEquals(2, dps.length);

    int value = 1;
    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      value++;
      timestamp += 30000;
    }
    assertEquals(300, dps[0].size());

    value = 300;
    timestamp = 1356998430000L;
    for (DataPoint dp : dps[1]) {
      assertEquals(value, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      value--;
      timestamp += 30000;
    }
    assertEquals(300, dps[1].size());
  }

  @Test
  public void runLongSingleTSRate() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, true);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    long timestamp = 1356998460000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(0.033F, dp.doubleValue(), 0.001);
      assertEquals(timestamp, dp.timestamp());
      timestamp += 30000;
    }
    assertEquals(299, dps[0].size());
  }

  @Test
  public void runLongSingleTSRateMs() throws Exception {
    storeLongTimeSeriesMs();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, true);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    long timestamp = 1356998401000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(2.0F, dp.doubleValue(), 0.001);
      assertEquals(timestamp, dp.timestamp());
      timestamp += 500;
    }
    assertEquals(299, dps[0].size());
  }

  @Test
  public void runFloatSingleTS() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    double value = 1.25D;
    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0.001);
      assertEquals(timestamp, dp.timestamp());
      value += 0.25D;
      timestamp += 30000;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runFloatSingleTSMs() throws Exception {
    storeFloatTimeSeriesMs();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    double value = 1.25D;
    long timestamp = 1356998400500L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0.001);
      assertEquals(timestamp, dp.timestamp());
      value += 0.25D;
      timestamp += 500;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runFloatTwoAggSum() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);

    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);

    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(76.25, dp.doubleValue(), 0.00001);
      assertEquals(timestamp, dp.timestamp());
      timestamp += 30000;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runFloatTwoAggNoneAgg() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);

    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.NONE, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);
    assertMeta(dps, 1, false);
    assertEquals(2, dps.length);

    double value = 1.25D;
    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0.0001);
      assertEquals(timestamp, dp.timestamp());
      value += 0.25D;
      timestamp += 30000;
    }
    assertEquals(300, dps[0].size());

    value = 75D;
    timestamp = 1356998430000L;
    for (DataPoint dp : dps[1]) {
      assertEquals(value, dp.doubleValue(), 0.0001);
      assertEquals(timestamp, dp.timestamp());
      value -= 0.25d;
      timestamp += 30000;
    }
    assertEquals(300, dps[1].size());
  }

  @Test
  public void runFloatTwoAggSumMs() throws Exception {
    storeFloatTimeSeriesMs();

    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);

    long timestamp = 1356998400500L;
    for (DataPoint dp : dps[0]) {
      assertEquals(76.25, dp.doubleValue(), 0.00001);
      assertEquals(timestamp, dp.timestamp());
      timestamp += 500;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runFloatTwoGroup() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(TAGK_STRING , "*");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);
    assertMeta(dps, 1, false);
    assertEquals(2, dps.length);

    double value = 1.25D;
    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0.0001);
      assertEquals(timestamp, dp.timestamp());
      value += 0.25D;
      timestamp += 30000;
    }
    assertEquals(300, dps[0].size());

    value = 75D;
    timestamp = 1356998430000L;
    for (DataPoint dp : dps[1]) {
      assertEquals(value, dp.doubleValue(), 0.0001);
      assertEquals(timestamp, dp.timestamp());
      value -= 0.25d;
      timestamp += 30000;
    }
    assertEquals(300, dps[1].size());
  }

  @Test
  public void runFloatSingleTSRate() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, true);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    long timestamp = 1356998460000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(0.00833F, dp.doubleValue(), 0.00001);
      assertEquals(timestamp, dp.timestamp());
      timestamp += 30000;
    }
    assertEquals(299, dps[0].size());
  }

  @Test
  public void runFloatSingleTSRateMs() throws Exception {
    storeFloatTimeSeriesMs();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, true);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    long timestamp = 1356998401000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(0.5F, dp.doubleValue(), 0.00001);
      assertEquals(timestamp, dp.timestamp());
      timestamp += 500;
    }
    assertEquals(299, dps[0].size());
  }

  @Test
  public void runFloatSingleTSCompacted() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);
    storage.tsdbCompactAllRows();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    long timestamp = 1356998430000L;
    double value = 1.25D;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.doubleValue(), 0.001);
      assertEquals(timestamp, dp.timestamp());
      value += 0.25D;
      timestamp += 30000;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMixedSingleTS() throws Exception {
    storeMixedTimeSeriesSeconds();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.AVG, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    long timestamp = 1356998430000L;
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
      assertEquals(timestamp, dp.timestamp());
      timestamp += 30000;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMixedSingleTSMsAndS() throws Exception {
    storeMixedTimeSeriesMsAndS();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.AVG, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    long timestamp = 1356998400500L;
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
      assertEquals(timestamp, dp.timestamp());
      timestamp += 500;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runMixedSingleTSPostCompaction() throws Exception {
    storeMixedTimeSeriesSeconds();

    final Field compact = Config.class.getDeclaredField("enable_compactions");
    compact.setAccessible(true);
    compact.set(config, true);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.AVG, false);
    assertNotNull(query.run());

    // this should only compact the rows for the time series that we fetched and
    // leave the others alone

    final byte[] key =
        IncomingDataPoints.rowKeyTemplate(tsdb, METRIC_STRING, tags);
    RowKey.prefixKeyWithSalt(key);
    System.arraycopy(Bytes.fromInt(1356998400), 0, key,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    assertEquals(1, storage.numColumns(key));
    System.arraycopy(Bytes.fromInt(1357002000), 0, key,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    assertEquals(1, storage.numColumns(key));
    System.arraycopy(Bytes.fromInt(1357005600), 0, key,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    assertEquals(1, storage.numColumns(key));

    // run it again to verify the compacted data uncompacts properly
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    long timestamp = 1356998430000L;
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
      assertEquals(timestamp, dp.timestamp());
      timestamp += 30000;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runEndTime() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357001900);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    int value = 1;
    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      value++;
      timestamp += 30000;
    }
    assertEquals(119, dps[0].size());
  }

  @Test
  public void runCompactPostQuery() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    final Field compact = Config.class.getDeclaredField("enable_compactions");
    compact.setAccessible(true);
    compact.set(config, true);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    // this should only compact the rows for the time series that we fetched and
    // leave the others alone
    final byte[] key_a =
        IncomingDataPoints.rowKeyTemplate(tsdb, METRIC_STRING, tags);
    RowKey.prefixKeyWithSalt(key_a);
    final Map<String, String> tags_copy = new HashMap<String, String>(tags);
    tags_copy.put(TAGK_STRING, TAGV_B_STRING);
    final byte[] key_b =
        IncomingDataPoints.rowKeyTemplate(tsdb, METRIC_STRING, tags_copy);
    RowKey.prefixKeyWithSalt(key_b);

    System.arraycopy(Bytes.fromInt(1356998400), 0, key_a,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    assertEquals(1, storage.numColumns(key_a));

    System.arraycopy(Bytes.fromInt(1356998400), 0, key_b,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    if (config.enable_appends()) {
      assertEquals(1, storage.numColumns(key_b));
    } else {
      assertEquals(119, storage.numColumns(key_b));
    }

    System.arraycopy(Bytes.fromInt(1357002000), 0, key_a,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    assertEquals(1, storage.numColumns(key_a));

    System.arraycopy(Bytes.fromInt(1357002000), 0, key_b,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    if (config.enable_appends()) {
      assertEquals(1, storage.numColumns(key_b));
    } else {
      assertEquals(120, storage.numColumns(key_b));
    }

    System.arraycopy(Bytes.fromInt(1357005600), 0, key_a,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    assertEquals(1, storage.numColumns(key_a));

    System.arraycopy(Bytes.fromInt(1357005600), 0, key_b,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    if (config.enable_appends()) {
      assertEquals(1, storage.numColumns(key_b));
    } else {
      assertEquals(61, storage.numColumns(key_b));
    }

    // run it again to verify the compacted data uncompacts properly
    dps = query.run();
    assertMeta(dps, 0, false);

    int value = 1;
    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      value++;
      timestamp += 30000;
    }
    assertEquals(300, dps[0].size());
  }

  @Test (expected = IllegalStateException.class)
  public void runStartNotSet() throws Exception {
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    query.run();
  }

  @Test
  public void runFloatAndIntSameTSNoFix() throws Exception {
    tsdb.config.overrideConfig("tsd.storage.use_otsdb_timestamp", "true");
    // if a row has an integer and a float for the same timestamp, there will be
    // two different qualifiers that will resolve to the same offset. With DTCS enabled
	// the conflicts are auto resolved i.e. either the maximum or the minimum value of
	// data is returned depending on the configuration tsd.storage.use_max_value
	// If DTCS is disabled, it will fall throw the exception.
	// DTCS -> Date Tiered Compaction Strategy (tsd.storage.use_otsdb_timestamp config parameter)

    storeLongTimeSeriesSeconds(true, false);

    tsdb.addPoint(METRIC_STRING, 1356998430, 42.5F, tags).joinUninterruptibly();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    if (config.enable_appends() || config.use_otsdb_timestamp()) {
      DataPoints[] dps = query.run();
      assertMeta(dps, 0, false, false);

      int value = 1;
      long timestamp = 1356998430000L;
      for (DataPoint dp : dps[0]) {
        if (value == 1) {
          // first value was replaced in the append
          assertEquals(42.5, dp.doubleValue(), 0.0001);
        } else {
          assertEquals(value, dp.longValue());
        }
        assertEquals(timestamp, dp.timestamp());
        value++;
        timestamp += 30000;
      }
      assertEquals(300, dps[0].size());
    }

    tsdb.config.overrideConfig("tsd.storage.use_otsdb_timestamp", "false");
    try {
        query.run();
        fail("Expected an IllegalDataException");
      } catch (IllegalDataException ide) { }

  }

  @Test
  public void runFloatAndIntSameTSFix() throws Exception {
    config.setFixDuplicates(true);
    // if a row has an integer and a float for the same timestamp, there will be
    // two different qualifiers that owill resolve to the same offset. This no
    // longer tosses an exception, and keeps the maximum of all values
    // This can also be configured via tsdb.storage.use_max_value config
    storeLongTimeSeriesSeconds(true, false);

    tsdb.addPoint(METRIC_STRING, 1356998430, 42.5F, tags).joinUninterruptibly();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    int value = 1;
    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      if (value == 1) {
        assertEquals(42.5, dp.doubleValue(), 0.001);
      } else {
        assertEquals(value, dp.longValue());
      }
      assertEquals(timestamp, dp.timestamp());
      value++;
      timestamp += 30000;
    }
    assertEquals(300, dps[0].aggregatedSize());
  }

  @Test
  public void multipleValuesAtSameTimestampShouldReturnMaxValueDefault() throws Exception {
    tsdb.config.overrideConfig("tsd.storage.use_otsdb_timestamp", "true");
    config.setFixDuplicates(true);
    // if a row has an integer and a float for the same timestamp, there will be
    // two different qualifiers that will resolve to the same offset. This no
    // longer tosses an exception, and keeps the maximum of all values
    // This can also be configured via tsdb.storage.use_max_value config
    setDataPointStorage();

    tsdb.addPoint(METRIC_STRING, 1356998430, 69755263, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998430, 62500.52F, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998430, 2533, tags).joinUninterruptibly();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(69755263, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
    }
    assertEquals(1, dps[0].aggregatedSize());
  }

  @Test
  public void multipleValuesAtSameTimestampShouldReturnMinValueIfConfigured() throws Exception {
    tsdb.config.overrideConfig("tsd.storage.use_otsdb_timestamp", "true");
	// This test explicitly configures the use_max_value as false thus when different data type values
	// are written at same timestamp, the minimum of all will be provided in output
	tsdb.getConfig().overrideConfig("tsd.storage.use_max_value", "false");
    config.setFixDuplicates(true);
    setDataPointStorage();

    tsdb.addPoint(METRIC_STRING, 1356998430, 69755263, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998430, 62500.52F, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998430, 2533, tags).joinUninterruptibly();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(2533, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
    }
    assertEquals(1, dps[0].aggregatedSize());
  }


  @Test
  public void runWithAnnotation() throws Exception {
    storeLongTimeSeriesSeconds(true, false);
    storeAnnotation(1356998490);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false, true);

    int value = 1;
    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      value++;
      timestamp += 30000;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runWithAnnotationPostCompact() throws Exception {
    storeLongTimeSeriesSeconds(true, false);
    storeAnnotation(1356998490);

    final Field compact = Config.class.getDeclaredField("enable_compactions");
    compact.setAccessible(true);
    compact.set(config, true);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    DataPoints[] dps = query.run();
    assertMeta(dps, 0, false, true);

    // this should only compact the rows for the time series that we fetched and
    // leave the others alone
    final byte[] key_a =
        IncomingDataPoints.rowKeyTemplate(tsdb, METRIC_STRING, tags);
    RowKey.prefixKeyWithSalt(key_a);
    final Map<String, String> tags_copy = new HashMap<String, String>(tags);
    tags_copy.put(TAGK_STRING, TAGV_B_STRING);
    final byte[] key_b =
        IncomingDataPoints.rowKeyTemplate(tsdb, METRIC_STRING, tags_copy);
    RowKey.prefixKeyWithSalt(key_b);

    System.arraycopy(Bytes.fromInt(1356998400), 0, key_a,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    assertEquals(2, storage.numColumns(key_a));

    System.arraycopy(Bytes.fromInt(1356998400), 0, key_b,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    if (config.enable_appends()) {
      assertEquals(1, storage.numColumns(key_b));
    } else {
      assertEquals(119, storage.numColumns(key_b));
    }

    System.arraycopy(Bytes.fromInt(1357002000), 0, key_a,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    assertEquals(1, storage.numColumns(key_a));

    System.arraycopy(Bytes.fromInt(1357002000), 0, key_b,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    if (config.enable_appends()) {
      assertEquals(1, storage.numColumns(key_b));
    } else {
      assertEquals(120, storage.numColumns(key_b));
    }

    System.arraycopy(Bytes.fromInt(1357005600), 0, key_a,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    assertEquals(1, storage.numColumns(key_a));

    System.arraycopy(Bytes.fromInt(1357005600), 0, key_b,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    if (config.enable_appends()) {
      assertEquals(1, storage.numColumns(key_b));
    } else {
      assertEquals(61, storage.numColumns(key_b));
    }

    dps = query.run();
    assertMeta(dps, 0, false, true);

    int value = 1;
    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      value++;
      timestamp += 30000;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runWithOnlyAnnotation() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    // verifies that we can pickup an annotation stored all by it's lonesome
    // in a row without any data
    final byte[] key =
        IncomingDataPoints.rowKeyTemplate(tsdb, METRIC_STRING, tags);
    RowKey.prefixKeyWithSalt(key);
    System.arraycopy(Bytes.fromInt(1357002000), 0, key,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    storage.flushRow(key);

    storeAnnotation(1357002090);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false, true);

    int value = 1;
    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      if (timestamp == 1357001970000L) {
        timestamp = 1357005600000L;
      } else {
        timestamp += 30000;
      }
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
    setDataPointStorage();

    // verifies that we can pickup an annotation stored all by it's lonesome
    // in a row without any data
    final byte[] key =
        IncomingDataPoints.rowKeyTemplate(tsdb, METRIC_STRING, tags);
    RowKey.prefixKeyWithSalt(key);
    System.arraycopy(Bytes.fromInt(1357002000), 0, key,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    storage.flushRow(key);

    storeAnnotation(1357002090);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    // TODO - apparently if you only fetch annotations, the metric and tags
    // may not be set. Check this
    //assertMeta(dps, 0, false, true);
    assertEquals(1, dps[0].getAnnotations().size());
    assertEquals(NOTE_DESCRIPTION, dps[0].getAnnotations().get(0)
        .getDescription());
    assertEquals(NOTE_NOTES, dps[0].getAnnotations().get(0).getNotes());
    assertEquals(0, dps[0].size());
  }

  @Test
  public void runSingleDataPoint() throws Exception {
    setDataPointStorage();
    long timestamp = 1356998410;
    tsdb.addPoint(METRIC_STRING, timestamp, 42, tags).joinUninterruptibly();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    storage.dumpToSystemOut();
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    assertEquals(1, dps[0].size());
    assertEquals(42, dps[0].longValue(0));
    assertEquals(1356998410000L, dps[0].timestamp(0));
  }

  @Test
  public void runSingleDataPointWithAnnotation() throws Exception {
    setDataPointStorage();
    long timestamp = 1356998410;
    tsdb.addPoint(METRIC_STRING, timestamp, 42, tags).joinUninterruptibly();

    final byte[] key =
        IncomingDataPoints.rowKeyTemplate(tsdb, METRIC_STRING, tags);
    RowKey.prefixKeyWithSalt(key);
    System.arraycopy(Bytes.fromInt(1357002000), 0, key,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    storage.flushRow(key);

    storeAnnotation(1357002090);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false, true);

    assertEquals(1, dps[0].size());
    assertEquals(42, dps[0].longValue(0));
    assertEquals(1356998410000L, dps[0].timestamp(0));
  }

  @Test
  public void runTSUIDQuery() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add("000001000001000001");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    int value = 1;
    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      value++;
      timestamp += 30000;
    }
    assertEquals(300, dps[0].aggregatedSize());
  }

  @Test
  public void runTSUIDsAggSum() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add("000001000001000001");
    tsuids.add("000001000001000002");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);

    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(301, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      timestamp += 30000;
    }
    assertEquals(300, dps[0].size());
  }

  @Test
  public void runTSUIDQueryNoData() throws Exception {
    setDataPointStorage();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);

    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add("000001000001000001");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals(0, dps.length);
  }

  @Test
  public void runTSUIDQueryNoDataForTSUID() throws Exception {
    // this doesn't throw an exception since the UIDs are only looked for when
    // the query completes.
    setDataPointStorage();

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add("000001000001000005");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals(0, dps.length);
  }

  @Test (expected = NoSuchUniqueId.class)
  public void runTSUIDQueryNSU() throws Exception {
    when(metrics.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenThrow(new NoSuchUniqueId("metrics", new byte[] { 0, 0, 1 }));
    storeLongTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    final List<String> tsuids = new ArrayList<String>(1);
    tsuids.add("000001000001000001");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    dps[0].metricName();
  }

  @Test
  public void runRateCounterDefault() throws Exception {
    setDataPointStorage();
    long timestamp = 1356998400;
    tsdb.addPoint(METRIC_STRING, timestamp += 30, Long.MAX_VALUE - 55, tags)
      .joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, timestamp += 30, Long.MAX_VALUE - 25, tags)
      .joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, timestamp += 30, 5, tags).joinUninterruptibly();

    final RateOptions ro = new RateOptions(true, Long.MAX_VALUE, 0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, true, ro);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    timestamp = 1356998460000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(1.0, dp.doubleValue(), 0.001);
      assertEquals(timestamp, dp.timestamp());
      timestamp += 30000;
    }
    assertEquals(2, dps[0].size());
  }

  @Test
  public void runRateCounterDefaultNoOp() throws Exception {
    setDataPointStorage();
    long timestamp = 1356998400;
    tsdb.addPoint(METRIC_STRING, timestamp += 30, 30, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, timestamp += 30, 60, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, timestamp += 30, 90, tags).joinUninterruptibly();

    final RateOptions ro = new RateOptions(true, Long.MAX_VALUE, 0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, true, ro);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    timestamp = 1356998460000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(1.0, dp.doubleValue(), 0.001);
      assertEquals(timestamp, dp.timestamp());
      timestamp += 30000;
    }
    assertEquals(2, dps[0].size());
  }

  @Test
  public void runRateCounterMaxSet() throws Exception {
    setDataPointStorage();
    long timestamp = 1356998400;
    tsdb.addPoint(METRIC_STRING, timestamp += 30, 45, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, timestamp += 30, 75, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, timestamp += 30, 5, tags).joinUninterruptibly();

    final RateOptions ro = new RateOptions(true, 100, 0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, true, ro);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    timestamp = 1356998460000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(1.0, dp.doubleValue(), 0.001);
      assertEquals(timestamp, dp.timestamp());
      timestamp += 30000;
    }
    assertEquals(2, dps[0].size());
  }

  @Test
  public void runRateCounterAnomally() throws Exception {
    setDataPointStorage();
    long timestamp = 1356998400;
    tsdb.addPoint(METRIC_STRING, timestamp += 30, 45, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, timestamp += 30, 75, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, timestamp += 30, 25, tags).joinUninterruptibly();

    final RateOptions ro = new RateOptions(true, 10000, 35);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, true, ro);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    assertEquals(1.0, dps[0].doubleValue(0), 0.001);
    assertEquals(1356998460000L, dps[0].timestamp(0));
    assertEquals(0, dps[0].doubleValue(1), 0.001);
    assertEquals(1356998490000L, dps[0].timestamp(1));
    assertEquals(2, dps[0].size());
  }

  @Test
  public void runRateCounterAnomallyDrop() throws Exception {
    setDataPointStorage();
    long timestamp = 1356998400;
    tsdb.addPoint(METRIC_STRING, timestamp += 30, 45, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, timestamp += 30, 75, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, timestamp += 30, 25, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, timestamp += 30, 55, tags).joinUninterruptibly();

    final RateOptions ro = new RateOptions(true, 10000, 35, true);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, true, ro);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    assertEquals(1.0, dps[0].doubleValue(0), 0.001);
    assertEquals(1356998460000L, dps[0].timestamp(0));
    assertEquals(1, dps[0].doubleValue(1), 0.001);
    assertEquals(1356998520000L, dps[0].timestamp(1));
    assertEquals(2, dps[0].size());
  }

  @Test
  public void runMultiCompact() throws Exception {
    final byte[] qual1 = { 0x00, 0x17 };
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

    final byte[] key = IncomingDataPoints.rowKeyTemplate(tsdb, METRIC_STRING, tags);
    RowKey.prefixKeyWithSalt(key);
    System.arraycopy(Bytes.fromInt(1356998400), 0, key,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);

    setDataPointStorage();
    storage.addColumn(key,
        MockBase.concatByteArrays(qual1, qual2),
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(key,
        MockBase.concatByteArrays(qual3, qual4),
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));
    storage.addColumn(key,
        MockBase.concatByteArrays(qual5, qual6),
        MockBase.concatByteArrays(val5, val6, new byte[] { 0 }));

    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(TAGK_STRING , TAGV_STRING );
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    int value = 1;
    long timestamp = 1356998401000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      value++;
      timestamp += 1000;
    }
    assertEquals(6, dps[0].aggregatedSize());
  }

  @Test
  public void runMultiCompactAndSingles() throws Exception {
    final byte[] qual1 = { 0x00, 0x17 };
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

    final byte[] key = IncomingDataPoints.rowKeyTemplate(tsdb, METRIC_STRING, tags);
    RowKey.prefixKeyWithSalt(key);
    System.arraycopy(Bytes.fromInt(1356998400), 0, key,
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);

    setDataPointStorage();
    storage.addColumn(key,
        MockBase.concatByteArrays(qual1, qual2),
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(key, qual3, val3);
    storage.addColumn(key, qual4, val4);
    storage.addColumn(key,
        MockBase.concatByteArrays(qual5, qual6),
        MockBase.concatByteArrays(val5, val6, new byte[] { 0 }));

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);

    int value = 1;
    long timestamp = 1356998401000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      value++;
      timestamp += 1000;
    }
    assertEquals(6, dps[0].aggregatedSize());
  }

  @Test
  public void runInterpolationSeconds() throws Exception {
    setDataPointStorage();
    long timestamp = 1356998400;
    for (int i = 1; i <= 300; i++) {
      tsdb.addPoint(METRIC_STRING, timestamp += 30, i, tags)
        .joinUninterruptibly();
    }
    tags.clear();
    tags.put(TAGK_STRING , TAGV_B_STRING);
    timestamp = 1356998415;
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint(METRIC_STRING, timestamp += 30, i, tags)
        .joinUninterruptibly();
    }

    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);

    long v = 1;
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.longValue());

      if (dp.timestamp() == 1357007400000L) {
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
    setDataPointStorage();
    long timestamp = 1356998400000L;
    for (int i = 1; i <= 300; i++) {
      tsdb.addPoint(METRIC_STRING, timestamp += 500, i, tags)
        .joinUninterruptibly();
    }
    tags.clear();
    tags.put(TAGK_STRING , TAGV_B_STRING );
    timestamp = 1356998400250L;
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint(METRIC_STRING, timestamp += 500, i, tags)
        .joinUninterruptibly();
    }

    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);

    long v = 1;
    long ts = 1356998400500L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 250;
      assertEquals(v, dp.longValue());

      if (dp.timestamp() == 1356998550000L) {
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
    setDataPointStorage();
    // ts = 1356998400500, v = 1
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
    long timestamp = 1356998400000L;
    for (int i = 1; i <= 120; i++) {
      timestamp += i <= 100 ? 500 : 5000;
      tsdb.addPoint(METRIC_STRING, timestamp, i, tags)
        .joinUninterruptibly();
    }

    // ts = 1356998400750, v = 300
    // ts = 1356998401250, v = 299
    // ts = 1356998401750, v = 298
    // ts = 1356998402250, v = 297
    // ts = 1356998402750, v = 296
    // ...
    // ts = 1356998549250, v = 3
    // ts = 1356998549750, v = 2
    // ts = 1356998550250, v = 1
    tags.clear();
    tags.put(TAGK_STRING , TAGV_B_STRING);
    timestamp = 1356998400250L;
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint(METRIC_STRING, timestamp += 500, i, tags)
        .joinUninterruptibly();
    }

    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    query.downsample(1000, Aggregators.SUM);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);

    // TS1 in intervals = (1), (2,3), (4,5) ... (98,99), 100, (), (), (), (),
    //                    (101), ... (120)
    // TS2 in intervals = (300), (299,298), (297,296), ... (203, 202) ...
    //                    (3,2), (1)
    // TS1 downsample = 1, 5, 9, ... 197, 100, _, _, _, _, 101, ... 120
    // TS1 interpolation = 1, 5, ... 197, 100, 100.2, 100.4, 100.6, 100.8, 101,
    //                     ... 119.6, 119.8, 120
    // TS2 downsample = 300, 597, 593, ... 405, 401, ... 5, 1
    // TS1 + TS2 = 301, 602, 602, ... 501, 497.2, ... 124.8, 121
    int i = 0;
    long ts = 1356998400000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 1000;
      if (i == 0) {
        assertEquals(301, dp.doubleValue(), 0.0000001);
      } else if (i < 50) {
        // TS1 = i * 2 + i * 2 + 1
        // TS2 = (300 - i * 2 + 1) + (300 - i * 2)
        // TS1 + TS2 = 602
        assertEquals(602, dp.doubleValue(), 0.0000001);
      } else {
        // TS1 = 100 + (i - 50) * 0.2
        // TS2 = (300 - i * 2 + 1) + (300 - i * 2)
        // TS1 + TS2 = 701 + (i - 50) * 0.2 - i * 4
        double value = 701 + (i - 50) * 0.2 - i * 4;
        assertEquals(value, dp.doubleValue(), 0.0000001);
      }
      ++i;
    }
    assertEquals(151, dps[0].size());
  }

  @Test
  public void runRegexp() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    tags.clear();
    tags.put("host", "regexp(web01)");
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, false);
    verify(tag_values, atLeast(1)).getNameAsync(TAGV_BYTES);
    verify(tag_values, atLeast(1)).getNameAsync(TAGV_B_BYTES);
    int value = 1;
    long timestamp = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      assertEquals(timestamp, dp.timestamp());
      value++;
      timestamp += 30000;
    }
    assertEquals(300, dps[0].aggregatedSize());
  }

  @Test
  public void runRegexpNoMatch() throws Exception {
    storeLongTimeSeriesSeconds(true, false);

    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    tags.clear();
    tags.put("host", "regexp(dbsvr.*)");
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();
    verify(tag_values, atLeast(1)).getNameAsync(TAGV_BYTES);
    verify(tag_values, atLeast(1)).getNameAsync(TAGV_B_BYTES);
    assertEquals(0, dps.length);
  }

  @Test
  public void runPreAggregate() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    final List<byte[]> families = new ArrayList<byte[]>();
    families.add("t".getBytes(MockBase.ASCII()));
    storage.addTable("tsdb-agg".getBytes(), families);
    setupGroupByTagValues();
    long start_timestamp = 1356998400L;
    Whitebox.setInternalState(tsdb, "agg_tag_key", 
        config.getString("tsd.rollups.agg_tag_key"));
    Whitebox.setInternalState(tsdb, "raw_agg_tag_value", 
        config.getString("tsd.rollups.raw_agg_tag_value"));
    Whitebox.setInternalState(tsdb, "default_interval", new RollupInterval("tsdb", 
        "tsdb-agg", "1m", "1h", true));
    
    tsdb.addAggregatePoint(METRIC_STRING, start_timestamp, 42L, tags, true, null, 
        null, "SUM");
    
    tags.put(config.getString("tsd.rollups.agg_tag_key"), "SUM");
    TSQuery ts_query = new TSQuery();
    ts_query.setStart("1356998400");
    ts_query.setEnd("1357041600");
    
    final TSSubQuery sub = new TSSubQuery();
    sub.setMetric(METRIC_STRING);
    sub.setTags(new HashMap<String, String>(tags));
    sub.setAggregator("sum");

    ts_query.setQueries(Arrays.asList(sub));
    ts_query.validateAndSetQuery();
    query.configureFromQuery(ts_query, 0);

    final DataPoints[] dps = query.run();
    assertEquals(1, dps.length);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals(TAGV_STRING, dps[0].getTags().get(TAGK_STRING));

    long ts = start_timestamp * 1000;
    final DataPoint dp = dps[0].iterator().next();
    assertTrue(dp.isInteger());
    assertEquals(42, dp.longValue());
    assertEquals(ts, dp.timestamp());
    assertEquals(1, dps[0].size());
  }
  
  @Test
  public void filterExplicitTagsOK() throws Exception {
    tsdb.getConfig().overrideConfig("tsd.query.enable_fuzzy", "true");
    storeLongTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setExplicitTags(true);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();

    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].aggregatedSize());
    // assert fuzzy
    for (final MockScanner scanner : storage.getScanners()) {
      assertTrue(scanner.getFilter() instanceof FilterList);
    }
  }

  @Test
  public void filterExplicitTagsGroupByOK() throws Exception {
    tsdb.getConfig().overrideConfig("tsd.query.enable_fuzzy", "true");
    storeLongTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "*");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setExplicitTags(true);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();

    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    assertEquals(300, dps[0].aggregatedSize());
    // assert fuzzy
    for (final MockScanner scanner : storage.getScanners()) {
      assertTrue(scanner.getFilter() instanceof FilterList);
    }
  }

  @Test
  public void filterExplicitTagsMissing() throws Exception {
    tsdb.getConfig().overrideConfig("tsd.query.enable_fuzzy", "true");
    when(tag_names.getIdAsync("colo"))
      .thenReturn(Deferred.fromResult(new byte[] { 0, 0, 0, 4 }));
    when(tag_values.getIdAsync("lga"))
    .thenReturn(Deferred.fromResult(new byte[] { 0, 0, 0, 4 }));
    storeLongTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tags.put("colo", "lga");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setExplicitTags(true);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);

    final DataPoints[] dps = query.run();

    assertNotNull(dps);
    assertEquals(0, dps.length);
    // assert fuzzy
    for (final MockScanner scanner : storage.getScanners()) {
      assertTrue(scanner.getFilter() instanceof FilterList);
    }
  }

}
