// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec.OffsetResolution;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HBaseClient.class })
public class TestTsdb1xQueryResult extends UTBase {
  //GMT: Monday, January 1, 2018 12:15:00 AM
  public static final int START_TS = 1514765700;
 
  // GMT: Monday, January 1, 2018 1:15:00 AM
  public static final int END_TS = 1514769300;
 
  private Tsdb1xQueryNode node;
  private Schema schema; 
  private QuerySourceConfig source_config;
  public DefaultRollupConfig rollup_config;
  public TimeSeriesQuery query;
  
  @Before
  public void before() throws Exception {
    node = mock(Tsdb1xQueryNode.class);
    schema = spy(new Schema(tsdb, null));
    source_config = mock(QuerySourceConfig.class);
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Integer.toString(START_TS))
            .setEnd(Integer.toString(END_TS))
            .setAggregator("avg"))
        .addMetric(Metric.newBuilder()
            .setMetric(METRIC_STRING))
        .build();
    
    when(node.fetchDataType(any(byte.class))).thenReturn(true);
    when(node.schema()).thenReturn(schema);
    when(node.config()).thenReturn(source_config);
    when(source_config.configuration()).thenReturn(tsdb.config);
    when(source_config.query()).thenReturn(query);
  }
  
  @Test
  public void decodeSingleColumnNumericPut() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    ArrayList<KeyValue> row = Lists.newArrayList();
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(0, (short) 0) ,
        NumericCodec.vleEncodeLong(1)
        ));
    
    result.decode(row, null);
    
    assertEquals(1, result.timeSeries().size());
    TimeSeries series = result.timeSeries().iterator().next();
    Iterator<TimeSeriesValue<?>> it = series.iterator(NumericType.TYPE).get();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES, v.timestamp().epoch());
    assertEquals(1, v.value().longValue());
    assertFalse(it.hasNext());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
  }

  @Test
  public void decodeSingleColumnNumericPutFiltered() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    ArrayList<KeyValue> row = Lists.newArrayList();
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(0, (short) 0) ,
        NumericCodec.vleEncodeLong(1)
        ));
    when(node.fetchDataType((byte) 1)).thenReturn(false);
    
    result.decode(row, null);
    
    assertTrue(result.timeSeries().isEmpty());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
  }

  @Test
  public void decodeMultiColumnNumericPut() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    ArrayList<KeyValue> row = Lists.newArrayList();
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(0, (short) 0) ,
        NumericCodec.vleEncodeLong(1)
        ));
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(30, (short) 0) ,
        NumericCodec.vleEncodeLong(2)
        ));
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(60, (short) 0) ,
        NumericCodec.vleEncodeLong(3)
        ));

    result.decode(row, null);
    
    assertEquals(1, result.timeSeries().size());
    TimeSeries series = result.timeSeries().iterator().next();
    Iterator<TimeSeriesValue<?>> it = series.iterator(NumericType.TYPE).get();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES, v.timestamp().epoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES + 30, v.timestamp().epoch());
    assertEquals(2, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES + 60, v.timestamp().epoch());
    assertEquals(3, v.value().longValue());
    
    assertFalse(it.hasNext());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
  }

  @Test
  public void decodeMultiColumnNumericPutFiltered() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    ArrayList<KeyValue> row = Lists.newArrayList();
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(0, (short) 0) ,
        NumericCodec.vleEncodeLong(1)
        ));
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(30, (short) 0) ,
        NumericCodec.vleEncodeLong(2)
        ));
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(60, (short) 0) ,
        NumericCodec.vleEncodeLong(3)
        ));
    when(node.fetchDataType((byte) 1)).thenReturn(false);

    result.decode(row, null);
    
    assertTrue(result.timeSeries().isEmpty());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
  }

  @Test
  public void decodeSingleColumnNumericAppend() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    ArrayList<KeyValue> row = Lists.newArrayList();
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        new byte[] { Schema.APPENDS_PREFIX, 0, 0 },
        Bytes.concat(
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 1),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 3))
        ));

    result.decode(row, null);
    
    assertEquals(1, result.timeSeries().size());
    TimeSeries series = result.timeSeries().iterator().next();
    Iterator<TimeSeriesValue<?>> it = series.iterator(NumericType.TYPE).get();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES, v.timestamp().epoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES + 30, v.timestamp().epoch());
    assertEquals(2, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES + 60, v.timestamp().epoch());
    assertEquals(3, v.value().longValue());
    
    assertFalse(it.hasNext());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
  }

  @Test
  public void decodeSingleColumnNumericAppendFiltered() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    ArrayList<KeyValue> row = Lists.newArrayList();
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        new byte[] { Schema.APPENDS_PREFIX, 0, 0 },
        Bytes.concat(
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 1),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 3))
        ));
    when(node.fetchDataType((byte) 1)).thenReturn(false);
    
    result.decode(row, null);
    
    assertTrue(result.timeSeries().isEmpty());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
  }

  @Test
  public void decodeMultiColumnNumericPutsAndAppend() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    ArrayList<KeyValue> row = Lists.newArrayList();
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(0, (short) 0) ,
        NumericCodec.vleEncodeLong(1)
        ));
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(30, (short) 0) ,
        NumericCodec.vleEncodeLong(2)
        ));
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        new byte[] { Schema.APPENDS_PREFIX, 0, 0 },
        Bytes.concat(
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 3),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 4))
        ));
    
    result.decode(row, null);
    
    assertEquals(1, result.timeSeries().size());
    TimeSeries series = result.timeSeries().iterator().next();
    Iterator<TimeSeriesValue<?>> it = series.iterator(NumericType.TYPE).get();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES, v.timestamp().epoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES + 30, v.timestamp().epoch());
    assertEquals(2, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES + 60, v.timestamp().epoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES + 120, v.timestamp().epoch());
    assertEquals(4, v.value().longValue());
    
    assertFalse(it.hasNext());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
  }

  @Test
  public void decodeMultiColumnNumericPutsAndAppendFiltered() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    ArrayList<KeyValue> row = Lists.newArrayList();
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(0, (short) 0) ,
        NumericCodec.vleEncodeLong(1)
        ));
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(30, (short) 0) ,
        NumericCodec.vleEncodeLong(2)
        ));
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        new byte[] { Schema.APPENDS_PREFIX, 0, 0 },
        Bytes.concat(
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 3),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 4))
        ));
    when(node.fetchDataType((byte) 1)).thenReturn(false);
    
    result.decode(row, null);
    
    assertTrue(result.timeSeries().isEmpty());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
  }

  @Test
  public void decodeSingleColumnRollup() throws Exception {
    RollupInterval interval = RollupInterval.builder()
        .setInterval("1h")
        .setRowSpan("1d")
        .setTable("tsdb-rollup-1h")
        .setPreAggregationTable("tsdb-rollup-1h")
        .build();
    interval.setConfig(rollup_config);
    rollup_config = DefaultRollupConfig.builder()
        .addInterval(interval)
        .addAggregationId("sum", 0)
        .build();
    
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(0, node, schema);
    
    byte[] qualifier = RollupUtils.buildRollupQualifier(TS_SINGLE_SERIES, (short) 0, 0, interval);
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    ArrayList<KeyValue> row = Lists.newArrayList();
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        qualifier,
        NumericCodec.vleEncodeLong(1)
        ));
    
    result.decode(row, interval);
    
    assertEquals(1, result.timeSeries().size());
    TimeSeries series = result.timeSeries().iterator().next();
    Iterator<TimeSeriesValue<?>> it = series.iterator(NumericSummaryType.TYPE).get();
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(TS_SINGLE_SERIES, v.timestamp().epoch());
    assertEquals(1, v.value().value(0).longValue());
    assertFalse(it.hasNext());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
  }
  
  @Test
  public void decodeSingleColumnRollupStringPrefix() throws Exception {
    RollupInterval interval = RollupInterval.builder()
        .setInterval("1h")
        .setRowSpan("1d")
        .setTable("tsdb-rollup-1h")
        .setPreAggregationTable("tsdb-rollup-1h")
        .build();
    interval.setConfig(rollup_config);
    rollup_config = DefaultRollupConfig.builder()
        .addInterval(interval)
        .addAggregationId("sum", 0)
        .build();
    
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(0, node, schema);
    
    byte[] qualifier = buildStringQualifier(0, (short) 0, 0, interval);
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    ArrayList<KeyValue> row = Lists.newArrayList();
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        qualifier,
        NumericCodec.vleEncodeLong(1)
        ));
    
    result.decode(row, interval);
    
    assertEquals(1, result.timeSeries().size());
    TimeSeries series = result.timeSeries().iterator().next();
    Iterator<TimeSeriesValue<?>> it = series.iterator(NumericSummaryType.TYPE).get();
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(TS_SINGLE_SERIES, v.timestamp().epoch());
    assertEquals(1, v.value().value(0).longValue());
    assertFalse(it.hasNext());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
  }
  
  @Test
  public void decodeUnknownType() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    ArrayList<KeyValue> row = Lists.newArrayList();
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(0, (short) 0) ,
        NumericCodec.vleEncodeLong(1)
        ));
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        new byte[] { 8, 2, 0 },
        new byte[] { 2 }
        ));
    row.add(new KeyValue(row_key, Tsdb1xHBaseDataStore.DATA_FAMILY,
        new byte[] { Schema.APPENDS_PREFIX, 0, 0 },
        Bytes.concat(
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 3),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 4))
        ));
    
    result.decode(row, null);
    
    assertEquals(1, result.timeSeries().size());
    TimeSeries series = result.timeSeries().iterator().next();
    Iterator<TimeSeriesValue<?>> it = series.iterator(NumericType.TYPE).get();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES, v.timestamp().epoch());
    assertEquals(1, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES + 60, v.timestamp().epoch());
    assertEquals(3, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES + 120, v.timestamp().epoch());
    assertEquals(4, v.value().longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void decodeErrors() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(0, node, schema);
    
    try {
      result.decode(null, null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    try {
      result.decode(new ArrayList<KeyValue>(), null);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) { }
  }
  
  byte[] buildStringQualifier(int offset, short flags, int type, RollupInterval interval) {
    byte[] qualifier = RollupUtils.buildRollupQualifier(TS_SINGLE_SERIES + offset, flags, type, interval);
    String name = interval.rollupConfig().getAggregatorForId(type);
    if (Strings.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("No agg for ID: " + type);
    }
    name = name.toUpperCase();
    byte[] q = new byte[qualifier.length - 1 + name.length() + 1];
    System.arraycopy(name.getBytes(), 0, q, 0, name.length());
    q[name.length()] = ':';
    System.arraycopy(qualifier, 1, q, name.length() + 1, qualifier.length - 1);
    return q;
  }
  // TODO - test other types when codecs are ready
}
