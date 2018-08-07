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
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.base.Strings;
import com.google.common.primitives.Bytes;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec.OffsetResolution;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ExecutorService.class, BigtableSession.class, 
  Tsdb1xBigtableQueryNode.class, CredentialOptions.class })
public class TestTsdb1xBigtableQueryResult extends UTBase {
  //GMT: Monday, January 1, 2018 12:15:00 AM
  public static final int START_TS = 1514765700;
 
  // GMT: Monday, January 1, 2018 1:15:00 AM
  public static final int END_TS = 1514769300;
 
  private Tsdb1xBigtableQueryNode node;
  private Schema schema; 
  private QuerySourceConfig source_config;
  private DefaultRollupConfig rollup_config;
  private SemanticQuery query;
  
  @Before
  public void before() throws Exception {
    node = mock(Tsdb1xBigtableQueryNode.class);
    schema = spy(new Schema(tsdb, null));

    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(START_TS))
        .setEnd(Integer.toString(END_TS))
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .setId("graph")
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("datasource"))
            .build())
        .build();
    
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setId("m1")
        .build();
    final QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(tsdb);
    when(node.pipelineContext()).thenReturn(context);
    when(node.fetchDataType(any(byte.class))).thenReturn(true);
    when(node.schema()).thenReturn(schema);
    when(node.config()).thenReturn(source_config);
  }
  
  @Test
  public void decodeSingleColumnNumericPut() throws Exception {
    Tsdb1xBigtableQueryResult result = new Tsdb1xBigtableQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    FlatRow flat_row = buildFlatRow(row_key, 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        NumericCodec.buildSecondQualifier(0, (short) 0) ,
        NumericCodec.vleEncodeLong(1));
    
    result.decode(flat_row, null);
    
    assertEquals(1, result.timeSeries().size());
    TimeSeries series = result.timeSeries().iterator().next();
    Iterator<TimeSeriesValue<?>> it = series.iterator(NumericType.TYPE).get();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES, v.timestamp().epoch());
    assertEquals(1, v.value().longValue());
    assertFalse(it.hasNext());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
    
    Row row = buildRow(row_key, 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        NumericCodec.buildSecondQualifier(0, (short) 0) ,
        NumericCodec.vleEncodeLong(1));
    result = new Tsdb1xBigtableQueryResult(0, node, schema);
    result.decode(row, null);
    
    assertEquals(1, result.timeSeries().size());
    series = result.timeSeries().iterator().next();
    it = series.iterator(NumericType.TYPE).get();
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(TS_SINGLE_SERIES, v.timestamp().epoch());
    assertEquals(1, v.value().longValue());
    assertFalse(it.hasNext());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
  }
  
  @Test
  public void decodeSingleColumnNumericPutFiltered() throws Exception {
    Tsdb1xBigtableQueryResult result = new Tsdb1xBigtableQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    FlatRow row = buildFlatRow(row_key, 
        Tsdb1xBigtableDataStore.DATA_FAMILY, 
        NumericCodec.buildSecondQualifier(0, (short) 0) ,
        NumericCodec.vleEncodeLong(1));
    when(node.fetchDataType((byte) 1)).thenReturn(false);
    
    result.decode(row, null);
    
    assertTrue(result.timeSeries().isEmpty());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
  }

  @Test
  public void decodeMultiColumnNumericPut() throws Exception {
    Tsdb1xBigtableQueryResult result = new Tsdb1xBigtableQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    FlatRow.Builder builder = FlatRow.newBuilder()
        .withRowKey(ByteStringer.wrap(row_key));
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(0, (short) 0) ,
        NumericCodec.vleEncodeLong(1));
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(30, (short) 0) ,
        NumericCodec.vleEncodeLong(2));
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(60, (short) 0) ,
        NumericCodec.vleEncodeLong(3));

    result.decode(builder.build(), null);
    
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
    Tsdb1xBigtableQueryResult result = new Tsdb1xBigtableQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    FlatRow.Builder builder = FlatRow.newBuilder()
        .withRowKey(ByteStringer.wrap(row_key));
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(0, (short) 0) ,
        NumericCodec.vleEncodeLong(1));
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(30, (short) 0) ,
        NumericCodec.vleEncodeLong(2));
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(60, (short) 0) ,
        NumericCodec.vleEncodeLong(3));
    when(node.fetchDataType((byte) 1)).thenReturn(false);

    result.decode(builder.build(), null);
    
    assertTrue(result.timeSeries().isEmpty());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
  }

  @Test
  public void decodeSingleColumnNumericAppend() throws Exception {
    Tsdb1xBigtableQueryResult result = new Tsdb1xBigtableQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    FlatRow row = buildFlatRow(row_key, Tsdb1xBigtableDataStore.DATA_FAMILY,
        new byte[] { Schema.APPENDS_PREFIX, 0, 0 },
        Bytes.concat(
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 1),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 3)));

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
    Tsdb1xBigtableQueryResult result = new Tsdb1xBigtableQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    FlatRow row = buildFlatRow(row_key, Tsdb1xBigtableDataStore.DATA_FAMILY,
        new byte[] { Schema.APPENDS_PREFIX, 0, 0 },
        Bytes.concat(
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 1),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 30, 2),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 3)));
    when(node.fetchDataType((byte) 1)).thenReturn(false);
    
    result.decode(row, null);
    
    assertTrue(result.timeSeries().isEmpty());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
  }

  @Test
  public void decodeMultiColumnNumericPutsAndAppend() throws Exception {
    Tsdb1xBigtableQueryResult result = new Tsdb1xBigtableQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    FlatRow.Builder builder = FlatRow.newBuilder()
         .withRowKey(ByteStringer.wrap(row_key));
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
         NumericCodec.buildSecondQualifier(0, (short) 0) ,
         NumericCodec.vleEncodeLong(1));
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
         NumericCodec.buildSecondQualifier(30, (short) 0) ,
         NumericCodec.vleEncodeLong(2));
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
         new byte[] { Schema.APPENDS_PREFIX, 0, 0 },
         Bytes.concat(
             NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 3),
             NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 4)));
    
    result.decode(builder.build(), null);
    
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
    Tsdb1xBigtableQueryResult result = new Tsdb1xBigtableQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    FlatRow.Builder builder = FlatRow.newBuilder()
        .withRowKey(ByteStringer.wrap(row_key));
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(0, (short) 0) ,
        NumericCodec.vleEncodeLong(1));
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(30, (short) 0) ,
        NumericCodec.vleEncodeLong(2));
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
        new byte[] { Schema.APPENDS_PREFIX, 0, 0 },
        Bytes.concat(
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 3),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 4)));
    when(node.fetchDataType((byte) 1)).thenReturn(false);
    
    result.decode(builder.build(), null);
    
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
    
    Tsdb1xBigtableQueryResult result = new Tsdb1xBigtableQueryResult(0, node, schema);
    
    byte[] qualifier = RollupUtils.buildRollupQualifier(TS_SINGLE_SERIES, (short) 0, 0, interval);
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    FlatRow row = buildFlatRow(row_key, Tsdb1xBigtableDataStore.DATA_FAMILY,
        qualifier,
        NumericCodec.vleEncodeLong(1));
    
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
    
    Tsdb1xBigtableQueryResult result = new Tsdb1xBigtableQueryResult(0, node, schema);
    
    byte[] qualifier = buildStringQualifier(0, (short) 0, 0, interval);
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    FlatRow row = buildFlatRow(row_key, Tsdb1xBigtableDataStore.DATA_FAMILY,
        qualifier,
        NumericCodec.vleEncodeLong(1));
    
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
    Tsdb1xBigtableQueryResult result = new Tsdb1xBigtableQueryResult(0, node, schema);
    
    final byte[] row_key = makeRowKey(METRIC_BYTES, TS_SINGLE_SERIES, TAGK_BYTES, TAGV_BYTES);
    FlatRow.Builder builder = FlatRow.newBuilder()
        .withRowKey(ByteStringer.wrap(row_key));
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
        NumericCodec.buildSecondQualifier(0, (short) 0) ,
        NumericCodec.vleEncodeLong(1));
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
        new byte[] { 8, 2, 0 },
        new byte[] { 2 });
    addCell(builder, Tsdb1xBigtableDataStore.DATA_FAMILY,
        new byte[] { Schema.APPENDS_PREFIX, 0, 0 },
        Bytes.concat(
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 3),
            NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 4)));
    
    result.decode(builder.build(), null);
    
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
    Tsdb1xBigtableQueryResult result = new Tsdb1xBigtableQueryResult(0, node, schema);
    
    try {
      result.decode((FlatRow) null, null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    try {
      result.decode((Row) null, null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    try {
      result.decode(FlatRow.newBuilder().build(), null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
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
