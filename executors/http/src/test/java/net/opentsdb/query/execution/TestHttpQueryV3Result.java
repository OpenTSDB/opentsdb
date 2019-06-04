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
package net.opentsdb.query.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.ZoneId;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.utils.JSON;

public class TestHttpQueryV3Result {

  private QueryNode query_node;
  private QueryPipelineContext context;
  private TimeSeriesQuery query;
  
  @Before
  public void before() throws Exception {
    query_node = mock(QueryNode.class);
    context = mock(QueryPipelineContext.class);
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig)
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("system.cpu.user")
                .build())
            .setFilterId(null)
            .setId("m1")
            .build();

    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1h-ago")
        .addExecutionGraphNode(config)
        .build();
    
    when(query_node.pipelineContext()).thenReturn(context);
    when(context.query()).thenReturn(query);
  }
  
  @Test
  public void numericType() throws Exception {
    String json = "{\"results\":[{\"source\":\"m0:m0\",\"data\":["
        + "{\"metric\":\"system.cpu.user\",\"tags\":{\"host\":\"web01\"},"
        + "\"aggregateTags\":[],\"NumericType\":{\"1540567593\":"
        + "23.399999618530273,\"1540567653\":23,\"1540567713\":"
        + "23.399999618530273,\"1540567773\":23.399999618530273}},"
        + "{\"metric\":\"system.cpu.user\",\"tags\":{\"host\":\"web02\"},"
        + "\"aggregateTags\":[],\"NumericType\":{\"1540567584\":"
        + "52.29999923706055,\"1540567644\":52.29999923706055,"
        + "\"1540567704\":75,\"1540567764\":75.19999694824219}}]}]}";
    
    JsonNode node = JSON.getMapper().readTree(json);
    node = node.get("results").iterator().next();
    HttpQueryV3Result result = new HttpQueryV3Result(query_node, node, null);
    assertNull(result.timeSpecification());
    assertEquals(2, result.timeSeries().size());
    assertNull(result.error());
    assertNull(result.exception());
    assertEquals("m0", result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    
    Iterator<TimeSeries> ts_iterator = result.timeSeries().iterator();
    TimeSeries ts = ts_iterator.next();
    TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    assertEquals("system.cpu.user", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertEquals(1, ts.types().size());
    assertTrue(ts.types().contains(NumericType.TYPE));
    
    TypedTimeSeriesIterator iterator = ts.iterator(NumericType.TYPE).get();
    TimeSeriesValue<NumericType> value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1540567593, value.timestamp().epoch());
    assertEquals(23.399999618530273, value.value().doubleValue(), 0.0001);
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1540567653, value.timestamp().epoch());
    assertEquals(23, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1540567713, value.timestamp().epoch());
    assertEquals(23.399999618530273, value.value().doubleValue(), 0.0001);
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1540567773, value.timestamp().epoch());
    assertEquals(23.399999618530273, value.value().doubleValue(), 0.0001);
    assertFalse(iterator.hasNext());
    
    ts = ts_iterator.next();
    id = (TimeSeriesStringId) ts.id();
    assertEquals("system.cpu.user", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web02", id.tags().get("host"));
    assertEquals(1, ts.types().size());
    assertTrue(ts.types().contains(NumericType.TYPE));
    
    iterator = ts.iterator(NumericType.TYPE).get();
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1540567584, value.timestamp().epoch());
    assertEquals(52.29999923706055, value.value().doubleValue(), 0.0001);
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1540567644, value.timestamp().epoch());
    assertEquals(52.29999923706055, value.value().doubleValue(), 0.0001);
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1540567704, value.timestamp().epoch());
    assertEquals(75, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1540567764, value.timestamp().epoch());
    assertEquals(75.19999694824219, value.value().doubleValue(), 0.0001);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void numericArrayType() throws Exception {
    String json = "{\"results\":[{\"source\":\"groupby:m1\","
        + "\"timeSpecification\":{\"start\":1540567560,\"end\":1540567740,"
        + "\"intervalISO\":\"PT1M\",\"interval\":\"1m\",\"timeZone\":"
        + "\"UTC\",\"units\":\"Minutes\"},\"data\":[{\"metric\":"
        + "\"system.cpu.user\",\"tags\":{\"host\":\"web01\"},"
        + "\"NumericType\":[6790.780004132539,6784.159999866039,"
        + "6771.350002059713,6806.850009173155]},{\"metric\":"
        + "\"system.cpu.user\",\"tags\":{\"host\":\"web02\"},"
        + "\"NumericType\":[6790.780004132539,6784.159999866039,"
        + "6771.350002059713,6806.850009173155]}]}]}";
    
    JsonNode node = JSON.getMapper().readTree(json);
    node = node.get("results").iterator().next();
    HttpQueryV3Result result = new HttpQueryV3Result(query_node, node, null);
    assertEquals(1540567560, result.timeSpecification().start().epoch());
    assertEquals(1540567740, result.timeSpecification().end().epoch());
    assertEquals("1m", result.timeSpecification().stringInterval());
    assertEquals(ZoneId.of("UTC"), result.timeSpecification().timezone());
    assertEquals(2, result.timeSeries().size());
    assertNull(result.error());
    assertNull(result.exception());
    assertEquals("m1", result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    
    Iterator<TimeSeries> ts_iterator = result.timeSeries().iterator();
    TimeSeries ts = ts_iterator.next();
    TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    assertEquals("system.cpu.user", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertEquals(1, ts.types().size());
    assertTrue(ts.types().contains(NumericArrayType.TYPE));
    
    TypedTimeSeriesIterator iterator = ts.iterator(NumericArrayType.TYPE).get();
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(1540567560, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(4, value.value().end());
    double[] v = value.value().doubleArray();
    
    assertEquals(6790.780004132539, v[0], 0.0001);
    assertEquals(6784.159999866039, v[1], 0.0001);
    assertEquals(6771.350002059713, v[2], 0.0001);
    assertEquals(6806.850009173155, v[3], 0.0001);
    assertFalse(iterator.hasNext());
    
    ts = ts_iterator.next();
    id = (TimeSeriesStringId) ts.id();
    assertEquals("system.cpu.user", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web02", id.tags().get("host"));
    assertEquals(1, ts.types().size());
    assertTrue(ts.types().contains(NumericArrayType.TYPE));
    
    value = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(1540567560, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(4, value.value().end());
    v = value.value().doubleArray();
    
    assertEquals(6790.780004132539, v[0], 0.0001);
    assertEquals(6784.159999866039, v[1], 0.0001);
    assertEquals(6771.350002059713, v[2], 0.0001);
    assertEquals(6806.850009173155, v[3], 0.0001);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void numericSummaryType() throws Exception {
    String json = "{\"results\":[{\"source\":\"summarizer:m1\",\"data\":"
        + "[{\"metric\":\"system.cpu.user\",\"tags\":{\"host\":\"web01\"},"
        + "\"aggregateTags\":[],\"NumericSummaryType\":{\"aggregations\":"
        + "[\"sum\",\"count\",\"min\",\"max\",\"avg\",\"first\","
        + "\"last\"],\"data\":[{\"1540567560\":[407693.2907707449,"
        + "60,6704.690003493801,6867.070027515292,6794.888179512415,"
        + "6790.780004132539,6835.990010544658]}]}},{\"metric\":"
        + "\"system.cpu.user\",\"tags\":{\"host\":\"web02\"},"
        + "\"aggregateTags\":[],\"NumericSummaryType\":{\"aggregations\":"
        + "[\"sum\",\"count\",\"min\",\"max\",\"avg\",\"first\","
        + "\"last\"],\"data\":[{\"1540567560\":[407693.2907707449,60,"
        + "6704.690003493801,6867.070027515292,6794.888179512415,"
        + "6790.780004132539,6835.990010544658]}]}}]}]}";
    
    JsonNode node = JSON.getMapper().readTree(json);
    node = node.get("results").iterator().next();
    HttpQueryV3Result result = new HttpQueryV3Result(query_node, node, null);
    assertNull(result.timeSpecification());
    assertNull(result.error());
    assertNull(result.exception());
    assertEquals("m1", result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    
    RollupConfig rollup_config = result.rollupConfig();
    assertEquals(7, rollup_config.getAggregationIds().size());
    assertEquals("sum", rollup_config.getAggregatorForId(0));
    assertEquals("count", rollup_config.getAggregatorForId(1));
    assertEquals("min", rollup_config.getAggregatorForId(2));
    assertEquals("max", rollup_config.getAggregatorForId(3));
    assertEquals("avg", rollup_config.getAggregatorForId(4));
    assertEquals("first", rollup_config.getAggregatorForId(5));
    assertEquals("last", rollup_config.getAggregatorForId(6));
    
    Iterator<TimeSeries> ts_iterator = result.timeSeries().iterator();
    TimeSeries ts = ts_iterator.next();
    TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    assertEquals("system.cpu.user", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertEquals(1, ts.types().size());
    assertTrue(ts.types().contains(NumericSummaryType.TYPE));
    
    TypedTimeSeriesIterator iterator = ts.iterator(NumericSummaryType.TYPE).get();
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1540567560, value.timestamp().epoch());
    assertEquals(407693.2907707449, value.value().value(0).doubleValue(), 0.0001);
    assertEquals(60, value.value().value(1).longValue());
    assertEquals(6704.690003493801, value.value().value(2).doubleValue(), 0.0001);
    assertEquals(6867.070027515292, value.value().value(3).doubleValue(), 0.0001);
    assertEquals(6794.888179512415, value.value().value(4).doubleValue(), 0.0001);
    assertEquals(6790.780004132539, value.value().value(5).doubleValue(), 0.0001);
    assertEquals(6835.990010544658, value.value().value(6).doubleValue(), 0.0001);
    assertFalse(iterator.hasNext());
    
    ts = ts_iterator.next();
    iterator = ts.iterator(NumericSummaryType.TYPE).get();
    id = (TimeSeriesStringId) ts.id();
    assertEquals("system.cpu.user", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web02", id.tags().get("host"));
    assertEquals(1, ts.types().size());
    assertTrue(ts.types().contains(NumericSummaryType.TYPE));
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1540567560, value.timestamp().epoch());
    assertEquals(407693.2907707449, value.value().value(0).doubleValue(), 0.0001);
    assertEquals(60, value.value().value(1).longValue());
    assertEquals(6704.690003493801, value.value().value(2).doubleValue(), 0.0001);
    assertEquals(6867.070027515292, value.value().value(3).doubleValue(), 0.0001);
    assertEquals(6794.888179512415, value.value().value(4).doubleValue(), 0.0001);
    assertEquals(6790.780004132539, value.value().value(5).doubleValue(), 0.0001);
    assertEquals(6835.990010544658, value.value().value(6).doubleValue(), 0.0001);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void numericSummaryTypeRollupGiven() throws Exception {
    String json = "{\"results\":[{\"source\":\"summarizer:m1\",\"data\":"
        + "[{\"metric\":\"system.cpu.user\",\"tags\":{\"host\":\"web01\"},"
        + "\"aggregateTags\":[],\"NumericSummaryType\":{\"aggregations\":"
        + "[\"sum\",\"count\",\"min\",\"max\",\"avg\",\"first\","
        + "\"last\"],\"data\":[{\"1540567560\":[407693.2907707449,"
        + "60,6704.690003493801,6867.070027515292,6794.888179512415,"
        + "6790.780004132539,6835.990010544658]}]}},{\"metric\":"
        + "\"system.cpu.user\",\"tags\":{\"host\":\"web02\"},"
        + "\"aggregateTags\":[],\"NumericSummaryType\":{\"aggregations\":"
        + "[\"sum\",\"count\",\"min\",\"max\",\"avg\",\"first\","
        + "\"last\"],\"data\":[{\"1540567560\":[407693.2907707449,60,"
        + "6704.690003493801,6867.070027515292,6794.888179512415,"
        + "6790.780004132539,6835.990010544658]}]}}]}]}";
    
    RollupConfig rollup_config = DefaultRollupConfig.newBuilder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 1)
        .addInterval(RollupInterval.builder()
            .setInterval("1m")
            .setRowSpan("1h")
            .setTable("foo")
            .setPreAggregationTable("foo"))
        .build();
    
    JsonNode node = JSON.getMapper().readTree(json);
    node = node.get("results").iterator().next();
    HttpQueryV3Result result = new HttpQueryV3Result(query_node, node, rollup_config);
    assertNull(result.timeSpecification());
    assertNull(result.error());
    assertNull(result.exception());
    assertEquals("m1", result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    
    rollup_config = result.rollupConfig();
    assertEquals(2, rollup_config.getAggregationIds().size());
    assertEquals("sum", rollup_config.getAggregatorForId(0));
    assertEquals("count", rollup_config.getAggregatorForId(1));
  }
  
  @Test
  public void exception() throws Exception {
    RuntimeException ex = new RuntimeException("Boo!");
    HttpQueryV3Result result = new HttpQueryV3Result(query_node, null, null, ex);
    assertNull(result.timeSpecification());
    assertTrue(result.timeSeries().isEmpty());
    assertEquals("Boo!", result.error());
    assertTrue(result.exception() instanceof RuntimeException);
    assertNull(result.dataSource());
  }
  
  @Test
  public void runAll() throws Exception {
    String json = "{\"results\":[{\"source\":\"groupby:m1\","
        + "\"timeSpecification\":{\"start\":1540567560,\"end\":1540567740,"
        + "\"intervalISO\":null,\"interval\":\"0all\",\"timeZone\":"
        + "\"UTC\",\"units\":null},\"data\":[{\"metric\":"
        + "\"system.cpu.user\",\"tags\":{\"host\":\"web01\"},"
        + "\"NumericType\":[42]},{\"metric\":"
        + "\"system.cpu.user\",\"tags\":{\"host\":\"web02\"},"
        + "\"NumericType\":[24]}]}]}";
    
    JsonNode node = JSON.getMapper().readTree(json);
    node = node.get("results").iterator().next();
    HttpQueryV3Result result = new HttpQueryV3Result(query_node, node, null);
    
    assertEquals(1540567560, result.timeSpecification().start().epoch());
    assertEquals(1540567740, result.timeSpecification().end().epoch());
    assertNull(result.timeSpecification().interval());
    assertNull(result.timeSpecification().units());
    assertEquals("0all", result.timeSpecification().stringInterval());
    assertEquals(ZoneId.of("UTC"), result.timeSpecification().timezone());
    assertEquals(2, result.timeSeries().size());
    assertNull(result.error());
    assertNull(result.exception());
    assertEquals("m1", result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    
    Iterator<TimeSeries> ts_iterator = result.timeSeries().iterator();
    TimeSeries ts = ts_iterator.next();
    TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    assertEquals("system.cpu.user", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertEquals(1, ts.types().size());
    assertTrue(ts.types().contains(NumericArrayType.TYPE));
    
    TypedTimeSeriesIterator iterator = ts.iterator(NumericArrayType.TYPE).get();
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(1540567560, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(1, value.value().end());
    long[] v = value.value().longArray();
    
    assertEquals(42, v[0]);
    assertFalse(iterator.hasNext());
    
    ts = ts_iterator.next();
    id = (TimeSeriesStringId) ts.id();
    iterator = ts.iterator(NumericArrayType.TYPE).get();
    assertEquals("system.cpu.user", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web02", id.tags().get("host"));
    assertEquals(1, ts.types().size());
    assertTrue(ts.types().contains(NumericArrayType.TYPE));
    
    value = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(1540567560, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(1, value.value().end());
    v = value.value().longArray();
    
    assertEquals(24, v[0]);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void runAllMillis() throws Exception {
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1559520000123")
        .setEnd("1559523600123")
        .addExecutionGraphNode((TimeSeriesDataSourceConfig)
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("system.cpu.user")
                .build())
            .setFilterId(null)
            .setId("m1")
            .build())
        .build();
    when(context.query()).thenReturn(query);
    
    String json = "{\"results\":[{\"source\":\"groupby:m1\","
        + "\"timeSpecification\":{\"start\":1559520000,\"end\":1559523600,"
        + "\"intervalISO\":null,\"interval\":\"0all\",\"timeZone\":"
        + "\"UTC\",\"units\":null},\"data\":[{\"metric\":"
        + "\"system.cpu.user\",\"tags\":{\"host\":\"web01\"},"
        + "\"NumericType\":[42]},{\"metric\":"
        + "\"system.cpu.user\",\"tags\":{\"host\":\"web02\"},"
        + "\"NumericType\":[24]}]}]}";
    
    JsonNode node = JSON.getMapper().readTree(json);
    node = node.get("results").iterator().next();
    HttpQueryV3Result result = new HttpQueryV3Result(query_node, node, null);
    
    assertEquals(query.startTime().msEpoch(), result.timeSpecification().start().msEpoch());
    assertEquals(query.endTime().msEpoch(), result.timeSpecification().end().msEpoch());
    assertNull(result.timeSpecification().interval());
    assertNull(result.timeSpecification().units());
    assertEquals("0all", result.timeSpecification().stringInterval());
    assertEquals(ZoneId.of("UTC"), result.timeSpecification().timezone());
    assertEquals(2, result.timeSeries().size());
    assertNull(result.error());
    assertNull(result.exception());
    assertEquals("m1", result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    
    Iterator<TimeSeries> ts_iterator = result.timeSeries().iterator();
    TimeSeries ts = ts_iterator.next();
    TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    assertEquals("system.cpu.user", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertEquals(1, ts.types().size());
    assertTrue(ts.types().contains(NumericArrayType.TYPE));
    
    TypedTimeSeriesIterator iterator = ts.iterator(NumericArrayType.TYPE).get();
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(query.startTime().msEpoch(), value.timestamp().msEpoch());
    assertEquals(0, value.value().offset());
    assertEquals(1, value.value().end());
    long[] v = value.value().longArray();
    
    assertEquals(42, v[0]);
    assertFalse(iterator.hasNext());
    
    ts = ts_iterator.next();
    id = (TimeSeriesStringId) ts.id();
    iterator = ts.iterator(NumericArrayType.TYPE).get();
    assertEquals("system.cpu.user", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web02", id.tags().get("host"));
    assertEquals(1, ts.types().size());
    assertTrue(ts.types().contains(NumericArrayType.TYPE));
    
    value = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(query.startTime().msEpoch(), value.timestamp().msEpoch());
    assertEquals(0, value.value().offset());
    assertEquals(1, value.value().end());
    v = value.value().longArray();
    
    assertEquals(24, v[0]);
    assertFalse(iterator.hasNext());
  }
  
}
