//This file is part of OpenTSDB.
//Copyright (C) 2018  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 2.1 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.
package net.opentsdb.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.TimeSeriesQuery.LogLevel;
import net.opentsdb.query.execution.serdes.JsonV2QuerySerdesOptions;
import net.opentsdb.query.filter.DefaultNamedFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.NamedFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.storage.MockDataStoreFactory;
import net.opentsdb.utils.JSON;

public class TestSemanticQuery {

  @Test
  public void builder() throws Exception {
    NamedFilter filter = DefaultNamedFilter.newBuilder()
            .setFilter(TagValueLiteralOrFilter.newBuilder()
            .setFilter("web01")
            .setTagKey("host")
            .build())
        .setId("f1")
        .build();
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setTimeZone("America/Denver")
        .addFilter(filter)
        .setExecutionGraph(graph)
        .addSerdesConfig(JsonV2QuerySerdesOptions.newBuilder()
            .addFilter("ds")
            .setId("serdes")
            .build())
        .build();
    
    assertEquals(QueryMode.SINGLE, query.getMode());
    assertEquals("1514764800", query.getStart());
    assertEquals(1514764800, query.startTime().epoch());
    assertEquals("1514768400", query.getEnd());
    assertEquals(1514768400, query.endTime().epoch());
    assertEquals("America/Denver", query.getTimezone());
    assertEquals("web01", ((TagValueLiteralOrFilter) query.getFilter("f1")).getFilter());
    assertEquals("f1", query.getFilters().get(0).getId());
    assertEquals("web01", ((TagValueLiteralOrFilter) query.getFilters().get(0).getFilter()).getFilter());
    assertEquals(1, query.getExecutionGraph().size());
    assertEquals("ds", query.getSerdesConfigs().get(0).getFilter().get(0));
    assertEquals(LogLevel.ERROR, query.getLogLevel());
    
    try {
      SemanticQuery.newBuilder()
          //.setMode(QueryMode.SINGLE)
          .setStart("1514764800")
          .setEnd("1514768400")
          .setTimeZone("America/Denver")
          .addFilter(filter)
          .setExecutionGraph(graph)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    try {
      SemanticQuery.newBuilder()
          .setMode(QueryMode.SINGLE)
  //        .setStart("1514764800")
  //        .setEnd("1514768400")
          .setTimeZone("America/Denver")
          .addFilter(filter)
          .setExecutionGraph(graph)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      SemanticQuery.newBuilder()
          .setMode(QueryMode.SINGLE)
          .setStart("1514764800")
          .setEnd("1514768400")
          .setTimeZone("America/Denver")
          .addFilter(filter)
          //.setExecutionGraph(graph)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void timestampPermutations() throws Exception {

    NamedFilter filter = DefaultNamedFilter.newBuilder()
        .setFilter(TagValueLiteralOrFilter.newBuilder()
            .setFilter("web01")
            .setTagKey("host")
            .build())
        .setId("f1")
        .build();
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build());

    SemanticQuery endTimeAfterCurrentTimeAndNoStartTime = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("")
        .setEnd("9999999999")
        .setTimeZone("America/Denver")
        .addFilter(filter)
        .setExecutionGraph(graph)
        .addSerdesConfig(JsonV2QuerySerdesOptions.newBuilder()
            .addFilter("ds")
            .setId("serdes")
            .build())
        .build();

    assertEquals(null, endTimeAfterCurrentTimeAndNoStartTime.getStart());
    assertTrue(1000000000 < endTimeAfterCurrentTimeAndNoStartTime.startTime().epoch() && endTimeAfterCurrentTimeAndNoStartTime.startTime().epoch() < 9999999999L);
    assertEquals("9999999999", endTimeAfterCurrentTimeAndNoStartTime.getEnd());
    assertEquals(9999999999L, endTimeAfterCurrentTimeAndNoStartTime.endTime().epoch());

    SemanticQuery endTimeBeforeCurrentTimeAndNoStartTime = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("")
        .setEnd("1000000000")
        .setTimeZone("America/Denver")
        .addFilter(filter)
        .setExecutionGraph(graph)
        .addSerdesConfig(JsonV2QuerySerdesOptions.newBuilder()
            .addFilter("ds")
            .setId("serdes")
            .build())
        .build();

    assertEquals(null, endTimeBeforeCurrentTimeAndNoStartTime.getStart());
    assertEquals(1000000000, endTimeBeforeCurrentTimeAndNoStartTime.startTime().epoch());
    assertEquals("1000000000", endTimeBeforeCurrentTimeAndNoStartTime.getEnd());
    assertTrue(1000000000 < endTimeBeforeCurrentTimeAndNoStartTime.endTime().epoch() && endTimeBeforeCurrentTimeAndNoStartTime.endTime().epoch() < 9999999999L);

    SemanticQuery noEndTimeAndStartTimeAfterCurrentTime = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("9999999999")
        .setEnd("")
        .setTimeZone("America/Denver")
        .addFilter(filter)
        .setExecutionGraph(graph)
        .addSerdesConfig(JsonV2QuerySerdesOptions.newBuilder()
            .addFilter("ds")
            .setId("serdes")
            .build())
        .build();

    assertEquals("9999999999", noEndTimeAndStartTimeAfterCurrentTime.getStart());
    assertTrue(1000000000 < noEndTimeAndStartTimeAfterCurrentTime.startTime().epoch() && noEndTimeAndStartTimeAfterCurrentTime.startTime().epoch() < 9999999999L);
    assertEquals(null, noEndTimeAndStartTimeAfterCurrentTime.getEnd());
    assertEquals(9999999999L, noEndTimeAndStartTimeAfterCurrentTime.endTime().epoch());

    SemanticQuery noEndTimeAndStartTimeBeforeCurrentTime = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1000000000")
        .setEnd("")
        .setTimeZone("America/Denver")
        .addFilter(filter)
        .setExecutionGraph(graph)
        .addSerdesConfig(JsonV2QuerySerdesOptions.newBuilder()
            .addFilter("ds")
            .setId("serdes")
            .build())
        .build();

    assertEquals("1000000000", noEndTimeAndStartTimeBeforeCurrentTime.getStart());
    assertEquals(1000000000, noEndTimeAndStartTimeBeforeCurrentTime.startTime().epoch());
    assertEquals(null, noEndTimeAndStartTimeBeforeCurrentTime.getEnd());
    assertTrue(1000000000 < noEndTimeAndStartTimeBeforeCurrentTime.endTime().epoch() && noEndTimeAndStartTimeBeforeCurrentTime.endTime().epoch() < 9999999999L);

    SemanticQuery EndTimeBeforeStartTime = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("2000000000")
        .setEnd("1000000000")
        .setTimeZone("America/Denver")
        .addFilter(filter)
        .setExecutionGraph(graph)
        .addSerdesConfig(JsonV2QuerySerdesOptions.newBuilder()
            .addFilter("ds")
            .setId("serdes")
            .build())
        .build();

    assertEquals("2000000000", EndTimeBeforeStartTime.getStart());
    assertEquals(1000000000, EndTimeBeforeStartTime.startTime().epoch());
    assertEquals("1000000000", EndTimeBeforeStartTime.getEnd());
    assertEquals(2000000000, EndTimeBeforeStartTime.endTime().epoch());

    SemanticQuery StartTimeBeforeEndTime = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1000000000")
        .setEnd("2000000000")
        .setTimeZone("America/Denver")
        .addFilter(filter)
        .setExecutionGraph(graph)
        .addSerdesConfig(JsonV2QuerySerdesOptions.newBuilder()
            .addFilter("ds")
            .setId("serdes")
            .build())
        .build();

    assertEquals("1000000000", StartTimeBeforeEndTime.getStart());
    assertEquals(1000000000, StartTimeBeforeEndTime.startTime().epoch());
    assertEquals("2000000000", StartTimeBeforeEndTime.getEnd());
    assertEquals(2000000000, StartTimeBeforeEndTime.endTime().epoch());

  }
  
  @Test
  public void serdes() throws Exception {
    NumericInterpolatorConfig numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
    
    NamedFilter filter = DefaultNamedFilter.newBuilder()
            .setFilter(TagValueLiteralOrFilter.newBuilder()
            .setFilter("web01")
            .setTagKey("host")
            .build())
        .setId("f1")
        .build();
    
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setId("foo")
            .setInterval("15s")
            .setStart("1514764800")
            .setEnd("1514768400")
            .addInterpolatorConfig(numeric_config)
            .addSource("m1")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setTimeZone("America/Denver")
        .addFilter(filter)
        .setExecutionGraph(graph)
        .setLogLevel(LogLevel.DEBUG)
        .addSerdesConfig(JsonV2QuerySerdesOptions.newBuilder()
            .addFilter("ds")
            .setId("JsonV2QuerySerdes")
            .build())
        .build();
    
    final String json = JSON.serializeToString(query);
    assertTrue(json.contains("\"start\":\"1514764800\""));
    assertTrue(json.contains("\"end\":\"1514768400\""));
    assertTrue(json.contains("\"filters\":["));
    assertTrue(json.contains("\"id\":\"f1\""));
    assertTrue(json.contains("\"filter\":\"web01\""));
    assertTrue(json.contains("\"mode\":\"SINGLE\""));
    assertTrue(json.contains("\"timezone\":\"America/Denver\""));
    assertTrue(json.contains("\"logLevel\":\"DEBUG\""));
    assertTrue(json.contains("\"executionGraph\":["));
    assertTrue(json.contains("\"id\":\"m1\""));
    //assertTrue(json.contains("\"type\":\"" + DefaultTimeSeriesDataSourceConfig.TYPE + "\""));
    assertTrue(json.contains("\"metric\":{"));
    assertTrue(json.contains("\"id\":\"foo\""));
    assertTrue(json.contains("\"type\":\"Downsample\""));
    assertTrue(json.contains("\"sources\":[\"m1\"]"));
    assertTrue(json.contains("\"interval\":\"15s\""));
    assertTrue(json.contains("\"serdesConfigs\":["));
    assertTrue(json.contains("\"filter\":[\"ds\"]"));
    
    MockTSDB tsdb = new MockTSDB();
    tsdb.registry = new DefaultRegistry(tsdb);
    ((DefaultRegistry) tsdb.registry).initialize(true);
    MockDataStoreFactory factory = new MockDataStoreFactory();
    factory.initialize(tsdb, null);
    tsdb.registry.registerPlugin(TimeSeriesDataSourceFactory.class, 
        null, factory);
    
    JsonNode node = JSON.getMapper().readTree(json);
    query = SemanticQuery.parse(tsdb, node).build();
    
    assertEquals(QueryMode.SINGLE, query.getMode());
    assertEquals("1514764800", query.getStart());
    assertEquals(1514764800, query.startTime().epoch());
    assertEquals("1514768400", query.getEnd());
    assertEquals(1514768400, query.endTime().epoch());
    assertEquals("America/Denver", query.getTimezone());
    assertEquals("web01", ((TagValueLiteralOrFilter) query.getFilter("f1")).getFilter());
    assertEquals("f1", query.getFilters().get(0).getId());
    assertEquals("web01", ((TagValueLiteralOrFilter) query.getFilters().get(0).getFilter()).getFilter());
    assertEquals(2, query.getExecutionGraph().size());
    assertEquals("ds", query.getSerdesConfigs().get(0).getFilter().get(0));
    assertEquals(LogLevel.DEBUG, query.getLogLevel());
  }
  
  @Test
  public void getFilter() {
    NumericInterpolatorConfig numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
    
    NamedFilter filter = DefaultNamedFilter.newBuilder()
          .setFilter(TagValueLiteralOrFilter.newBuilder()
          .setFilter("web01")
          .setTagKey("host")
          .build())
      .setId("f1")
        .build();
    
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
          .setMetric(MetricLiteralFilter.newBuilder()
              .setMetric("sys.cpu.user")
              .build())
          .setFilterId("f1")
          .setId("m1")
          .build(),
     DownsampleConfig.newBuilder()
          .setAggregator("sum")
          .setId("foo")
          .setInterval("15s")
          .setStart("1514764800")
          .setEnd("1514768400")
            .addInterpolatorConfig(numeric_config)
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
      .setEnd("1514768400")
      .setTimeZone("America/Denver")
      .addFilter(filter)
      .setExecutionGraph(graph)
      .build();
    
    assertSame(filter.getFilter(), query.getFilter("f1"));
    assertNull(query.getFilter("nosuchfilter"));
    
    graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
          .setMetric(MetricLiteralFilter.newBuilder()
              .setMetric("sys.cpu.user")
              .build())
          .setId("m1")
          .build(),
     DownsampleConfig.newBuilder()
          .setAggregator("sum")
          .setId("foo")
          .setInterval("15s")
          .setStart("1514764800")
          .setEnd("1514768400")
            .addInterpolatorConfig(numeric_config)
            .build());
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setTimeZone("America/Denver")
        .setExecutionGraph(graph)
        .build();
    assertNull(query.getFilter("f1"));
  }
}
