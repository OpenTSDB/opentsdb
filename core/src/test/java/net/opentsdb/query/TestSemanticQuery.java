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

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.execution.serdes.JsonV2QuerySerdesOptions;
import net.opentsdb.query.filter.DefaultNamedFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.NamedFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
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
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("g1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.user")
                    .build())
                .setFilterId("f1")
                .setId("m1")
                .build()))
        .build();
    
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
    assertEquals(1, query.getExecutionGraph().getNodes().size());
    assertEquals("ds", query.getSerdesConfigs().get(0).getFilter().get(0));
    
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
          //.setStart("1514764800")
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
    
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("g1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.user")
                    .build())
                .setFilterId("f1")
                .setId("m1")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("ds")
            .setType("downsample")
            .addSource("DataSource")
            .setConfig(DownsampleConfig.newBuilder()
                .setAggregator("sum")
                .setId("foo")
                .setInterval("15s")
                .setStart("1514764800")
                .setEnd("1514768400")
                .addInterpolatorConfig(numeric_config)
                .build())
            .build())
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setTimeZone("America/Denver")
        .addFilter(filter)
        .setExecutionGraph(graph)
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
    assertTrue(json.contains("\"executionGraph\":{"));
    assertTrue(json.contains("\"id\":\"g1\""));
    assertTrue(json.contains("\"id\":\"DataSource\""));
    assertTrue(json.contains("\"type\":\"DataSource\""));
    assertTrue(json.contains("\"metric\":{"));
    assertTrue(json.contains("\"id\":\"ds\""));
    assertTrue(json.contains("\"sources\":[\"DataSource\"]"));
    assertTrue(json.contains("\"interval\":\"15s\""));
    assertTrue(json.contains("\"serdesConfigs\":["));
    assertTrue(json.contains("\"filter\":[\"ds\"]"));
    
    MockTSDB tsdb = new MockTSDB();
    tsdb.registry = new DefaultRegistry(tsdb);
    ((DefaultRegistry) tsdb.registry).initialize(true);
    
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
    assertEquals(2, query.getExecutionGraph().getNodes().size());
    assertEquals("ds", query.getSerdesConfigs().get(0).getFilter().get(0));
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
    
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("g1")
      .addNode(ExecutionGraphNode.newBuilder()
          .setId("DataSource")
          .setConfig(QuerySourceConfig.newBuilder()
              .setMetric(MetricLiteralFilter.newBuilder()
                  .setMetric("sys.cpu.user")
                  .build())
              .setFilterId("f1")
              .setId("m1")
              .build()))
      .addNode(ExecutionGraphNode.newBuilder()
          .setId("ds")
          .setType("downsample")
          .addSource("DataSource")
          .setConfig(DownsampleConfig.newBuilder()
              .setAggregator("sum")
              .setId("foo")
              .setInterval("15s")
              .setStart("1514764800")
              .setEnd("1514768400")
                .addInterpolatorConfig(numeric_config)
                .build())
            .build())
        .build();
    
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
    
    ExecutionGraph.newBuilder()
      .setId("g1")
      .addNode(ExecutionGraphNode.newBuilder()
          .setId("DataSource")
          .setConfig(QuerySourceConfig.newBuilder()
              .setMetric(MetricLiteralFilter.newBuilder()
                  .setMetric("sys.cpu.user")
                  .build())
              .setId("m1")
              .build()))
      .addNode(ExecutionGraphNode.newBuilder()
          .setId("ds")
          .setType("downsample")
          .addSource("DataSource")
          .setConfig(DownsampleConfig.newBuilder()
              .setAggregator("sum")
              .setId("foo")
              .setInterval("15s")
              .setStart("1514764800")
              .setEnd("1514768400")
                .addInterpolatorConfig(numeric_config)
                .build())
            .build())
        .build();
    
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
