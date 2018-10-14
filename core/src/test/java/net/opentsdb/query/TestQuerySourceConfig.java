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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.processor.topn.TopNConfig;
import net.opentsdb.utils.JSON;

public class TestQuerySourceConfig {

  @Test
  public void builder() throws Exception {
    final TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    
    QuerySourceConfig config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setSourceId("HBase")
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .addPushDownNode(mock(QueryNodeConfig.class))
        .setId("UT")
        .build();
    assertEquals("HBase", config.getSourceId());
    assertSame(query, config.query());
    assertEquals("system.cpu.user", config.getMetric().getMetric());
    assertEquals("UT", config.getId());
    assertEquals(1, config.getPushDownNodes().size());
    assertFalse(config.pushDown());
    
    try {
      QuerySourceConfig.newBuilder()
        .setQuery(query)
        //.setMetric("system.cpu.user")
        .setId("UT")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void builderClone() throws Exception {
    final TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    QuerySourceConfig config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setSourceId("HBase")
        .setQuery(query)
        .setTypes(Lists.newArrayList("Numeric", "Annotation"))
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .addPushDownNode(mock(QueryNodeConfig.class))
        .setId("UT")
        .build();
    
    QuerySourceConfig clone = QuerySourceConfig.newBuilder(config).build();
    assertNotSame(config, clone);
    assertEquals("HBase", clone.getSourceId());
    assertSame(query, clone.query());
    assertNotSame(config.getTypes(), clone.getTypes());
    assertEquals(2, clone.getTypes().size());
    assertTrue(clone.getTypes().contains("Numeric"));
    assertTrue(clone.getTypes().contains("Annotation"));
    assertSame(config.getMetric(), clone.getMetric());
    assertEquals("UT", clone.getId());
    assertNull(clone.getPushDownNodes());
  }

  @Test
  public void serdes() throws Exception {
    final TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    QuerySourceConfig config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setSourceId("HBase")
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .setFilterId("f1")
        .setQueryFilter(TagValueLiteralOrFilter.newBuilder()
            .setFilter("web01")
            .setTagKey("host")
            .build())
        .setFetchLast(true)
        .addPushDownNode(TopNConfig.newBuilder()
            .setTop(true)
            .setCount(10)
            .setInfectiousNan(true)
            .setId("Toppy")
            .build())
        .setId("UT")
        .build();
    
    final String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"sourceId\":\"HBase\""));
    assertTrue(json.contains("\"id\":\"UT\""));
    assertTrue(json.contains("\"metric\":{"));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
    assertTrue(json.contains("\"type\":\"MetricLiteral\""));
    assertTrue(json.contains("\"filterId\":\"f1\""));
    assertTrue(json.contains("\"fetchLast\":true"));
    assertTrue(json.contains("\"pushDownNodes\":["));
    assertTrue(json.contains("\"id\":\"Toppy\""));
    assertTrue(json.contains("\"type\":\"TopN\""));
    
    MockTSDB tsdb = new MockTSDB();
    tsdb.registry = new DefaultRegistry(tsdb);
    ((DefaultRegistry) tsdb.registry).initialize(true);
    
    JsonNode root = JSON.getMapper().readTree(json);
    config = (QuerySourceConfig) 
        new QueryDataSourceFactory().parseConfig(JSON.getMapper(), 
            tsdb, root);
    
    assertEquals("HBase", config.getSourceId());
    assertEquals("system.cpu.user", config.getMetric().getMetric());
    assertEquals("UT", config.getId());
    assertEquals(1, config.getPushDownNodes().size());
    assertTrue(config.getPushDownNodes().get(0) instanceof TopNConfig);
    assertEquals("f1", config.getFilterId());
    assertTrue(config.getFetchLast());
    assertNull(config.filter()); // cause the ID is set. Bad.
  }
  
}
