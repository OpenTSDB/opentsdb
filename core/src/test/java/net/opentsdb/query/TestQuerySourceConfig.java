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

import com.google.common.collect.Lists;

import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.processor.topn.TopNConfig;
import net.opentsdb.utils.JSON;

public class TestQuerySourceConfig {

  @Test
  public void builder() throws Exception {
    final TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    
    QuerySourceConfig qsc = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .addPushDownNode(mock(ExecutionGraphNode.class))
        .setId("UT")
        .build();
    assertSame(query, qsc.query());
    assertEquals("system.cpu.user", qsc.getMetric().getMetric());
    assertEquals("UT", qsc.getId());
    assertEquals(1, qsc.getPushDownNodes().size());
    assertFalse(qsc.pushDown());
    
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
    QuerySourceConfig qsc = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setTypes(Lists.newArrayList("Numeric", "Annotation"))
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .addPushDownNode(mock(ExecutionGraphNode.class))
        .setId("UT")
        .build();
    
    QuerySourceConfig clone = QuerySourceConfig.newBuilder(qsc).build();
    assertNotSame(qsc, clone);
    assertSame(query, clone.query());
    assertNotSame(qsc.getTypes(), clone.getTypes());
    assertEquals(2, clone.getTypes().size());
    assertTrue(clone.getTypes().contains("Numeric"));
    assertTrue(clone.getTypes().contains("Annotation"));
    assertSame(qsc.getMetric(), clone.getMetric());
    assertEquals("UT", clone.getId());
    assertNull(clone.getPushDownNodes());
  }

  @Test
  public void serialize() throws Exception {
    final TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    QuerySourceConfig qsc = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .setFilterId("f1")
        .setFetchLast(true)
        .addPushDownNode(ExecutionGraphNode.newBuilder()
            .setId("topn")
            .setConfig(TopNConfig.newBuilder()
                .setTop(true)
                .setCount(10)
                .setInfectiousNan(true)
                .setId("Toppy")
                .build())
            .build())
        .setId("UT")
        .build();
    
    final String json = JSON.serializeToString(qsc);
    assertTrue(json.contains("\"id\":\"UT\""));
    assertTrue(json.contains("\"metric\":{"));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
    assertTrue(json.contains("\"type\":\"MetricLiteral\""));
    assertTrue(json.contains("\"filterId\":\"f1\""));
    assertTrue(json.contains("\"fetchLast\":true"));
    assertTrue(json.contains("\"pushDownNodes\":["));
    assertTrue(json.contains("\"id\":\"topn\""));
  }
}
