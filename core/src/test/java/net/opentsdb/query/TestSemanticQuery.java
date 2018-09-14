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
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.DefaultNamedFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.NamedFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;

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
  
}
