// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.graph.MutableGraph;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import org.junit.Before;
import org.junit.Test;

public class TestDefaultTimeSeriesDataSourceConfig {

  private DefaultQueryPlanner planner;

  @Before
  public void before() throws Exception {

    planner = mock(DefaultQueryPlanner.class);
    MutableGraph<QueryNodeConfig> graph = mock(MutableGraph.class);
    when(planner.configGraph()).thenReturn(graph);


  }

  @Test
  public void builder() {

    final QueryNodeConfig build = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setSourceId("HBase")
        .setNamespace("Verizon")
        .setMetric(MetricLiteralFilter.newBuilder().setMetric("system.cpu.use").build())
        .setId("UT")
        .setSources(Lists.newArrayList("colo1", "colo2"))
        .build();

    assertEquals("UT", build.getId());
    assertEquals(2, build.getSources().size());
    assertTrue(build.getSources().contains("colo1"));
    assertTrue(build.getSources().contains("colo2"));
    assertEquals("TimeSeriesDataSource", build.getType());

  }

  @Test
  public void setUpTimeShiftSingleNode() {
    final QueryNodeConfig build = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setSourceId("HBase")
        .setNamespace("Verizon")
        .setTimeShiftInterval("1h")
        .setMetric(MetricLiteralFilter.newBuilder().setMetric("system.cpu.use").build())
        .setId("UT")
        .setSources(Lists.newArrayList("colo1", "colo2"))
        .build();

    DefaultTimeSeriesDataSourceConfig.setupTimeShift((TimeSeriesDataSourceConfig) build, planner);
  }

}
