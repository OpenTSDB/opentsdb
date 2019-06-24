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

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.graph.MutableGraph;
import net.opentsdb.query.filter.*;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.processor.topn.TopNConfig;
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
        .setPreviousIntervals(1)
        .setMetric(MetricLiteralFilter.newBuilder().setMetric("system.cpu.use").build())
        .setId("UT")
        .setSources(Lists.newArrayList("colo1", "colo2"))
        .build();

    DefaultTimeSeriesDataSourceConfig.setupTimeShift((TimeSeriesDataSourceConfig) build, planner);
  }


  @Test
  public void equality() throws Exception {
    AnyFieldRegexFilter filter = AnyFieldRegexFilter.newBuilder()
            .setFilter("ogg-01.ops.ankh.morpork.com")
            .build();

    QueryNodeConfig config = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setSourceId("HBase")
            .setNamespace("Verizon")
            .setTypes(Lists.newArrayList("type1, type2"))
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("system.cpu.user")
                    .build())
            .setFilterId("f1")
            .setQueryFilter(filter)
            .setFetchLast(true)
            .setTimeShiftInterval("1m")
            .setPreviousIntervals(1)
            .setId("c1")
            .build();

    AnyFieldRegexFilter filter2 = AnyFieldRegexFilter.newBuilder()
            .setFilter("ogg-01.ops.ankh.morpork.com")
            .build();

    QueryNodeConfig config2 = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setSourceId("HBase")
            .setNamespace("Verizon")
            .setTypes(Lists.newArrayList("type1, type2"))
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("system.cpu.user")
                    .build())
            .setFilterId("f1")
            .setQueryFilter(filter2)
            .setFetchLast(true)
            .setTimeShiftInterval("1m")
            .setPreviousIntervals(1)
            .setId("c1")
            .build();

    AnyFieldRegexFilter filter3 = AnyFieldRegexFilter.newBuilder()
            .setFilter("ogg-01.ops.ankh.morpork.com")
            .build();

    QueryNodeConfig config3 = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setSourceId("DB")     // DIFF
            .setNamespace("Verizon")
            .setTypes(Lists.newArrayList("type1, type2"))
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("system.cpu.user")
                    .build())
            .setFilterId("f1")
            .setQueryFilter(filter3)
            .setFetchLast(true)
            .setTimeShiftInterval("1m")
            .setPreviousIntervals(1)
            .setId("c1")
            .build();


    assertTrue(config.equals(config2));
    assertTrue(!config.equals(config3));
    assertEquals(config.hashCode(), config2.hashCode());
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setSourceId("HBase")
            .setNamespace("VDMS")     // DIFF
            .setTypes(Lists.newArrayList("type1, type2"))
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("system.cpu.user")
                    .build())
            .setFilterId("f1")
            .setQueryFilter(filter3)
            .setFetchLast(true)
            .setTimeShiftInterval("1m")
            .setPreviousIntervals(1)
            .setId("c1")
            .build();


    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());


    config3 = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setSourceId("HBase")
            .setNamespace("Verizon")
            .setTypes(Lists.newArrayList("type1, type2", "type3"))     // DIFF
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("system.cpu.user")
                    .build())
            .setFilterId("f1")
            .setQueryFilter(filter3)
            .setFetchLast(true)
            .setTimeShiftInterval("1m")
            .setPreviousIntervals(1)
            .setId("c1")
            .build();


    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());


    config3 = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setSourceId("HBase")
            .setNamespace("Verizon")
            .setTypes(Lists.newArrayList("type1, type2"))
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("system.cpu.pct")
                    .build())     // DIFF
            .setFilterId("f1")
            .setQueryFilter(filter3)
            .setFetchLast(true)
            .setTimeShiftInterval("1m")
            .setPreviousIntervals(1)
            .setId("c1")
            .build();


    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setSourceId("HBase")
            .setNamespace("Verizon")
            .setTypes(Lists.newArrayList("type1, type2"))
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("system.cpu.user")
                    .build())
            .setFilterId("f2")     // DIFF
            .setQueryFilter(filter3)
            .setFetchLast(true)
            .setTimeShiftInterval("1m")
            .setPreviousIntervals(1)
            .setId("c1")
            .build();


    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    filter3 = AnyFieldRegexFilter.newBuilder()
            .setFilter("ogg-01.ops.ankh.diff.com")
            .build();

    config3 = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setSourceId("HBase")
            .setNamespace("Verizon")
            .setTypes(Lists.newArrayList("type1, type2"))
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("system.cpu.user")
                    .build())
            .setFilterId("f1")
            .setQueryFilter(filter3)     // DIFF
            .setFetchLast(true)
            .setTimeShiftInterval("1m")
            .setPreviousIntervals(1)
            .setId("c1")
            .build();

    filter3 = AnyFieldRegexFilter.newBuilder()
            .setFilter("ogg-01.ops.ankh.morpork.com")
            .build();


    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setSourceId("HBase")
            .setNamespace("Verizon")
            .setTypes(Lists.newArrayList("type1, type2"))
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("system.cpu.user")
                    .build())
            .setFilterId("f1")
            .setQueryFilter(filter3)
            .setFetchLast(false)     // DIFF
            .setTimeShiftInterval("1m")
            .setPreviousIntervals(1)
            .setId("c1")
            .build();


    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setSourceId("HBase")
            .setNamespace("Verizon")
            .setTypes(Lists.newArrayList("type1, type2"))
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("system.cpu.user")
                    .build())
            .setFilterId("f1")
            .setQueryFilter(filter3)
            .setFetchLast(true)
            .setTimeShiftInterval("1h")     // DIFF
            .setPreviousIntervals(1)
            .setId("c1")
            .build();


    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setSourceId("HBase")
            .setNamespace("Verizon")
            .setTypes(Lists.newArrayList("type1, type2"))
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("system.cpu.user")
                    .build())
            .setFilterId("f1")
            .setQueryFilter(filter3)
            .setFetchLast(true)
            .setTimeShiftInterval("1m")
            .setPreviousIntervals(1)
            .setId("c2")     // DIFF
            .build();


    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setSourceId("HBase")
            .setNamespace("Verizon")
            .setTypes(Lists.newArrayList("type1, type2"))
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("system.cpu.user")
                    .build())
            .setFilterId("f1")
            .setQueryFilter(filter3)
            .setFetchLast(true)
            .setId("c2")
            .build();


    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());



  }




}

