// This file is part of OpenTSDB.
// Copyright (C) 2019-2021  The OpenTSDB Authors.
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.temporal.TemporalAmount;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.graph.MutableGraph;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.query.filter.AnyFieldRegexFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Pair;

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
    DefaultTimeSeriesDataSourceConfig build = 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setSourceId("HBase")
        .setNamespace("Verizon")
        .setId("UT")
        .setSources(Lists.newArrayList("colo1", "colo2"))
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.use")
            .build())
        .build();

    assertEquals("UT", build.getId());
    assertEquals(2, build.getSources().size());
    assertTrue(build.getSources().contains("colo1"));
    assertTrue(build.getSources().contains("colo2"));
    assertEquals("TimeSeriesDataSource", build.getType());
    assertEquals(1, build.resultIds().size());
    assertEquals(new DefaultQueryResultId("UT", "UT"), build.resultIds().get(0));
    
    build = 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setSourceId("HBase")
        .setNamespace("Verizon")
        .setId("UT")
        .setSources(Lists.newArrayList("colo1", "colo2"))
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.use")
            .build())
        .addResultId(new DefaultQueryResultId("m1", "m1"))
        .build();

    assertEquals("UT", build.getId());
    assertEquals(2, build.getSources().size());
    assertTrue(build.getSources().contains("colo1"));
    assertTrue(build.getSources().contains("colo2"));
    assertEquals("TimeSeriesDataSource", build.getType());
    assertEquals(1, build.resultIds().size());
    assertEquals(new DefaultQueryResultId("m1", "m1"), build.resultIds().get(0));
  }

  @Test
  public void setUpTimeShiftSingleNode() {
    DefaultTimeSeriesDataSourceConfig build = 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setSourceId("HBase")
        .setNamespace("Verizon")
        .setTimeShiftInterval("1h")
        .setId("UT")
        .setSources(Lists.newArrayList("colo1", "colo2"))
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.use")
            .build())
        .build();
    assertEquals("UT", build.getId());
    assertEquals(2, build.getSources().size());
    assertTrue(build.getSources().contains("colo1"));
    assertTrue(build.getSources().contains("colo2"));
    assertEquals("TimeSeriesDataSource", build.getType());
    assertEquals(1, build.resultIds().size());
    assertEquals(new DefaultQueryResultId("UT", "UT"), build.resultIds().get(0));
    assertEquals(new Pair<Boolean, TemporalAmount>(
        true, DateTime.parseDuration2("1h")), build.timeShifts());
  }
  
  @Test
  public void cloneBuilder() {
    DefaultTimeSeriesDataSourceConfig original = 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setSourceId("HBase")
        .setNamespace("Verizon")
        .setTimeShiftInterval("1h")
        .setId("UT")
        .setSources(Lists.newArrayList("colo1", "colo2"))
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.use")
            .build())
        .setDataSource("mym1")
        .setStartTimeStamp(new SecondTimeStamp(1))
        .setEndTimeStamp(new SecondTimeStamp(2))
        .addResultId(new DefaultQueryResultId("m1", "m1"))
        .build();
    
    DefaultTimeSeriesDataSourceConfig.Builder builder = DefaultTimeSeriesDataSourceConfig.newBuilder();
    DefaultTimeSeriesDataSourceConfig.cloneBuilder(original, builder);
    DefaultTimeSeriesDataSourceConfig build = builder.build();
    assertEquals("UT", build.getId());
    assertEquals(2, build.getSources().size());
    assertTrue(build.getSources().contains("colo1"));
    assertTrue(build.getSources().contains("colo2"));
    assertEquals("TimeSeriesDataSource", build.getType());
    assertEquals(1, build.resultIds().size());
    assertEquals(new DefaultQueryResultId("m1", "m1"), build.resultIds().get(0));
    assertEquals(new Pair<Boolean, TemporalAmount>(
        true, DateTime.parseDuration2("1h")), build.timeShifts());
    assertEquals("mym1", build.getDataSource());
    assertEquals(1, build.startTimestamp().epoch());
    assertEquals(2, build.endTimestamp().epoch());
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
            .setQueryFilter(filter)
            .setFetchLast(true)
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

  @Test
  public void serdes() throws Exception {
    // TODO - test the rest.
    DefaultTimeSeriesDataSourceConfig build =
            DefaultTimeSeriesDataSourceConfig.newBuilder()
                    .setSourceId("HBase")
                    .setNamespace("Verizon")
                    .setId("UT")
                    .setMetric(MetricLiteralFilter.newBuilder()
                            .setMetric("system.cpu.use")
                            .build())
                    .setDataSource("m1")
                    .setStartTimeStamp(new SecondTimeStamp(1609372800))
                    .setEndTimeStamp(new SecondTimeStamp(1609416000))
                    .build();

    final String json = JSON.serializeToString(build);
    assertTrue(json.contains("\"id\":\"UT\""));
    assertTrue(json.contains("\"type\":\"TimeSeriesDataSource\""));
    assertTrue(json.contains("\"namespace\":\"Verizon\""));
    assertTrue(json.contains("\"metric\":\"system.cpu.use\""));
    assertTrue(json.contains("\"sourceId\":\"HBase\""));
    assertTrue(json.contains("\"dataSource\":\"m1\""));
    assertTrue(json.contains("\"startOverrideMs\":1609372800000"));
    assertTrue(json.contains("\"endOverrideMs\":1609416000000"));

    MockTSDB tsdb = MockTSDBDefault.getMockTSDB();
    JsonNode node = JSON.getMapper().readTree(json);
    build = DefaultTimeSeriesDataSourceConfig.parseConfig(JSON.getMapper(), tsdb, node);

    assertEquals("UT", build.getId());
    assertEquals("TimeSeriesDataSource", build.getType());
    assertEquals(1, build.resultIds().size());
    assertEquals(new DefaultQueryResultId("UT", "m1"),
            build.resultIds().get(0));
    assertEquals("m1", build.getDataSource());
    assertEquals(1609372800, build.startTimestamp().epoch());
    assertEquals(1609416000, build.endTimestamp().epoch());
  }

}
