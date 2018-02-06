// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.plan;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;

public class TestSplitMetricPlanner {

  private TimeSeriesQuery query;
  
  @Test
  public void generatePlan() {
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1h-ago")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder().setId("m1").setMetric("sys.cpu.user"))
        .addMetric(Metric.newBuilder().setId("m2").setMetric("sys.cpu.idle"))
        .build();
    
    final QueryPlanner planner = new SplitMetricPlanner(query);
    assertEquals(2, planner.getPlannedQuery().subQueries().size());
    assertEquals(1, planner.getPlannedQuery().subQueries()
        .get(0).getMetrics().size());
    assertEquals("sys.cpu.user", planner.getPlannedQuery().subQueries()
        .get(0).getMetrics().get(0).getMetric());
    assertEquals("1h-ago", planner.getPlannedQuery().subQueries().get(0)
        .getTime().getStart());
    assertEquals(1, planner.getPlannedQuery().subQueries()
        .get(1).getMetrics().size());
    assertEquals("sys.cpu.idle", planner.getPlannedQuery().subQueries()
        .get(1).getMetrics().get(0).getMetric());
    assertEquals("1h-ago", planner.getPlannedQuery().subQueries().get(1)
        .getTime().getStart());
  }
  
  @Test
  public void generatePlanWithFilters() {
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1h-ago")
            .setAggregator("sum"))
        .addFilter(Filter.newBuilder().setId("f1"))
        .addFilter(Filter.newBuilder().setId("f2"))
        .addMetric(Metric.newBuilder().setId("m1").setFilter("f1")
            .setMetric("sys.cpu.user"))
        .addMetric(Metric.newBuilder().setId("m2").setFilter("f2")
            .setMetric("sys.cpu.idle"))
        .build();
    
    final QueryPlanner planner = new SplitMetricPlanner(query);
    assertEquals(2, planner.getPlannedQuery().subQueries().size());
    assertEquals(1, planner.getPlannedQuery().subQueries()
        .get(0).getMetrics().size());
    assertEquals("sys.cpu.user", planner.getPlannedQuery().subQueries()
        .get(0).getMetrics().get(0).getMetric());
    assertEquals("1h-ago", planner.getPlannedQuery().subQueries().get(0)
        .getTime().getStart());
    assertEquals(1, planner.getPlannedQuery().subQueries().get(0)
        .getFilters().size());
    assertEquals("f1", planner.getPlannedQuery().subQueries().get(0)
        .getFilters().get(0).getId());
    assertEquals(1, planner.getPlannedQuery().subQueries()
        .get(1).getMetrics().size());
    assertEquals("sys.cpu.idle", planner.getPlannedQuery().subQueries()
        .get(1).getMetrics().get(0).getMetric());
    assertEquals("1h-ago", planner.getPlannedQuery().subQueries().get(1)
        .getTime().getStart());
    assertEquals(1, planner.getPlannedQuery().subQueries().get(1)
        .getFilters().size());
    assertEquals("f2", planner.getPlannedQuery().subQueries().get(1)
        .getFilters().get(0).getId());
  }
  
  @Test (expected = IllegalStateException.class)
  public void generatePlanMissingFilter() {
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1h-ago")
            .setAggregator("sum"))
        .addFilter(Filter.newBuilder().setId("f1"))
        .addFilter(Filter.newBuilder().setId("f2"))
        .addMetric(Metric.newBuilder().setId("m1").setFilter("f1")
            .setMetric("sys.cpu.user"))
        .addMetric(Metric.newBuilder().setId("m2").setFilter("f3") // <-- doh!
            .setMetric("sys.cpu.idle"))
        .build();
    
    new SplitMetricPlanner(query);
  }
  
  @Test (expected = IllegalStateException.class)
  public void generatePlanNoMetrics() {
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1h-ago")
            .setAggregator("sum"))
        .build();
    
    new SplitMetricPlanner(query);
  }
}
