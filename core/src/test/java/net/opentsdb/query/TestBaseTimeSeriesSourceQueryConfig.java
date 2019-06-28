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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.time.temporal.TemporalAmount;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.processor.topn.TopNConfig;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Pair;

public class TestBaseTimeSeriesSourceQueryConfig {

  @Test
  public void builder() throws Exception {
    UTConfig.Builder builder = UTConfig.createBuilder();
    builder.setMetric(MetricLiteralFilter.newBuilder()
        .setMetric("system.cpu.user")
        .build()).setSummaryInterval("1h");
    builder.setSourceId("HBase")
        .setNamespace("Verizon")
        .addPushDownNode(mock(QueryNodeConfig.class))
        .addRollupInterval("1h")
        .addSummaryAggregation("sum")
        .setPrePadding("30m")
        .setPostPadding("1h")
        .setTimeShiftInterval("1d")
        .setId("UT");
    UTConfig config = builder.build();
    assertEquals("HBase", config.getSourceId());
    assertEquals("Verizon", config.getNamespace());
    assertEquals("system.cpu.user", config.getMetric().getMetric());
    assertEquals("UT", config.getId());
    assertEquals(1, config.getPushDownNodes().size());
    assertEquals(1, config.getRollupIntervals().size());
    assertTrue(config.getRollupIntervals().contains("1h"));
    assertEquals("1h", config.getSummaryInterval());
    assertEquals(1, config.getSummaryAggregations().size());
    assertTrue(config.getSummaryAggregations().contains("sum"));
    assertEquals("30m", config.getPrePadding());
    assertEquals("1h", config.getPostPadding());
    assertFalse(config.pushDown());
    Pair<Boolean, TemporalAmount> pair = config.timeShifts();
    assertTrue(pair.getKey());
    assertEquals(DateTime.parseDuration2("1d"), pair.getValue());

    try {
      UTConfig.createBuilder()
        //.setMetric("system.cpu.user")
        .setId("UT")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void builderClone() throws Exception {
    UTConfig.Builder builder = UTConfig.createBuilder();
    builder.setMetric(MetricLiteralFilter.newBuilder()
        .setMetric("system.cpu.user")
        .build()).setSummaryInterval("1h");
    builder.setSourceId("HBase")
        .setNamespace("Verizon")
        .setTypes(Lists.newArrayList("Numeric", "Annotation"))
        .addPushDownNode(mock(QueryNodeConfig.class))
        .addRollupInterval("1h")
        .addSummaryAggregation("sum")
        .setPrePadding("30m")
        .setPostPadding("1h")
        .setTimeShiftInterval("1d")
        .setId("UT").build();
    UTConfig config = builder.build();

    builder = UTConfig.createBuilder();

    UTConfig.cloneBuilder(config, builder);
    UTConfig clone = builder.build();

    assertNotSame(config, clone);
    assertEquals("HBase", clone.getSourceId());
    assertEquals("Verizon", config.getNamespace());
    assertNotSame(config.getTypes(), clone.getTypes());
    assertEquals(2, clone.getTypes().size());
    assertTrue(clone.getTypes().contains("Numeric"));
    assertTrue(clone.getTypes().contains("Annotation"));
    assertSame(config.getMetric(), clone.getMetric());
    assertEquals("UT", clone.getId());
    assertEquals(1, clone.getPushDownNodes().size());
    assertTrue(clone.getPushDownNodes().get(0) instanceof QueryNodeConfig);
    assertEquals(1, config.getRollupIntervals().size());
    assertTrue(config.getRollupIntervals().contains("1h"));
    assertEquals("1h", config.getSummaryInterval());
    assertEquals(1, config.getSummaryAggregations().size());
    assertTrue(config.getSummaryAggregations().contains("sum"));
    assertEquals("30m", config.getPrePadding());
    assertEquals("1h", config.getPostPadding());
    Pair<Boolean, TemporalAmount> pair = config.timeShifts();
    assertTrue(pair.getKey());
    assertEquals(DateTime.parseDuration2("1d"), pair.getValue());
  }

  @Test
  public void serdes() throws Exception {
    UTConfig.Builder builder = UTConfig.createBuilder();
    builder.setMetric(MetricLiteralFilter.newBuilder()
        .setMetric("system.cpu.user")
        .build()).setSummaryInterval("1h");

    builder.setSourceId("HBase")
        .setNamespace("Verizon")
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
        .addRollupInterval("1h")
        .addSummaryAggregation("sum")
        .setPrePadding("30m")
        .setPostPadding("1h")
        .setTimeShiftInterval("1d")
        .setId("UT");
    UTConfig config = builder.build();

    final String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"sourceId\":\"HBase\""));
    assertTrue(json.contains("\"id\":\"UT\""));
    assertTrue(json.contains("\"namespace\":\"Verizon\""));
    assertTrue(json.contains("\"metric\":{"));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
    assertTrue(json.contains("\"type\":\"MetricLiteral\""));
    assertTrue(json.contains("\"filterId\":\"f1\""));
    assertTrue(json.contains("\"fetchLast\":true"));
    assertTrue(json.contains("\"pushDownNodes\":["));
    assertTrue(json.contains("\"id\":\"Toppy\""));
    assertTrue(json.contains("\"type\":\"TopN\""));
    assertTrue(json.contains("\"rollupIntervals\":[\"1h\"]"));
    assertTrue(json.contains("\"summaryInterval\":\"1h\""));
    assertTrue(json.contains("\"summaryAggregations\":[\"sum\"]"));
    assertTrue(json.contains("\"prePadding\":\"30m\""));
    assertTrue(json.contains("\"postPadding\":\"1h\""));
    assertTrue(json.contains("\"timeShiftInterval\":\"1d\""));
    MockTSDB tsdb = MockTSDBDefault.getMockTSDB();
    JsonNode root = JSON.getMapper().readTree(json);
    builder = UTConfig.createBuilder();
    DefaultTimeSeriesDataSourceConfig.parseConfig(JSON.getMapper(),
            tsdb, root, builder);

    assertEquals("HBase", config.getSourceId());
    assertEquals("Verizon", config.getNamespace());
    assertEquals("system.cpu.user", config.getMetric().getMetric());
    assertEquals("UT", config.getId());
    assertEquals(1, config.getPushDownNodes().size());
    assertTrue(config.getPushDownNodes().get(0) instanceof TopNConfig);
    assertEquals("f1", config.getFilterId());
    assertTrue(config.getFetchLast());
    assertEquals("web01", ((TagValueLiteralOrFilter) config.getFilter()).getFilter());
    assertEquals(1, config.getRollupIntervals().size());
    assertTrue(config.getRollupIntervals().contains("1h"));
    assertEquals("1h", config.getSummaryInterval());
    assertEquals(1, config.getSummaryAggregations().size());
    assertTrue(config.getSummaryAggregations().contains("sum"));
    assertEquals("30m", config.getPrePadding());
    assertEquals("1h", config.getPostPadding());
    assertEquals("1d", config.getTimeShiftInterval());
  }
  
  static class UTConfig extends DefaultTimeSeriesDataSourceConfig {

    protected UTConfig(Builder builder) {
      super(builder);
    }

    static Builder createBuilder() {
      return new Builder();
    }

//    static void cloneBuilder(UTConfig config, Builder builder) {
//
//    }

    static class Builder extends DefaultTimeSeriesDataSourceConfig.Builder {
      @Override
      public UTConfig build() {
        return new UTConfig(this);
      }

    }
    
  }
}
