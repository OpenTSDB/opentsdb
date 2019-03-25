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
    UTConfig config = (UTConfig) UTConfig.newBuilder()
        .setSourceId("HBase")
        .setNamespace("Verizon")
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .addPushDownNode(mock(QueryNodeConfig.class))
        .addRollupInterval("1h")
        .setSummaryInterval("1h")
        .addSummaryAggregation("sum")
        .setPrePadding("30m")
        .setPostPadding("1h")
        .setTimeShiftInterval("1d")
        .setPreviousIntervals(2)
        .setNextIntervals(1)
        .setId("UT")
        .build();
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
    assertEquals(3, config.timeShifts().size());
    Pair<Boolean, TemporalAmount> pair = config.timeShifts().get("UT-previous-P1D");
    assertTrue(pair.getKey());
    assertEquals(DateTime.parseDuration2("1d"), pair.getValue());
    pair = config.timeShifts().get("UT-previous-P2D");
    assertTrue(pair.getKey());
    assertEquals(DateTime.parseDuration2("2d"), pair.getValue());
    pair = config.timeShifts().get("UT-next-P1D");
    assertFalse(pair.getKey());
    assertEquals(DateTime.parseDuration2("1d"), pair.getValue());
    assertEquals(2, config.getPreviousIntervals());
    assertEquals(1, config.getNextIntervals());
        
    try {
      UTConfig.newBuilder()
        //.setMetric("system.cpu.user")
        .setId("UT")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void builderClone() throws Exception {
    UTConfig config = (UTConfig) UTConfig.newBuilder()
        .setSourceId("HBase")
        .setNamespace("Verizon")
        .setTypes(Lists.newArrayList("Numeric", "Annotation"))
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .addPushDownNode(mock(QueryNodeConfig.class))
        .addRollupInterval("1h")
        .setSummaryInterval("1h")
        .addSummaryAggregation("sum")
        .setPrePadding("30m")
        .setPostPadding("1h")
        .setTimeShiftInterval("1d")
        .setPreviousIntervals(2)
        .setNextIntervals(1)
        .setId("UT")
        .build();
    
    UTConfig.Builder builder = UTConfig.newBuilder();
    UTConfig clone = (UTConfig) UTConfig.newBuilder(config, builder).build();
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
    assertEquals(3, config.timeShifts().size());
    Pair<Boolean, TemporalAmount> pair = config.timeShifts().get("UT-previous-P1D");
    assertTrue(pair.getKey());
    assertEquals(DateTime.parseDuration2("1d"), pair.getValue());
    pair = config.timeShifts().get("UT-previous-P2D");
    assertTrue(pair.getKey());
    assertEquals(DateTime.parseDuration2("2d"), pair.getValue());
    pair = config.timeShifts().get("UT-next-P1D");
    assertFalse(pair.getKey());
    assertEquals(DateTime.parseDuration2("1d"), pair.getValue());
    assertEquals(2, config.getPreviousIntervals());
    assertEquals(1, config.getNextIntervals());
  }

  @Test
  public void serdes() throws Exception {
    UTConfig config = (UTConfig) UTConfig.newBuilder()
        .setSourceId("HBase")
        .setNamespace("Verizon")
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
        .addRollupInterval("1h")
        .setSummaryInterval("1h")
        .addSummaryAggregation("sum")
        .setPrePadding("30m")
        .setPostPadding("1h")
        .setTimeShiftInterval("1d")
        .setPreviousIntervals(2)
        .setNextIntervals(1)
        .setId("UT")
        .build();
    
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
    assertTrue(json.contains("\"previousIntervals\":2"));
    assertTrue(json.contains("\"nextIntervals\":1"));
    MockTSDB tsdb = MockTSDBDefault.getMockTSDB();
    JsonNode root = JSON.getMapper().readTree(json);
    UTConfig.Builder builder = UTConfig.newBuilder();
    BaseTimeSeriesDataSourceConfig.parseConfig(JSON.getMapper(), 
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
    assertEquals(2, config.getPreviousIntervals());
    assertEquals(1, config.getNextIntervals());
    assertEquals(3, config.timeShifts().size());
  }
  
  static class UTConfig extends BaseTimeSeriesDataSourceConfig {

    protected UTConfig(Builder builder) {
      super(builder);
    }
    
    @Override
    public Builder toBuilder() {
      // TODO Auto-generated method stub
      return null;
    }
    
    static Builder newBuilder() {
      return new Builder();
    }
    
    static class Builder extends BaseTimeSeriesDataSourceConfig.Builder {

      @Override
      public TimeSeriesDataSourceConfig build() {
        return new UTConfig(this);
      }
      
      @Override
      public String id() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public String sourceId() {
        // TODO Auto-generated method stub
        return null;
      }
      
    }
    
  }
}
