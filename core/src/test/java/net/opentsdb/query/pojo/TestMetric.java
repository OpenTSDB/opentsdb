// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.pojo;

import net.opentsdb.core.FillPolicy;
import net.opentsdb.query.expression.NumericFillPolicy;
import net.opentsdb.utils.JSON;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMetric {
  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenMetricIsNull() throws Exception {
    String json = "{\"id\":\"1\",\"filter\":\"2\","
        + "\"timeOffset\":\"1h-ago\",\"aggregator\":\"sum\","
        + "\"fillPolicy\":{\"policy\":\"nan\"}}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenMetricIsEmpty() throws Exception {
    String json = "{\"metric\":\"\",\"id\":\"1\",\"filter\":\"2\","
        + "\"timeOffset\":\"1h-ago\",\"aggregator\":\"sum\","
        + "\"fillPolicy\":{\"policy\":\"nan\"}}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenIDIsNull() throws Exception {
    String json = "{\"metric\":\"system.cpu\",\"id\":null,\"filter\":\"2\","
        + "\"timeOffset\":\"1h-ago\",\"aggregator\":\"sum\","
        + "\"fillPolicy\":{\"policy\":\"nan\"}}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenIDIsEmpty() throws Exception {
    String json = "{\"metric\":\"system.cpu\",\"id\":\"\",\"filter\":\"2\","
        + "\"timeOffset\":\"1h-ago\",\"aggregator\":\"sum\","
        + "\"fillPolicy\":{\"policy\":\"nan\"}}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenIDIsInvalid() throws Exception {
    String json = "{\"metric\":\"system.cpu\",\"id\":\"system.cpu\",\"filter\":\"2\","
        + "\"timeOffset\":\"1h-ago\",\"aggregator\":\"sum\","
        + "\"fillPolicy\":{\"policy\":\"nan\"}}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
  }
  
  @Test
  public void deserializeAllFields() throws Exception {
    String json = "{\"metric\":\"YAMAS.cpu.idle\",\"id\":\"e1\",\"filter\":\"f2\","
        + "\"timeOffset\":\"1h-ago\",\"aggregator\":\"sum\","
        + "\"fillPolicy\":{\"policy\":\"nan\"}}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
    Metric expectedMetric = Metric.Builder().setMetric("YAMAS.cpu.idle")
        .setId("e1").setFilter("f2").setTimeOffset("1h-ago")
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy(FillPolicy.NOT_A_NUMBER))
        .build();
    
    assertEquals(expectedMetric, metric);
  }

  @Test
  public void serialize() throws Exception {
    Metric metric = Metric.Builder().setMetric("YAMAS.cpu.idle")
        .setId("e1").setFilter("f2").setTimeOffset("1h-ago")
        .setFillPolicy(new NumericFillPolicy(FillPolicy.NOT_A_NUMBER))
        .build();

    String actual = JSON.serializeToString(metric);
    assertTrue(actual.contains("\"metric\":\"YAMAS.cpu.idle\""));
    assertTrue(actual.contains("\"id\":\"e1\""));
    assertTrue(actual.contains("\"filter\":\"f2\""));
    assertTrue(actual.contains("\"timeOffset\":\"1h-ago\""));
    assertTrue(actual.contains("\"fillPolicy\":{"));
  }

  @Test
  public void unknownShouldBeIgnored() throws Exception {
    String json = "{\"aggregator\":\"sum\",\"tags\":[\"foo\",\"bar\"],\"unknown\":\"garbage\"}";
    JSON.parseToObject(json, Metric.class);
    // pass if no unexpected exception
  }

  @Test(expected = IllegalArgumentException.class)
  public void validationtErrorWhenTimeOffsetIsInvalid() throws Exception {
    String json = "{\"metric\":\"YAMAS.cpu.idle\",\"id\":\"1\",\"filter\":\"2\","
        + "\"timeOffset\":\"what?\"}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void validationtErrorBadFill() throws Exception {
    String json = "{\"metric\":\"YAMAS.cpu.idle\",\"id\":\"1\",\"filter\":\"2\","
        + "\"fillPolicy\":{\"policy\":\"zero\",\"value\":42}}";
    Metric metric = JSON.parseToObject(json, Metric.class);
    metric.validate();
  }
}
