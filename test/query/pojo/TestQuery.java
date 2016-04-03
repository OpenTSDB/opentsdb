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
import net.opentsdb.query.expression.VariableIterator.SetOperator;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.utils.JSON;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestQuery {
  Timespan time;
  TagVFilter tag;
  Filter filter;
  Metric metric;
  Expression expression;
  Output output;

  String json = "{"
      + "  \"time\":{"
      + "    \"start\":\"3h-ago\","
      + "    \"end\":\"1h-ago\","
      + "    \"timezone\":\"UTC\","
      + "    \"aggregator\":\"avg\","
      + "    \"downsampler\":{\"interval\":\"15m\","
      + "      \"aggregator\":\"avg\","
      + "      \"fillPolicy\":{\"policy\":\"nan\"}}"
      + "  },"
      + "  \"filters\":["
      + "    {"
      + "      \"id\":\"f1\","
      + "      \"tags\":["
      + "        {"
      + "          \"tagk\":\"host\","
      + "          \"filter\":\"*\","
      + "          \"type\":\"iwildcard\","
      + "          \"groupBy\":false"
      + "        }"
      + "      ]"
      + "    }"
      + "  ],"
      + "  \"metrics\":["
      + "    {"
      + "      \"metric\":\"YAMAS.cpu.idle\","
      + "      \"id\":\"m1\","
      + "      \"filter\":\"f1\","
      + "      \"aggregator\":\"sum\","
      + "      \"timeOffset\":\"0\""
      + "    }"
      + "  ],"
      + "  \"expressions\":["
      + "    {"
      + "      \"id\":\"e1\","
      + "      \"expr\":\"a + b + c\""
      + "    }"
      + "  ],"
      + "  \"outputs\":["
      + "    {"
      + "      \"id\":\"m1\","
      + "      \"alias\":\"CPU Idle EAST DC\""
      + "    }"
      + "  ]"
      + "}";

  @Before
  public void setup() {
    time = Timespan.Builder().setStart("3h-ago").setAggregator("avg")
        .setEnd("1h-ago").setTimezone("UTC").setDownsampler(
            Downsampler.Builder().setInterval("15m").setAggregator("avg")
            .setFillPolicy(new NumericFillPolicy(FillPolicy.NOT_A_NUMBER)).build())
        .build();
    TagVFilter tag = new TagVFilter.Builder().setFilter("*").setGroupBy(
        false)
        .setTagk("host").setType("iwildcard").build();
    filter = Filter.Builder().setId("f1").setTags(Arrays.asList(tag)).build();
    metric = Metric.Builder().setMetric("YAMAS.cpu.idle")
        .setId("m1").setFilter("f1").setTimeOffset("0")
        .setAggregator("sum").build();
    expression = Expression.Builder().setId("e1")
        .setExpression("a + b + c").setJoin(
            Join.Builder().setOperator(SetOperator.UNION).build()).build();
    output = Output.Builder().setId("m1").setAlias("CPU Idle EAST DC")
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenTimeIsNull() throws Exception {
    Query query = getDefaultQueryBuilder().setTime(null).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidTime() throws Exception {
    Timespan invalidTime = Timespan.Builder().build();
    Query query = getDefaultQueryBuilder().setTime(invalidTime).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void metricsIsNull() throws Exception {
    Query query = getDefaultQueryBuilder().setMetrics(null).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void metricsIsEmpty() throws Exception {
    Query query = getDefaultQueryBuilder().setMetrics(
        Collections.<Metric>emptyList()).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidMetric() throws Exception {
    Metric invalidMetric = Metric.Builder().build();
    Query query = getDefaultQueryBuilder()
        .setMetrics(Arrays.asList(invalidMetric)).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidFilter() throws Exception {
    Filter invalidFilter = Filter.Builder().build();
    Query query = getDefaultQueryBuilder()
        .setFilters(Arrays.asList(invalidFilter)).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidExpression() throws Exception {
    Expression invalidExpression = Expression.Builder().build();
    Query query = getDefaultQueryBuilder()
        .setExpressions(Arrays.asList(invalidExpression)).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void noSuchFilterIdInMetric() throws Exception {
    Metric invalid_metric = Metric.Builder().setMetric("YAMAS.cpu.idle")
        .setId("m2").setFilter("f2").setTimeOffset("0").build();
    Query query = getDefaultQueryBuilder().setMetrics(
        Arrays.asList(invalid_metric, metric)).build();
    query.validate();
  }

  @Test
  public void deserialize() throws Exception {
    Query query = JSON.parseToObject(json, Query.class);
    query.validate();
    Query expected = Query.Builder().setExpressions(Arrays.asList(expression))
        .setFilters(Arrays.asList(filter)).setMetrics(Arrays.asList(metric))
        .setTime(time).setOutputs(Arrays.asList(output)).build();
    assertEquals(expected, query);
  }

  @Test(expected = IllegalArgumentException.class)
  public void duplicatedFilterId() throws Exception {
    Query query = getDefaultQueryBuilder().setFilters(
        Arrays.asList(filter, filter)).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void duplicatedExpressionId() throws Exception {
    Query query = getDefaultQueryBuilder().setExpressions(
        Arrays.asList(expression, expression)).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void duplicatedMetricId() throws Exception {
    Query query = getDefaultQueryBuilder().setMetrics(
        Arrays.asList(metric, metric)).build();
    query.validate();
  }

  @Test
  public void serialize() throws Exception {
    Query query = Query.Builder().setExpressions(Arrays.asList(expression))
        .setFilters(Arrays.asList(filter)).setMetrics(Arrays.asList(metric))
        .setName("q1").setTime(time).setOutputs(Arrays.asList(output)).build();

    String actual = JSON.serializeToString(query);
//    String expected = "{\"name\":\"q1\",\"time\":{\"start\":\"3h-ago\"," 
//        + "\"end\":\"1h-ago\",\"timezone\":\"UTC\",\"downsample\":\"15m-avg-nan\"," 
//        + "\"interpolation\":\"LERP\"},\"filters\":[{\"id\":\"f1\"," 
//        + "\"tags\":[{\"tagk\":\"host\",\"filter\":\"*\",\"group_by\":false," 
//        + "\"type\":\"iwildcard\"}],\"aggregator\":\"sum\"}],"
//        + "\"metrics\":[{\"metric\":\"YAMAS.cpu.idle\"," 
//        + "\"id\":\"m1\",\"filter\":\"f1\",\"time_offset\":\"0\"}],"
//        + "\"expressions\":[{\"id\":\"e1\",\"expr\":\"a + b + c\"}],"
//        + "\"outputs\":[{\"var\":\"q1.m1\",\"alias\":\"CPU Idle EAST DC\"}]}";
    assertTrue(actual.contains("\"name\":\"q1\""));
    assertTrue(actual.contains("\"start\":\"3h-ago\""));
    assertTrue(actual.contains("\"end\":\"1h-ago\""));
    assertTrue(actual.contains("\"timezone\":\"UTC\""));
    // TODO - finish the assertions
  }

  private Query.Builder getDefaultQueryBuilder() {
    return Query.Builder().setExpressions(Arrays.asList(expression))
          .setFilters(Arrays.asList(filter)).setMetrics(Arrays.asList(metric))
          .setName("q1").setTime(time).setOutputs(Arrays.asList(output));
  }
}
