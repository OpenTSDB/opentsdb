// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Join.SetOperator;
import net.opentsdb.utils.JSON;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
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
      + "      \"expr\":\"m1 * 1024\""
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
    time = Timespan.newBuilder().setStart("3h-ago").setAggregator("avg")
        .setEnd("1h-ago").setTimezone("UTC").setDownsampler(
            Downsampler.newBuilder().setInterval("15m").setAggregator("avg")
            .setFillPolicy(new NumericFillPolicy(FillPolicy.NOT_A_NUMBER)).build())
        .build();
    TagVFilter tag = new TagVFilter.Builder().setFilter("*").setGroupBy(
        false)
        .setTagk("host").setType("iwildcard").build();
    filter = Filter.newBuilder().setId("f1").setTags(Arrays.asList(tag)).build();
    metric = Metric.newBuilder().setMetric("YAMAS.cpu.idle")
        .setId("m1").setFilter("f1").setTimeOffset("0")
        .setAggregator("sum").build();
    expression = Expression.newBuilder().setId("e1")
        .setExpression("m1 * 1024").setJoin(
            Join.newBuilder().setOperator(SetOperator.UNION).build()).build();
    output = Output.newBuilder().setId("m1").setAlias("CPU Idle EAST DC")
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenTimeIsNull() throws Exception {
    Query query = getDefaultQueryBuilder().setTime(null).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidTime() throws Exception {
    Timespan invalidTime = Timespan.newBuilder().build();
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
    Metric invalidMetric = Metric.newBuilder().build();
    Query query = getDefaultQueryBuilder()
        .setMetrics(Arrays.asList(invalidMetric)).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidFilter() throws Exception {
    Filter invalidFilter = Filter.newBuilder().build();
    Query query = getDefaultQueryBuilder()
        .setFilters(Arrays.asList(invalidFilter)).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidExpression() throws Exception {
    Expression invalidExpression = Expression.newBuilder().build();
    Query query = getDefaultQueryBuilder()
        .setExpressions(Arrays.asList(invalidExpression)).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void noSuchFilterIdInMetric() throws Exception {
    Metric invalid_metric = Metric.newBuilder().setMetric("YAMAS.cpu.idle")
        .setId("m2").setFilter("f2").setTimeOffset("0").build();
    Query query = getDefaultQueryBuilder().setMetrics(
        Arrays.asList(invalid_metric, metric)).build();
    query.validate();
  }

  @Test
  public void deserialize() throws Exception {
    Query query = JSON.parseToObject(json, Query.class);
    query.validate();
    Query expected = Query.newBuilder().setExpressions(Arrays.asList(expression))
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
    Query query = Query.newBuilder().setExpressions(Arrays.asList(expression))
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

  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    Filter f1 = new Filter.Builder()
        .setId("f1")
        .setExplicitTags(false)
        .setTags(Lists.newArrayList(
            new TagVFilter.Builder()
              .setFilter("web01")
              .setTagk("host")
              .setType("literal_or")
              .setGroupBy(false)
              .build(),
            new TagVFilter.Builder()
              .setFilter("phx*")
              .setTagk("dc")
              .setType("wildcard")
              .setGroupBy(true)
              .build()))
        .build();
    
    Output o1 = new Output.Builder()
        .setId("out1")
        .setAlias("MyMetric")
        .build();
    
    Expression e1 = new Expression.Builder()
        .setId("e1")
        .setExpression("a + b")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .setJoin(new Join.Builder()
            .setOperator(SetOperator.INTERSECTION)
            .build())
        .build();
    
    Metric m1 = new Metric.Builder()
        .setId("m1")
        .setFilter("f1")
        .setMetric("sys.cpu.user")
        .setTimeOffset("1h-ago")
        .setAggregator("sum")
        .setFillPolicy(new NumericFillPolicy.Builder()
            .setPolicy(FillPolicy.NOT_A_NUMBER)
            .build())
        .build();
    
    final Query q1 = new Query.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .build();
    
    Query q2 = new Query.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .build();
    assertEquals(q1.hashCode(), q2.hashCode());
    assertEquals(q1, q2);
    assertEquals(0, q1.compareTo(q2));
    
    q2 = new Query.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(e1, expression))  // <-- diff order
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .build();
    assertEquals(q1.hashCode(), q2.hashCode());
    assertEquals(q1, q2);
    assertEquals(0, q1.compareTo(q2));
    
    q2 = new Query.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(f1, filter))  // <-- diff order 
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .build();
    assertEquals(q1.hashCode(), q2.hashCode());
    assertEquals(q1, q2);
    assertEquals(0, q1.compareTo(q2));
    
    q2 = new Query.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(m1, metric))  // <-- diff order 
        .setOutputs(Arrays.asList(output, o1))
        .build();
    assertEquals(q1.hashCode(), q2.hashCode());
    assertEquals(q1, q2);
    assertEquals(0, q1.compareTo(q2));
    
    q2 = new Query.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(o1, output))  // <-- diff order 
        .build();
    assertEquals(q1.hashCode(), q2.hashCode());
    assertEquals(q1, q2);
    assertEquals(0, q1.compareTo(q2));
    
    q2 = new Query.Builder()
        .setName("q2")  // <-- diff
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(-1, q1.compareTo(q2));
    
    q2 = new Query.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression))  // <-- diff
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(-1, q1.compareTo(q2));
    
    q2 = new Query.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter))  // <-- diff
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(1, q1.compareTo(q2));
    
    q2 = new Query.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric))  // <-- diff
        .setOutputs(Arrays.asList(output, o1))
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(1, q1.compareTo(q2));
    
    q2 = new Query.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output))  // <-- diff
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(1, q1.compareTo(q2));
    
    q2 = new Query.Builder()
        .setName("q1")
        .setTime(time)
        //.setExpressions(Arrays.asList(expression, e1))  // <-- diff
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(1, q1.compareTo(q2));
    
    q2 = new Query.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        //.setFilters(Arrays.asList(filter, f1))  // <-- diff
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(1, q1.compareTo(q2));
    
    q2 = new Query.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        //.setOutputs(Arrays.asList(output, o1))  // <-- diff
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(1, q1.compareTo(q2));
  }

  private Query.Builder getDefaultQueryBuilder() {
    return Query.newBuilder().setExpressions(Arrays.asList(expression))
          .setFilters(Arrays.asList(filter)).setMetrics(Arrays.asList(metric))
          .setName("q1").setTime(time).setOutputs(Arrays.asList(output));
  }
}
