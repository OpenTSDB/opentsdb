// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.query.pojo;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Join.SetOperator;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.expressions.ExpressionConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.utils.JSON;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestTimeSeriesQuery {
  
  private static Configuration configuration;
  
  private Timespan time;
  private Filter filter;
  private Metric metric;
  private Expression expression;
  private Output output;

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
      + "          \"groupBy\":true"
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

  @BeforeClass
  public static void beforeClass() throws Exception {
    configuration = UnitTestConfiguration.getConfiguration();
    configuration.register("tsd.query.test1", 42, false, "UT");
    configuration.register("tsd.query.test2", true, false, "UT");
    configuration.register("tsd.query.test3", "Tyrell", false, "UT");
    configuration.register("tsd.query.test4", null, false, "UT");
  }
  
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
    TimeSeriesQuery query = getDefaultQueryBuilder().setTime((Timespan) null).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidTime() throws Exception {
    Timespan invalidTime = Timespan.newBuilder().build();
    TimeSeriesQuery query = getDefaultQueryBuilder().setTime(invalidTime).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void metricsIsNull() throws Exception {
    TimeSeriesQuery query = getDefaultQueryBuilder().setMetrics(null).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void metricsIsEmpty() throws Exception {
    TimeSeriesQuery query = getDefaultQueryBuilder().setMetrics(
        Collections.<Metric>emptyList()).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidMetric() throws Exception {
    Metric invalidMetric = Metric.newBuilder().build();
    TimeSeriesQuery query = getDefaultQueryBuilder()
        .setMetrics(Arrays.asList(invalidMetric)).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidFilter() throws Exception {
    Filter invalidFilter = Filter.newBuilder().build();
    TimeSeriesQuery query = getDefaultQueryBuilder()
        .setFilters(Arrays.asList(invalidFilter)).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidExpression() throws Exception {
    Expression invalidExpression = Expression.newBuilder().build();
    TimeSeriesQuery query = getDefaultQueryBuilder()
        .setExpressions(Arrays.asList(invalidExpression)).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void noSuchFilterIdInMetric() throws Exception {
    Metric invalid_metric = Metric.newBuilder().setMetric("YAMAS.cpu.idle")
        .setId("m2").setFilter("f2").setTimeOffset("0").build();
    TimeSeriesQuery query = getDefaultQueryBuilder().setMetrics(
        Arrays.asList(invalid_metric, metric)).build();
    query.validate();
  }

  @Test
  public void deserialize() throws Exception {
    TimeSeriesQuery query = JSON.parseToObject(json, TimeSeriesQuery.class);
    query.validate();
    TimeSeriesQuery expected = TimeSeriesQuery.newBuilder().setExpressions(Arrays.asList(expression))
        .setFilters(Arrays.asList(filter)).setMetrics(Arrays.asList(metric))
        .setTime(time).setOutputs(Arrays.asList(output)).build();
    assertEquals(expected, query);
  }

  @Test(expected = IllegalArgumentException.class)
  public void duplicatedFilterId() throws Exception {
    TimeSeriesQuery query = getDefaultQueryBuilder().setFilters(
        Arrays.asList(filter, filter)).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void duplicatedExpressionId() throws Exception {
    TimeSeriesQuery query = getDefaultQueryBuilder().setExpressions(
        Arrays.asList(expression, expression)).build();
    query.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void duplicatedMetricId() throws Exception {
    TimeSeriesQuery query = getDefaultQueryBuilder().setMetrics(
        Arrays.asList(metric, metric)).build();
    query.validate();
  }

  @Test
  public void getFilter() throws Exception {
    TimeSeriesQuery query = getDefaultQueryBuilder().setTime((Timespan) null).build();
    assertEquals("host", query.getFilter("f1").getTags().get(0).getTagk());
    assertNull(query.getFilter("f2"));
    
    query = getDefaultQueryBuilder()
        .setFilters(null)
        .setTime((Timespan) null)
        .build();
    assertNull(query.getFilter("f1"));
    assertNull(query.getFilter("f2"));
  }
  
  @Test
  public void serialize() throws Exception {
    final TimeSeriesQuery query = TimeSeriesQuery.newBuilder().setExpressions(Arrays.asList(expression))
        .setFilters(Arrays.asList(filter)).setMetrics(Arrays.asList(metric))
        .setName("q1").setTime(time).setOutputs(Arrays.asList(output)).build();

    final String json = JSON.serializeToString(query);
    assertTrue(json.contains("\"name\":\"q1\""));
    assertTrue(json.contains("\"start\":\"3h-ago\""));
    assertTrue(json.contains("\"end\":\"1h-ago\""));
    assertTrue(json.contains("\"timezone\":\"UTC\""));
    assertTrue(json.contains("\"downsampler\":{"));
    assertTrue(json.contains("\"interval\":\"15m\""));
    assertTrue(json.contains("\"aggregator\":\"avg\""));
    assertTrue(json.contains("\"filters\":["));
    assertTrue(json.contains("\"id\":\"f1\""));
    assertTrue(json.contains("\"metrics\":["));
    assertTrue(json.contains("\"metric\":\"YAMAS.cpu.idle\""));
    assertTrue(json.contains("\"expressions\":["));
    assertTrue(json.contains("\"id\":\"e1\""));
    assertTrue(json.contains("\"join\":{"));
    assertTrue(json.contains("\"operator\":\"union\""));
    assertTrue(json.contains("\"outputs\":["));
    assertTrue(json.contains("\"id\":\"m1\""));
  }

  @Test
  public void build() throws Exception {
    final TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .addExpression(expression)
        .addFilter(filter)
        .addMetric(metric)
        .setName("q1")
        .setTime(time)
        .addOutput(output)
        .build();
    final TimeSeriesQuery clone = TimeSeriesQuery.newBuilder(query).build();
    assertNotSame(clone, query);
    assertNotSame(clone.getExpressions(), query.getExpressions());
    assertNotSame(clone.getFilters(), query.getFilters());
    assertNotSame(clone.getMetrics(), query.getMetrics());
    assertNotSame(clone.getOutputs(), query.getOutputs());
    assertEquals("q1", clone.getName());
    assertSame(clone.getTime(), query.getTime());
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
    
    final TimeSeriesQuery q1 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .addConfig("key1", "value1")
        .build();
    
    TimeSeriesQuery q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .addConfig("key1", "value1")
        .build();
    assertEquals(q1.hashCode(), q2.hashCode());
    assertArrayEquals(q1.buildTimelessHashCode().asBytes(), 
        q2.buildTimelessHashCode().asBytes());
    assertEquals(q1, q2);
    assertEquals(0, q1.compareTo(q2));
    
    time = Timespan.newBuilder()
        .setStart("2h-ago")  // <-- diff
        .setEnd("1h-ago")
        .setAggregator("avg")
        .setTimezone("UTC").setDownsampler(
            Downsampler.newBuilder().setInterval("15m").setAggregator("avg")
            .setFillPolicy(new NumericFillPolicy(FillPolicy.NOT_A_NUMBER)).build())
        .build();
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .addConfig("key1", "value1")
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertArrayEquals(q1.buildTimelessHashCode().asBytes(), 
        q2.buildTimelessHashCode().asBytes());
    assertNotEquals(q1, q2);
    assertEquals(1, q1.compareTo(q2));
    
    time = Timespan.newBuilder()
        .setStart("3h-ago")  // <-- back to normal
        .setEnd("1h-ago")
        .setAggregator("avg")
        .setTimezone("UTC").setDownsampler(
            Downsampler.newBuilder().setInterval("15m").setAggregator("avg")
            .setFillPolicy(new NumericFillPolicy(FillPolicy.NOT_A_NUMBER)).build())
        .build();
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(e1, expression))  // <-- diff order
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .addConfig("key1", "value1")
        .build();
    assertEquals(q1.hashCode(), q2.hashCode());
    assertEquals(q1, q2);
    assertEquals(0, q1.compareTo(q2));
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(f1, filter))  // <-- diff order 
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .addConfig("key1", "value1")
        .build();
    assertEquals(q1.hashCode(), q2.hashCode());
    assertEquals(q1, q2);
    assertEquals(0, q1.compareTo(q2));
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(m1, metric))  // <-- diff order 
        .setOutputs(Arrays.asList(output, o1))
        .addConfig("key1", "value1")
        .build();
    assertEquals(q1.hashCode(), q2.hashCode());
    assertEquals(q1, q2);
    assertEquals(0, q1.compareTo(q2));
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(o1, output))  // <-- diff order 
        .addConfig("key1", "value1")
        .build();
    assertEquals(q1.hashCode(), q2.hashCode());
    assertEquals(q1, q2);
    assertEquals(0, q1.compareTo(q2));
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q2")  // <-- diff
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .addConfig("key1", "value1")
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(-1, q1.compareTo(q2));
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression))  // <-- diff
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .addConfig("key1", "value1")
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(-1, q1.compareTo(q2));
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter))  // <-- diff
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .addConfig("key1", "value1")
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(1, q1.compareTo(q2));
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric))  // <-- diff
        .setOutputs(Arrays.asList(output, o1))
        .addConfig("key1", "value1")
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(1, q1.compareTo(q2));
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output))  // <-- diff
        .addConfig("key1", "value1")
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(1, q1.compareTo(q2));
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        //.setExpressions(Arrays.asList(expression, e1))  // <-- diff
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .addConfig("key1", "value1")
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(1, q1.compareTo(q2));
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        //.setFilters(Arrays.asList(filter, f1))  // <-- diff
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .addConfig("key1", "value1")
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(1, q1.compareTo(q2));
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        //.setOutputs(Arrays.asList(output, o1))  // <-- diff
        .addConfig("key1", "value1")
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(1, q1.compareTo(q2));
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        .addConfig("key1", "value2")  // <-- diff
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(-1, q1.compareTo(q2));
    
    q2 = new TimeSeriesQuery.Builder()
        .setName("q1")
        .setTime(time)
        .setExpressions(Arrays.asList(expression, e1))
        .setFilters(Arrays.asList(filter, f1))
        .setMetrics(Arrays.asList(metric, m1))
        .setOutputs(Arrays.asList(output, o1))
        //.addConfig("key1", "value1")  // <-- diff
        .build();
    assertNotEquals(q1.hashCode(), q2.hashCode());
    assertNotEquals(q1, q2);
    assertEquals(1, q1.compareTo(q2));
  }

  @Test
  public void getString() throws Exception {
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setFilters(Arrays.asList(filter))
        .setMetrics(Arrays.asList(metric))
        .setName("q1")
        .setTime(time)
        .setOutputs(Arrays.asList(output))
        .addConfig("tsd.query.test6", "foo")
        .build();
    
    assertEquals("foo", query.getString(configuration, "tsd.query.test6"));
    assertEquals("Tyrell", query.getString(configuration, "tsd.query.test3"));
    assertNull(query.getString(configuration, "tsd.query.test7"));
  }
  
  @Test
  public void getInt() throws Exception {
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setFilters(Arrays.asList(filter))
        .setMetrics(Arrays.asList(metric))
        .setName("q1")
        .setTime(time)
        .setOutputs(Arrays.asList(output))
        .addConfig("tsd.query.test6", "24")
        .build();
    
    assertEquals(24, query.getInt(configuration, "tsd.query.test6"));
    assertEquals(42, query.getInt(configuration, "tsd.query.test1"));
    try {
      query.getInt(configuration, "tsd.query.test7");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getBoolean() throws Exception {
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setFilters(Arrays.asList(filter))
        .setMetrics(Arrays.asList(metric))
        .setName("q1")
        .setTime(time)
        .setOutputs(Arrays.asList(output))
        .addConfig("tsd.query.test6", "yes")
        .build();
    
    assertTrue(query.getBoolean(configuration, "tsd.query.test6"));
    assertTrue(query.getBoolean(configuration, "tsd.query.test2"));
    try {
      query.getBoolean(configuration, "tsd.query.test7");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void hasKey() throws Exception {
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setFilters(Arrays.asList(filter))
        .setMetrics(Arrays.asList(metric))
        .setName("q1")
        .setTime(time)
        .setOutputs(Arrays.asList(output))
        .addConfig("tsd.query.test6", "yes")
        .build();
    
    assertTrue(query.hasKey("tsd.query.test6"));
    assertFalse(query.hasKey("tsd.query.test2"));
  }
  
  @Test
  public void convert() throws Exception {
    SemanticQuery.Builder builder = 
        JSON.parseToObject(json, TimeSeriesQuery.class).convert();
    SemanticQuery query = builder.build();
    System.out.println(query.getExecutionGraph());
    assertEquals(4, query.getExecutionGraph().getNodes().size());
    
    ExecutionGraphNode node = query.getExecutionGraph().getNodes().get(0);
    assertEquals("e1", node.getId());
    assertEquals("Expression", node.getType());
    assertEquals(1, node.getSources().size());
    assertTrue(node.getSources().contains("m1_GroupBy"));
    ExpressionConfig ex_config = (ExpressionConfig) node.getConfig();
    assertEquals("m1 * 1024", ex_config.getExpression());
    
    node = query.getExecutionGraph().getNodes().get(1);
    assertEquals("m1", node.getId());
    assertEquals("DataSource", node.getType());
    assertNull(node.getSources());
    QuerySourceConfig ds = (QuerySourceConfig) node.getConfig();
    assertEquals("YAMAS.cpu.idle", ds.getMetric());
    assertEquals("f1", ds.getFilterId());
    assertEquals("3h-ago", ds.getStart());
    assertEquals("1h-ago", ds.getEnd());
    
    node = query.getExecutionGraph().getNodes().get(2);
    assertEquals("m1_Downsampler", node.getId());
    assertEquals("Downsample", node.getType());
    assertEquals(1, node.getSources().size());
    assertTrue(node.getSources().contains("m1"));
    DownsampleConfig dsc = (DownsampleConfig) node.getConfig();
    assertEquals("15m", dsc.intervalAsString());
    assertEquals("avg", dsc.aggregator());
    
    node = query.getExecutionGraph().getNodes().get(3);
    assertEquals("m1_GroupBy", node.getId());
    assertEquals("GroupBy", node.getType());
    assertEquals(1, node.getSources().size());
    assertTrue(node.getSources().contains("m1_Downsampler"));
    GroupByConfig gb = (GroupByConfig) node.getConfig();
    assertEquals("sum", gb.getAggregator());
    assertEquals(1, gb.getTagKeys().size());
    assertTrue(gb.getTagKeys().contains("host"));
  }
  
  private TimeSeriesQuery.Builder getDefaultQueryBuilder() {
    return TimeSeriesQuery.newBuilder().setExpressions(Arrays.asList(expression))
          .setFilters(Arrays.asList(filter)).setMetrics(Arrays.asList(metric))
          .setName("q1").setTime(time).setOutputs(Arrays.asList(output));
  }
}
