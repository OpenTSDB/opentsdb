// This file is part of OpenTSDB.
// Copyright (C) 2010-2016  The OpenTSDB Authors.
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
package net.opentsdb.tsd;

import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.opentsdb.core.FillPolicy;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.query.expression.NumericFillPolicy;
import net.opentsdb.query.expression.BaseTimeSyncedIteratorTest;
import net.opentsdb.query.expression.VariableIterator.SetOperator;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Join;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.Output;
import net.opentsdb.query.pojo.Query;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.storage.MockBase;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, HttpQuery.class, 
  Deferred.class, TSQuery.class, DateTime.class, DeferredGroupException.class })
public class TestQueryExecutor extends BaseTimeSyncedIteratorTest {

  private Timespan time;
  private List<TagVFilter> tags;
  private List<Filter> filters;
  private List<Metric> metrics;
  private List<Expression> expressions;
  private List<Output> outputs;
  private Join intersection;
  
  @Before
  public void setup() {
    intersection = Join.Builder().setOperator(SetOperator.INTERSECTION).build();
    time = Timespan.Builder().setStart("1431561600")
        .setAggregator("sum").build();

    tags = Arrays.asList(new TagVFilter.Builder().setFilter("*").setGroupBy(true)
        .setTagk("D").setType("wildcard").build());
    
    filters = Arrays.asList(Filter.Builder().setId("f1")
        .setTags(tags).build());
    final Metric metric1 = Metric.Builder().setMetric("A").setId("a")
        .setFilter("f1").build();
    final Metric metric2 = Metric.Builder().setMetric("B").setId("b")
        .setFilter("f1").build();
    metrics = Arrays.asList(metric1, metric2);
    expressions = Arrays.asList(Expression.Builder().setId("e")
        .setExpression("a + b").setJoin(intersection).build());
    outputs = Arrays.asList(Output.Builder().setId("e").setAlias("A plus B")
        .build());
  }

  @Test
  public void oneExpressionWithOutputAlias() throws Exception {
    oneExtraSameE();
    final String json = JSON.serializeToString(getDefaultQueryBuilder().build());
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    NettyMocks.mockChannelFuture(query);
    rpc.execute(tsdb, query);
    
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"alias\":\"A plus B\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,12.0,18.0]"));
    assertTrue(response.contains("[1431561660000,14.0,20.0]"));
    assertTrue(response.contains("[1431561720000,16.0,22.0]"));
    assertTrue(response.contains("\"firstTimestamp\":1431561600000"));
    assertTrue(response.contains("\"index\":1"));
    assertTrue(response.contains("\"metrics\":[\"A\",\"B\"]"));
    assertTrue(response.contains("\"index\":2"));
  }
  
  @Test
  public void oneExpressionDefaultOutput() throws Exception {
    oneExtraSameE();
    final Query q = Query.Builder().setExpressions(expressions)
        .setFilters(filters).setMetrics(metrics).setName("q1")
        .setTime(time).build();
    final String json = JSON.serializeToString(q);
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"id\":\"e\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,12.0,18.0]"));
    assertTrue(response.contains("[1431561660000,14.0,20.0]"));
    assertTrue(response.contains("[1431561720000,16.0,22.0]"));
    assertTrue(response.contains("\"firstTimestamp\":1431561600000"));
    assertTrue(response.contains("\"index\":1"));
    assertTrue(response.contains("\"metrics\":[\"A\",\"B\"]"));
    assertTrue(response.contains("\"index\":2"));
    // TODO - more asserts once we settle on names
  }
  
  @Test
  public void oneExpressionOutputAndBAlso() throws Exception {
    oneExtraSameE();
    
    outputs = new ArrayList<Output>(3);
    outputs.add(Output.Builder().setId("e").setAlias("A plus B").build());
    outputs.add(Output.Builder().setId("a").build());
    outputs.add(Output.Builder().setId("b").build());
    
    final String json = JSON.serializeToString(getDefaultQueryBuilder().build());
    
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"alias\":\"A plus B\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,12.0,18.0]"));
    assertTrue(response.contains("[1431561660000,14.0,20.0]"));
    assertTrue(response.contains("[1431561720000,16.0,22.0]"));
    assertTrue(response.contains("\"firstTimestamp\":1431561600000"));
    assertTrue(response.contains("\"index\":1"));
    assertTrue(response.contains("\"metrics\":[\"A\",\"B\"]"));
    assertTrue(response.contains("\"index\":2"));
    
    assertTrue(response.contains("\"id\":\"a\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,1.0,4.0]"));
    assertTrue(response.contains("\"metrics\":[\"A\"]"));
    assertTrue(response.contains("\"id\":\"b\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,11.0,14.0,17.0]"));
    assertTrue(response.contains("\"metrics\":[\"B\"]"));
  }

  @Test
  public void oneExpressionDefaultFill() throws Exception {
    threeSameEGaps();
    String json = JSON.serializeToString(getDefaultQueryBuilder());
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    query.getQueryBaseRoute(); // to the correct serializer
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"alias\":\"A plus B\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,1.0,4.0,0.0]"));
    assertTrue(response.contains("[1431561660000,0.0,20.0,8.0]"));
    assertTrue(response.contains("[1431561720000,16.0,0.0,28.0]"));
    assertTrue(response.contains("\"firstTimestamp\":1431561600000"));
    assertTrue(response.contains("\"index\":1"));
    assertTrue(response.contains("\"metrics\":[\"A\",\"B\"]"));
    assertTrue(response.contains("\"index\":2"));
    assertTrue(response.contains("\"index\":3"));
  }
  
  @Test
  public void oneExpressionDownsamplingMissingTimestampNoFill() throws Exception {
    threeSameEGaps();
    final Downsampler downsampler = Downsampler.Builder()
        .setAggregator("sum")
        .setInterval("1m")
        .build();
    time = Timespan.Builder().setStart("1431561600")
        .setAggregator("sum")
        .setDownsampler(downsampler).build();
    String json = JSON.serializeToString(getDefaultQueryBuilder());
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    query.getQueryBaseRoute(); // to the correct serializer
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"alias\":\"A plus B\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,1.0,4.0,0.0]"));
    assertTrue(response.contains("[1431561660000,0.0,20.0,8.0]"));
    assertTrue(response.contains("[1431561720000,16.0,0.0,28.0]"));
    assertTrue(response.contains("\"firstTimestamp\":1431561600000"));
    assertTrue(response.contains("\"index\":1"));
    assertTrue(response.contains("\"metrics\":[\"A\",\"B\"]"));
    assertTrue(response.contains("\"index\":2"));
    assertTrue(response.contains("\"index\":3"));
  }
  
//  @Test
//  public void oneExpressionDownsamplingMissingTimestampZeroFill() throws Exception {
//    threeSameEGaps();
//    final Downsampler downsampler = Downsampler.Builder()
//        .setAggregator("sum")
//        .setInterval("1m")
//        .setFillPolicy(new NumericFillPolicy(FillPolicy.ZERO))
//        .build();
//    time = Timespan.Builder().setStart("1431561540")
//        .setEnd("1431561780")
//        .setAggregator("sum")
//        .setDownsampler(downsampler).build();
//    String json = JSON.serializeToString(getDefaultQueryBuilder());
//    final QueryRpc rpc = new QueryRpc();
//    final HttpQuery query = NettyMocks.postQuery(tsdb, 
//        "/api/query/exp", json);
//    query.getQueryBaseRoute(); // to the correct serializer
//    NettyMocks.mockChannelFuture(query);
//    
//    rpc.execute(tsdb, query);
//    final String response = 
//        query.response().getContent().toString(Charset.forName("UTF-8"));
//    assertTrue(response.contains("\"alias\":\"A plus B\""));
//    assertTrue(response.contains("\"dps\":[[1431561540000,0.0,0.0,0.0]"));
//    assertTrue(response.contains("[1431561600000,1.0,4.0,0.0]"));
//    assertTrue(response.contains("[1431561660000,0.0,20.0,8.0]"));
//    assertTrue(response.contains("[1431561720000,16.0,0.0,28.0]"));
//    assertTrue(response.contains("[1431561780000,0.0,0.0,0.0]"));
//    assertTrue(response.contains("\"firstTimestamp\":1431561540000"));
//    assertTrue(response.contains("\"index\":1"));
//    assertTrue(response.contains("\"metrics\":[\"A\",\"B\"]"));
//    assertTrue(response.contains("\"index\":2"));
//    assertTrue(response.contains("\"index\":3"));
//  }
  
  @Test
  public void oneExpressionNoFilter() throws Exception {
    oneExtraSameE();
    final Metric metric1 = Metric.Builder().setMetric("A").setId("a")
        .build();
    final Metric metric2 = Metric.Builder().setMetric("B").setId("b")
        .build();
    metrics = Arrays.asList(metric1, metric2);
    
    final String json = JSON.serializeToString(getDefaultQueryBuilder().build());
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
System.out.println(response);
    assertTrue(response.contains("\"alias\":\"A plus B\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,47.0]"));
    assertTrue(response.contains("[1431561660000,52.0]"));
    assertTrue(response.contains("[1431561720000,57.0]"));
    assertTrue(response.contains("\"firstTimestamp\":1431561600000"));
    assertTrue(response.contains("\"index\":1"));
    assertTrue(response.contains("\"metrics\":[\"A\",\"B\"]"));
    assertTrue(response.contains("\"index\":1"));
  }
  
  @Test
  public void twoExpressionsDefaultOutput() throws Exception {
    oneExtraSameE();
    expressions = Arrays.asList(
        Expression.Builder().setId("e").setExpression("a + b")
          .setJoin(intersection).build(),
        Expression.Builder().setId("e2").setExpression("a * b")
          .setJoin(intersection).build());
    
    final Query q = Query.Builder().setExpressions(expressions)
        .setFilters(filters).setMetrics(metrics).setName("q1")
        .setTime(time).build();
    final String json = JSON.serializeToString(q);
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"id\":\"e\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,12.0,18.0]"));
    assertTrue(response.contains("[1431561660000,14.0,20.0]"));
    assertTrue(response.contains("[1431561720000,16.0,22.0]"));
    assertTrue(response.contains("\"firstTimestamp\":1431561600000"));
    assertTrue(response.contains("\"index\":1"));
    assertTrue(response.contains("\"metrics\":[\"A\",\"B\"]"));
    assertTrue(response.contains("\"index\":2"));
    assertTrue(response.contains("\"id\":\"e2\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,11.0,56.0]"));
    assertTrue(response.contains("[1431561660000,24.0,75.0]"));
    assertTrue(response.contains("[1431561720000,39.0,96.0]"));
  }
  
  @Test
  public void twoExpressionsOneWithoutResultsDefaultOutput() throws Exception {
    oneExtraSameE();
    final Metric metric1 = Metric.Builder().setMetric("A").setId("a")
        .setFilter("f1").setAggregator("sum").build();
    final Metric metric2 = Metric.Builder().setMetric("B").setId("b")
        .setFilter("f1").setAggregator("sum").build();
    final Metric metric3 = Metric.Builder().setMetric("D").setId("d")
        .setFilter("f1").setAggregator("sum").build();
    final Metric metric4 = Metric.Builder().setMetric("F").setId("f")
        .setFilter("f1").setAggregator("sum").build();
    metrics = Arrays.asList(metric1, metric2, metric3, metric4);
    
    expressions = Arrays.asList(
        Expression.Builder().setId("e").setExpression("a + b")
          .setJoin(intersection).build(),
        Expression.Builder().setId("x").setExpression("d + f")
          .setJoin(intersection).build());
    
    final Query q = Query.Builder().setExpressions(expressions)
        .setFilters(filters).setMetrics(metrics).setName("q1")
        .setTime(time).build();
    String json = JSON.serializeToString(q);
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    query.getQueryBaseRoute(); // to the correct serializer
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"id\":\"e\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,12.0,18.0]"));
    assertTrue(response.contains("[1431561660000,14.0,20.0]"));
    assertTrue(response.contains("[1431561720000,16.0,22.0]"));
    assertTrue(response.contains("\"firstTimestamp\":1431561600000"));
    assertTrue(response.contains("\"index\":1"));
    assertTrue(response.contains("\"metrics\":[\"A\",\"B\"]"));
    assertTrue(response.contains("\"index\":2"));
    assertTrue(response.contains("\"id\":\"x\""));
    assertTrue(response.contains("\"dps\":[]"));
    assertTrue(response.contains("\"firstTimestamp\":0"));
    assertTrue(response.contains("\"series\":0"));
  }
  
  @Test
  public void multiExpressionsOneOutput() throws Exception {
    oneExtraSameE();
    expressions = Arrays.asList(
        Expression.Builder().setId("e").setExpression("a + b").setJoin(intersection).build(),
        Expression.Builder().setId("e2").setExpression("e * 2").setJoin(intersection).build(),
        Expression.Builder().setId("e3").setExpression("e * 2").setJoin(intersection).build(),
        Expression.Builder().setId("e4").setExpression("e2 + e3").setJoin(intersection).build());

    final String json = JSON.serializeToString(getDefaultQueryBuilder());
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    query.getQueryBaseRoute(); // to the correct serializer
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("[1431561660000,14.0,20.0]"));
    assertTrue(response.contains("[1431561720000,16.0,22.0]"));
    assertTrue(response.contains("\"firstTimestamp\":1431561600000"));
    assertTrue(response.contains("\"index\":1"));
    assertTrue(response.contains("\"metrics\":[\"A\",\"B\"]"));
    assertTrue(response.contains("\"index\":2"));
  }
  
  @Test
  public void nestedExpressionsOneLevelDefaultOutput() throws Exception {
    oneExtraSameE();
    expressions = Arrays.asList(
        Expression.Builder().setId("e").setExpression("a + b")
          .setJoin(intersection).build(),
        Expression.Builder().setId("e2").setExpression("e * 2")
          .setJoin(intersection).build());
    
    final Query q = Query.Builder().setExpressions(expressions)
        .setFilters(filters).setMetrics(metrics).setName("q1")
        .setTime(time).build();
    final String json = JSON.serializeToString(q);
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"id\":\"e\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,12.0,18.0]"));
    assertTrue(response.contains("[1431561660000,14.0,20.0]"));
    assertTrue(response.contains("[1431561720000,16.0,22.0]"));
    assertTrue(response.contains("\"firstTimestamp\":1431561600000"));
    assertTrue(response.contains("\"index\":1"));
    assertTrue(response.contains("\"metrics\":[\"A\",\"B\"]"));
    assertTrue(response.contains("\"index\":2"));
    assertTrue(response.contains("\"id\":\"e2\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,24.0,36.0]"));
    assertTrue(response.contains("[1431561660000,28.0,40.0]"));
    assertTrue(response.contains("[1431561720000,32.0,44.0]"));
  }
  
  @Test
  public void nestedExpressionsTwoLevelsDefaultOutput() throws Exception {
    oneExtraSameE();
    expressions = Arrays.asList(
        Expression.Builder().setId("e").setExpression("a + b").setJoin(intersection).build(),
        Expression.Builder().setId("e2").setExpression("e * 2").setJoin(intersection).build(),
        Expression.Builder().setId("e3").setExpression("e * 2").setJoin(intersection).build(),
        Expression.Builder().setId("e4").setExpression("e2 + e3").setJoin(intersection).build());
    
    final Query q = Query.Builder().setExpressions(expressions)
        .setFilters(filters).setMetrics(metrics).setName("q1")
        .setTime(time).build();
    final String json = JSON.serializeToString(q);
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"id\":\"e\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,12.0,18.0]"));
    assertTrue(response.contains("[1431561660000,14.0,20.0]"));
    assertTrue(response.contains("[1431561720000,16.0,22.0]"));
    assertTrue(response.contains("\"firstTimestamp\":1431561600000"));
    assertTrue(response.contains("\"index\":1"));
    assertTrue(response.contains("\"metrics\":[\"A\",\"B\"]"));
    assertTrue(response.contains("\"index\":2"));
    assertTrue(response.contains("\"id\":\"e2\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,24.0,36.0]"));
    assertTrue(response.contains("[1431561660000,28.0,40.0]"));
    assertTrue(response.contains("[1431561720000,32.0,44.0]"));
    assertTrue(response.contains("\"id\":\"e3\""));
    assertTrue(response.contains("\"id\":\"e4\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,48.0,72.0]"));
    assertTrue(response.contains("[1431561660000,56.0,80.0]"));
    assertTrue(response.contains("[1431561720000,64.0,88.0]"));
  }
  
  @Test
  public void nestedExpressionsTwoLevelsDefaultOutputOrdering() throws Exception {
    oneExtraSameE();
    expressions = Arrays.asList(
        Expression.Builder().setId("e2").setExpression("e * 2").setJoin(intersection).build(),
        Expression.Builder().setId("e4").setExpression("e2 + e3").setJoin(intersection).build(),
        Expression.Builder().setId("e3").setExpression("e * 2").setJoin(intersection).build(),
        Expression.Builder().setId("e").setExpression("a + b").setJoin(intersection).build()
        );
    
    final Query q = Query.Builder().setExpressions(expressions)
        .setFilters(filters).setMetrics(metrics).setName("q1")
        .setTime(time).build();
    final String json = JSON.serializeToString(q);
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"id\":\"e\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,12.0,18.0]"));
    assertTrue(response.contains("[1431561660000,14.0,20.0]"));
    assertTrue(response.contains("[1431561720000,16.0,22.0]"));
    assertTrue(response.contains("\"firstTimestamp\":1431561600000"));
    assertTrue(response.contains("\"index\":1"));
    assertTrue(response.contains("\"metrics\":[\"A\",\"B\"]"));
    assertTrue(response.contains("\"index\":2"));
    assertTrue(response.contains("\"id\":\"e2\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,24.0,36.0]"));
    assertTrue(response.contains("[1431561660000,28.0,40.0]"));
    assertTrue(response.contains("[1431561720000,32.0,44.0]"));
    assertTrue(response.contains("\"id\":\"e3\""));
    assertTrue(response.contains("\"id\":\"e4\""));
    assertTrue(response.contains("\"dps\":[[1431561600000,48.0,72.0]"));
    assertTrue(response.contains("[1431561660000,56.0,80.0]"));
    assertTrue(response.contains("[1431561720000,64.0,88.0]"));
  }
  
  @Test
  public void emptyResultSet() throws Exception {
    setDataPointStorage();
    String json = JSON.serializeToString(getDefaultQueryBuilder());
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    query.getQueryBaseRoute(); // to the correct serializer
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"dps\":[]"));
    assertTrue(response.contains("\"firstTimestamp\":0"));
    assertTrue(response.contains("\"series\":0"));
  }
  
  @Test
  public void scannerException() throws Exception {
    oneExtraSameE();
    storage.throwException(MockBase.stringToBytes(
        "00000B5553E58000000D00000F00000E00000E"), 
        new RuntimeException("Boo!"), true);
    final String json = JSON.serializeToString(getDefaultQueryBuilder().build());
    
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    query.getQueryBaseRoute(); // to the correct serializer
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"code\":400"));
    assertTrue(response.contains("\"message\":\"Boo!\""));
  }
  
  @Test
  public void nsunMetric() throws Exception {
    oneExtraSameE();
    final Metric metric1 = Metric.Builder().setMetric("A").setId("a")
        .setFilter("f1").setAggregator("sum").build();
    final Metric metric2 = Metric.Builder().setMetric(NSUN_METRIC).setId("b")
        .setFilter("f1").setAggregator("sum").build();
    metrics = Arrays.asList(metric1, metric2);
    final String json = JSON.serializeToString(getDefaultQueryBuilder().build());
    
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    query.getQueryBaseRoute(); // to the correct serializer
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"code\":400"));
    assertTrue(response.contains("\"message\":\"No such name for '" + 
        NSUN_METRIC + "'"));
  }

  @Test
  public void selfReferencingExpression() throws Exception {
    oneExtraSameE();
    expressions = Arrays.asList(
        Expression.Builder().setId("e").setExpression("a + b").build(),
        Expression.Builder().setId("e2").setExpression("e * 2").build(),
        Expression.Builder().setId("e3").setExpression("e * 2").build(),
        Expression.Builder().setId("e4").setExpression("e2 + e4").build());
    
    final Query q = Query.Builder().setExpressions(expressions)
        .setFilters(filters).setMetrics(metrics).setName("q1")
        .setTime(time).build();
    final String json = JSON.serializeToString(q);
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    query.getQueryBaseRoute(); // to the correct serializer
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"code\":400"));
    assertTrue(response.contains("\"message\":\"Self referencing"));
  }
  
  @Test
  public void circularReferenceExpression() throws Exception {
    oneExtraSameE();
    expressions = Arrays.asList(
        Expression.Builder().setId("e").setExpression("a + e4").build(),
        Expression.Builder().setId("e2").setExpression("e * 2").build(),
        Expression.Builder().setId("e3").setExpression("e * 2").build(),
        Expression.Builder().setId("e4").setExpression("e2 + e3").build());
    
    final Query q = Query.Builder().setExpressions(expressions)
        .setFilters(filters).setMetrics(metrics).setName("q1")
        .setTime(time).build();
    final String json = JSON.serializeToString(q);
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    query.getQueryBaseRoute(); // to the correct serializer
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"code\":400"));
    assertTrue(response.contains("\"message\":\"Circular reference found:"));
  }
  
  @Test
  public void noIntersectionsFound() throws Exception {
    threeDifE();
    
    String json = JSON.serializeToString(getDefaultQueryBuilder());
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    query.getQueryBaseRoute(); // to the correct serializer
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"code\":400"));
    assertTrue(response.contains("\"message\":\"No intersections found"));
  }
  
  @Test
  public void noIntersectionsFoundNestedExpression() throws Exception {
    oneExtraSameE();
    
    final Metric metric1 = Metric.Builder().setMetric("A").setId("a")
        .setFilter("f1").setAggregator("sum").build();
    final Metric metric2 = Metric.Builder().setMetric("B").setId("b")
        .setFilter("f1").setAggregator("sum").build();
    final Metric metric3 = Metric.Builder().setMetric("D").setId("d")
        .setFilter("f1").setAggregator("sum").build();
    metrics = Arrays.asList(metric1, metric2, metric3);
    
    expressions = Arrays.asList(
        Expression.Builder().setId("e").setExpression("a + b").setJoin(intersection).build(),
        Expression.Builder().setId("x").setExpression("d + e").setJoin(intersection).build());
    
    final Query q = Query.Builder().setExpressions(expressions)
        .setFilters(filters).setMetrics(metrics).setName("q1")
        .setTime(time).build();
    String json = JSON.serializeToString(q);
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    query.getQueryBaseRoute(); // to the correct serializer
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"code\":400"));
    assertTrue(response.contains("\"message\":\"No intersections found"));
  }
  
  @Test
  public void noIntersectionsFoundOneMetricEmpty() throws Exception {
    oneExtraSameE();
    final Metric metric1 = Metric.Builder().setMetric("A").setId("a")
        .setFilter("f1").setAggregator("sum").build();
    final Metric metric2 = Metric.Builder().setMetric("D").setId("b")
        .setFilter("f1").setAggregator("sum").build();
    metrics = Arrays.asList(metric1, metric2);
    String json = JSON.serializeToString(getDefaultQueryBuilder());
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    query.getQueryBaseRoute(); // to the correct serializer
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"code\":400"));
    assertTrue(response.contains("\"message\":\"No intersections found"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void notEnoughMetrics() throws Exception {
    oneExtraSameE();
    expressions = Arrays.asList(
        Expression.Builder().setId("e").setExpression("a + b + c").build());
    String json = JSON.serializeToString(getDefaultQueryBuilder());
    final QueryRpc rpc = new QueryRpc();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/query/exp", json);
    query.getQueryBaseRoute(); // to the correct serializer
    NettyMocks.mockChannelFuture(query);
    
    rpc.execute(tsdb, query);
  }

  protected Query.Builder getDefaultQueryBuilder() {
    return Query.Builder().setExpressions(expressions).setFilters(filters)
        .setMetrics(metrics).setName("q1").setTime(time).setOutputs(outputs);
  }
}
