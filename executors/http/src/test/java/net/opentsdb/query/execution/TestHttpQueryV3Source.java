// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.temporal.TemporalAmount;
import java.util.Map;

import net.opentsdb.query.filter.QueryFilter;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.auth.AuthState;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.filter.DefaultNamedFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.downsample.DownsampleFactory;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.stats.BlackholeStatsCollector;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Pair;
import net.opentsdb.utils.UnitTestException;

public class TestHttpQueryV3Source {

  private BaseHttpExecutorFactory factory;
  private QueryContext context;
  private QueryPipelineContext ctx;
  private CloseableHttpAsyncClient client;
  private String host;
  private String endpoint;
  private HttpUriRequest request;
  private FutureCallback<HttpResponse> callback;
  private QueryNode upstream;
  
  @Before
  public void before() throws Exception {
    factory = mock(HttpQueryV3Factory.class);
    context = mock(QueryContext.class);
    ctx = mock(QueryPipelineContext.class);
    client = mock(CloseableHttpAsyncClient.class);
    host = "http://localhost:4242";
    endpoint = "/api/query/graph";
    upstream = mock(QueryNode.class);
    
    TSDB tsdb = mock(TSDB.class);
    when(tsdb.getConfig()).thenReturn(mock(Configuration.class));
    when(ctx.tsdb()).thenReturn(tsdb);
    BlackholeStatsCollector stats = new BlackholeStatsCollector();
    when(tsdb.getStatsCollector()).thenReturn(stats);
    
    when(ctx.queryContext()).thenReturn(context);
    when(ctx.upstream(any(QueryNode.class)))
    .thenReturn(Lists.newArrayList(upstream));
    
    when(client.execute(any(HttpUriRequest.class), any(FutureCallback.class)))
      .thenAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          request = (HttpUriRequest) invocation.getArguments()[0];
          callback = (FutureCallback<HttpResponse>) invocation.getArguments()[1];
          return null;
        }
      });
  }
  
  @Test
  public void requestMetricOnly() throws Exception {
    TimeSeriesDataSourceConfig config = setQuery();
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.fetchNext(null);
    
    verify(client, times(1)).execute(any(HttpUriRequest.class), any(FutureCallback.class));
    assertEquals("application/json", request.getFirstHeader("Content-Type").getValue());
    assertNull(request.getFirstHeader("Cookie"));
    String json = EntityUtils.toString(((HttpPost) request).getEntity());
    assertTrue(json.matches(".*\"start\":\"\\d{13}\".*"));
    assertTrue(json.matches(".*\"end\":\"\\d{13}\".*"));
    assertTrue(json.contains("\"mode\":\"SINGLE\""));
    assertTrue(json.contains("\"id\":\"m1\""));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
  }
  
  @Test
  public void requestPushDowns() throws Exception {
    NumericInterpolatorConfig numeric_config = (NumericInterpolatorConfig) 
        NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NONE)
      .setRealFillPolicy(FillWithRealPolicy.NONE)
      .setDataType(NumericArrayType.TYPE.toString())
      .build();
    
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .addPushDownNode(DownsampleConfig.newBuilder()
            .setAggregator("max")
            .setInterval("1m")
            .addInterpolatorConfig(numeric_config)
            .setId("ds")
            .addSource("m1")
            .build())
        .addPushDownNode(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(numeric_config)
            .setId("gb")
            .addSource("ds")
            .build())
        .setId("m1")
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1h-ago")
        .addExecutionGraphNode(config)
        .build();
    
    when(ctx.query()).thenReturn(query);
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.fetchNext(null);
    
    verify(client, times(1)).execute(any(HttpUriRequest.class), any(FutureCallback.class));
    assertEquals("application/json", request.getFirstHeader("Content-Type").getValue());
    assertNull(request.getFirstHeader("Cookie"));
    String json = EntityUtils.toString(((HttpPost) request).getEntity());
    assertTrue(json.matches(".*\"start\":\"\\d{13}\".*"));
    assertTrue(json.matches(".*\"end\":\"\\d{13}\".*"));
    assertTrue(json.contains("\"mode\":\"SINGLE\""));
    assertTrue(json.contains("\"id\":\"m1\""));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
    assertFalse(json.contains("pushDownNodes"));
    assertTrue(json.contains("\"id\":\"gb\""));
    assertTrue(json.contains("\"sources\":[\"ds\"]"));
    assertTrue(json.contains("\"id\":\"ds\""));
    assertTrue(json.contains("\"sources\":[\"m1\"]"));
  }
  
  @Test
  public void requestPushDownsAutoInterval() throws Exception {
    NumericInterpolatorConfig numeric_config = (NumericInterpolatorConfig) 
        NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .setDataType(NumericArrayType.TYPE.toString())
        .build();
    
    DownsampleFactory f = new DownsampleFactory();
    MockTSDB tsdb = new MockTSDB();
    f.initialize(tsdb, null).join(250);
    
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .addPushDownNode(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("auto")
            .setIntervals(f.intervals())
            .addInterpolatorConfig(numeric_config)
            .setId("ds")
            .addSource("m1")
            .build())
        .addPushDownNode(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(numeric_config)
            .setId("gb")
            .addSource("ds")
            .build())
        .setId("m1")
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("24h-ago")
        .addExecutionGraphNode(config)
        .build();
    
    when(ctx.query()).thenReturn(query);
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.fetchNext(null);
    
    verify(client, times(1)).execute(any(HttpUriRequest.class), any(FutureCallback.class));
    assertEquals("application/json", request.getFirstHeader("Content-Type").getValue());
    assertNull(request.getFirstHeader("Cookie"));
    String json = EntityUtils.toString(((HttpPost) request).getEntity());
    assertTrue(json.matches(".*\"start\":\"\\d{13}\".*"));
    assertTrue(json.matches(".*\"end\":\"\\d{13}\".*"));
    assertTrue(json.contains("\"mode\":\"SINGLE\""));
    assertTrue(json.contains("\"id\":\"m1\""));
    assertTrue(json.contains("\"interval\":\"15m\""));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
    assertFalse(json.contains("pushDownNodes"));
    assertTrue(json.contains("\"id\":\"gb\""));
    assertTrue(json.contains("\"sources\":[\"ds\"]"));
    assertTrue(json.contains("\"id\":\"ds\""));
    assertTrue(json.contains("\"sources\":[\"m1\"]"));
  }

  @Test
  public void requestFilter() throws Exception {
    NumericInterpolatorConfig numeric_config = (NumericInterpolatorConfig) 
        NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NONE)
      .setRealFillPolicy(FillWithRealPolicy.NONE)
      .setDataType(NumericArrayType.TYPE.toString())
      .build();
    
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .addPushDownNode(DownsampleConfig.newBuilder()
            .setAggregator("max")
            .setInterval("1m")
            .addInterpolatorConfig(numeric_config)
            .setId("ds")
            .addSource("m1")
            .build())
        .addPushDownNode(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(numeric_config)
            .setId("gb")
            .addSource("ds")
            .build())
        .setFilterId("f1")
        .setId("m1")
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1h-ago")
        .addExecutionGraphNode(config)
        .addFilter(DefaultNamedFilter.newBuilder()
            .setId("f1")
            .setFilter(TagValueLiteralOrFilter.newBuilder()
                .setFilter("web01")
                .setTagKey("host")
                .build())
            .build())
        .build();
    
    when(ctx.query()).thenReturn(query);
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.fetchNext(null);
    
    verify(client, times(1)).execute(any(HttpUriRequest.class), any(FutureCallback.class));
    assertEquals("application/json", request.getFirstHeader("Content-Type").getValue());
    assertNull(request.getFirstHeader("Cookie"));
    String json = EntityUtils.toString(((HttpPost) request).getEntity());
    assertTrue(json.matches(".*\"start\":\"\\d{13}\".*"));
    assertTrue(json.matches(".*\"end\":\"\\d{13}\".*"));
    assertTrue(json.contains("\"mode\":\"SINGLE\""));
    assertTrue(json.contains("\"id\":\"m1\""));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
    assertFalse(json.contains("pushDownNodes"));
    assertTrue(json.contains("\"id\":\"gb\""));
    assertTrue(json.contains("\"sources\":[\"ds\"]"));
    assertTrue(json.contains("\"id\":\"ds\""));
    assertTrue(json.contains("\"sources\":[\"m1\"]"));
    assertTrue(json.contains("\"filters\":["));
    assertTrue(json.contains("\"id\":\"f1\""));
    assertTrue(json.contains("\"type\":\"TagValueLiteralOr\""));
    assertTrue(json.contains("\"filter\":\"web01\""));
  }

  @Test
  public void requestFilterNoOrNullFilterId() throws Exception {

    QueryFilter filter = TagValueLiteralOrFilter.newBuilder()
        .setFilter("web01")
        .setTagKey("host")
        .build();

    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig)
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("system.cpu.user")
                .build())
            .setQueryFilter(filter)
            .setFilterId(null)
            .setId("m1")
            .build();

    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1h-ago")
        .addExecutionGraphNode(config)
        .build();

    when(ctx.query()).thenReturn(query);
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.fetchNext(null);

    verify(client, times(1)).execute(any(HttpUriRequest.class), any(FutureCallback.class));
    assertEquals("application/json", request.getFirstHeader("Content-Type").getValue());
    assertNull(request.getFirstHeader("Cookie"));
    String json = EntityUtils.toString(((HttpPost) request).getEntity());
    assertTrue(json.matches(".*\"start\":\"\\d{13}\".*"));
    assertTrue(json.matches(".*\"end\":\"\\d{13}\".*"));
    assertTrue(json.contains("\"mode\":\"SINGLE\""));
    assertTrue(json.contains("\"id\":\"m1\""));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
    assertTrue(json.contains("\"filters\":["));
    assertTrue(json.contains("\"type\":\"TagValueLiteralOr\""));
    assertTrue(json.contains("\"filter\":\"web01\""));
  }
  
  @Test
  public void requestOffset() throws Exception {
    Map<String, Pair<Boolean, TemporalAmount>> offsets = Maps.newHashMap();
    offsets.put("m1-previous-P1H", new Pair<>(true, DateTime.parseDuration2("1h")));
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .setTimeShifts(offsets)
        .setFilterId("f1")
        .setId("m1-previous-P1H")
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1546304400")
        .setEnd("1546308000")
        .addExecutionGraphNode(config)
        .addFilter(DefaultNamedFilter.newBuilder()
            .setId("f1")
            .setFilter(TagValueLiteralOrFilter.newBuilder()
                .setFilter("web01")
                .setTagKey("host")
                .build())
            .build())
        .build();
    
    when(ctx.query()).thenReturn(query);
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.fetchNext(null);
    
    verify(client, times(1)).execute(any(HttpUriRequest.class), any(FutureCallback.class));
    assertEquals("application/json", request.getFirstHeader("Content-Type").getValue());
    assertNull(request.getFirstHeader("Cookie"));
    String json = EntityUtils.toString(((HttpPost) request).getEntity());
    assertTrue(json.contains("\"start\":\"1546300800000\""));
    assertTrue(json.contains("\"end\":\"1546304400000\""));
    assertTrue(json.contains("\"mode\":\"SINGLE\""));
    assertTrue(json.contains("\"id\":\"m1-previous-P1H\""));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
  }
  
  @Test
  public void requestAuthCookie() throws Exception {
    AuthState auth = mock(AuthState.class);
    when(auth.getTokenType()).thenReturn("Cookie");
    when(auth.getToken()).thenReturn("MyCookie".getBytes(Const.UTF8_CHARSET));
    when(context.authState()).thenReturn(auth);
    
    TimeSeriesDataSourceConfig config = setQuery();
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.fetchNext(null);
    
    verify(client, times(1)).execute(any(HttpUriRequest.class), any(FutureCallback.class));
    assertEquals("application/json", request.getFirstHeader("Content-Type").getValue());
    assertEquals("MyCookie", request.getFirstHeader("Cookie").getValue());
    String json = EntityUtils.toString(((HttpPost) request).getEntity());
    assertTrue(json.matches(".*\"start\":\"\\d{13}\".*"));
    assertTrue(json.matches(".*\"end\":\"\\d{13}\".*"));
    assertTrue(json.contains("\"mode\":\"SINGLE\""));
    assertTrue(json.contains("\"id\":\"m1\""));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
  }
  
  @Test
  public void requestAuthUser() throws Exception {
    AuthState auth = mock(AuthState.class);
    TSDB tsdb = mock(TSDB.class);
    Configuration cfg = mock(Configuration.class);
    when(tsdb.getConfig()).thenReturn(cfg);
    when(ctx.tsdb()).thenReturn(tsdb);
    BlackholeStatsCollector stats = new BlackholeStatsCollector();
    when(tsdb.getStatsCollector()).thenReturn(stats);
    when(cfg.getString(anyString())).thenReturn("X-OpenTSDB-User");
    when(auth.getTokenType()).thenReturn("Cookie");
    when(auth.getToken()).thenReturn("MyCookie".getBytes(Const.UTF8_CHARSET));
    when(auth.getUser()).thenReturn("UnitTest");
    when(context.authState()).thenReturn(auth);
    
    TimeSeriesDataSourceConfig config = setQuery();
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.fetchNext(null);
    
    verify(client, times(1)).execute(any(HttpUriRequest.class), any(FutureCallback.class));
    assertEquals("application/json", request.getFirstHeader("Content-Type").getValue());
    assertEquals("MyCookie", request.getFirstHeader("Cookie").getValue());
    assertEquals("UnitTest", request.getFirstHeader("X-OpenTSDB-User").getValue());
    String json = EntityUtils.toString(((HttpPost) request).getEntity());
    assertTrue(json.matches(".*\"start\":\"\\d{13}\".*"));
    assertTrue(json.matches(".*\"end\":\"\\d{13}\".*"));
    assertTrue(json.contains("\"mode\":\"SINGLE\""));
    assertTrue(json.contains("\"id\":\"m1\""));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
  }

  @Test
  public void responseCancelled() throws Exception {
    TimeSeriesDataSourceConfig config = setQuery();
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.initialize(null).join(250);
    src.fetchNext(null);
    
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    callback.cancelled();
    
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, times(1)).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
  }
  
  @Test
  public void responseException() throws Exception {
    TimeSeriesDataSourceConfig config = setQuery();
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.initialize(null).join(250);
    src.fetchNext(null);
    
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    callback.failed(new UnitTestException());
    
    verify(upstream, times(1)).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(UnitTestException.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
  }
  
  @Test
  public void responseOKOneResult() throws Exception {
    String json = "{\"results\":[{\"source\":\"m0:m0\",\"data\":["
        + "{\"metric\":\"system.cpu.user\",\"tags\":{\"host\":\"web01\"},"
        + "\"aggregateTags\":[],\"NumericType\":{\"1540567593\":"
        + "23.399999618530273,\"1540567653\":23,\"1540567713\":"
        + "23.399999618530273,\"1540567773\":23.399999618530273}},"
        + "{\"metric\":\"system.cpu.user\",\"tags\":{\"host\":\"web02\"},"
        + "\"aggregateTags\":[],\"NumericType\":{\"1540567584\":"
        + "52.29999923706055,\"1540567644\":52.29999923706055,"
        + "\"1540567704\":75,\"1540567764\":75.19999694824219}}]}]}";
    
    TimeSeriesDataSourceConfig config = setQuery();
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.initialize(null).join(250);
    src.fetchNext(null);
    
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    HttpResponse response = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);
    HttpEntity entity = new StringEntity(json);
    when(response.getStatusLine()).thenReturn(status);
    when(response.getEntity()).thenReturn(entity);
    when(status.getStatusCode()).thenReturn(200);
    
    callback.completed(response);
    
    verify(upstream, times(1)).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
  }
  
  @Test
  public void responseOKTwoResult() throws Exception {
    String json = "{\"results\":[{\"source\":\"m0:m0\",\"data\":["
        + "{\"metric\":\"system.cpu.user\",\"tags\":{\"host\":\"web01\"},"
        + "\"aggregateTags\":[],\"NumericType\":{\"1540567593\":"
        + "23.399999618530273,\"1540567653\":23,\"1540567713\":"
        + "23.399999618530273,\"1540567773\":23.399999618530273}},"
        + "{\"metric\":\"system.cpu.user\",\"tags\":{\"host\":\"web02\"},"
        + "\"aggregateTags\":[],\"NumericType\":{\"1540567584\":"
        + "52.29999923706055,\"1540567644\":52.29999923706055,"
        + "\"1540567704\":75,\"1540567764\":75.19999694824219}}]},"
        + "{\"source\":\"m0:m0\",\"data\":["
        + "{\"metric\":\"system.cpu.user\",\"tags\":{\"host\":\"web01\"},"
        + "\"aggregateTags\":[],\"NumericType\":{\"1540567593\":"
        + "23.399999618530273,\"1540567653\":23,\"1540567713\":"
        + "23.399999618530273,\"1540567773\":23.399999618530273}},"
        + "{\"metric\":\"system.cpu.user\",\"tags\":{\"host\":\"web02\"},"
        + "\"aggregateTags\":[],\"NumericType\":{\"1540567584\":"
        + "52.29999923706055,\"1540567644\":52.29999923706055,"
        + "\"1540567704\":75,\"1540567764\":75.19999694824219}}]}"
        + "]}";
    
    TimeSeriesDataSourceConfig config = setQuery();
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.initialize(null).join(250);
    src.fetchNext(null);
    
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    HttpResponse response = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);
    HttpEntity entity = new StringEntity(json);
    when(response.getStatusLine()).thenReturn(status);
    when(response.getEntity()).thenReturn(entity);
    when(status.getStatusCode()).thenReturn(200);
    
    callback.completed(response);
    
    verify(upstream, times(2)).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
  }
  
  @Test
  public void responseOKBadJson() throws Exception {
    String json = "{\"results\":[{\"source\":\"m0:m0\",\"data\":["
        + "{\"metric\":\"system.cpu.user\",\"tags\":{\"host\":\"web01\"},"
        + "\"aggregateTags\":[],\"NumericType\":{\"1540567593\":"
        + "23.399999618530273,\"1540567653\":23,\"1540567713\":"
        + "23.399999618530273,\"1540567773\":23.399999618530273}},"
        + "{\"metric\":\"system.cpu.user\",\"tags\":{\"host\":\"web02\"},"
        + "\"aggregateTags\":[],\"NumericType\":{\"1540567584\":"
        + "52.29999923706055,\"1540567644\":52.299";
    
    TimeSeriesDataSourceConfig config = setQuery();
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.initialize(null).join(250);
    src.fetchNext(null);
    
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    HttpResponse response = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);
    HttpEntity entity = new StringEntity(json);
    when(response.getStatusLine()).thenReturn(status);
    when(response.getEntity()).thenReturn(entity);
    when(status.getStatusCode()).thenReturn(200);
    
    callback.completed(response);
    
    verify(upstream, times(1)).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
  }
  
  @Test
  public void response400Json() throws Exception {
    String json = "{\"error\":{\"code\":400,\"message\":\"Nofilter\","
        + "\"trace\":\"java.lang.IllegalArgumentException:Nofilter\"}}";
    
    TimeSeriesDataSourceConfig config = setQuery();
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.initialize(null).join(250);
    src.fetchNext(null);
    
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(client, times(1)).execute(any(HttpUriRequest.class), 
        any(FutureCallback.class));
    
    HttpResponse response = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);
    HttpEntity entity = new StringEntity(json);
    when(response.getStatusLine()).thenReturn(status);
    when(response.getEntity()).thenReturn(entity);
    when(status.getStatusCode()).thenReturn(400);
    
    callback.completed(response);
    
    verify(upstream, times(1)).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(client, times(1)).execute(any(HttpUriRequest.class), 
        any(FutureCallback.class));
  }
  
  @Test
  public void response405JsonTwice() throws Exception {
    String json = "{\"error\":{\"code\":405,\"message\":\"Not Allowed\","
        + "\"trace\":\"java.lang.IllegalArgumentException:Not Allowed\"}}";
    when(factory.retries()).thenReturn(3);
    
    TimeSeriesDataSourceConfig config = setQuery();
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.initialize(null).join(250);
    src.fetchNext(null);
    
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(client, times(1)).execute(any(HttpUriRequest.class), 
        any(FutureCallback.class));
    
    HttpResponse response = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);
    HttpEntity entity = new StringEntity(json);
    when(response.getStatusLine()).thenReturn(status);
    when(response.getEntity()).thenReturn(entity);
    when(status.getStatusCode()).thenReturn(405);
    
    callback.completed(response);
    
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(client, times(2)).execute(any(HttpUriRequest.class), 
        any(FutureCallback.class));
    
    // same issue
    callback.completed(response);
    
    verify(upstream, times(1)).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(client, times(2)).execute(any(HttpUriRequest.class), 
        any(FutureCallback.class));
  }
  
  @Test
  public void response405JsonThrice() throws Exception {
    String json = "{\"error\":{\"code\":405,\"message\":\"Not Allowed\","
        + "\"trace\":\"java.lang.IllegalArgumentException:Not Allowed\"}}";
    when(factory.retries()).thenReturn(2);
    
    TimeSeriesDataSourceConfig config = setQuery();
    HttpQueryV3Source src = new HttpQueryV3Source(factory, ctx, config, client, host, endpoint);
    src.initialize(null).join(250);
    src.fetchNext(null);
    
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(client, times(1)).execute(any(HttpUriRequest.class), 
        any(FutureCallback.class));
    
    HttpResponse response = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);
    HttpEntity entity = new StringEntity(json);
    when(response.getStatusLine()).thenReturn(status);
    when(response.getEntity()).thenReturn(entity);
    when(status.getStatusCode()).thenReturn(405);
    
    callback.completed(response);
    
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(client, times(2)).execute(any(HttpUriRequest.class), 
        any(FutureCallback.class));
    
    json = "{\"error\":{\"code\":405,\"message\":\"Not Allowed For bob\","
        + "\"trace\":\"java.lang.IllegalArgumentException:Not Allowed For bob\"}}";
    entity = new StringEntity(json);
    when(response.getEntity()).thenReturn(entity);
    
    // diff 
    callback.completed(response);
    
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(client, times(3)).execute(any(HttpUriRequest.class), 
        any(FutureCallback.class));
    
    json = "{\"error\":{\"code\":405,\"message\":\"Not Allowed For sue\","
        + "\"trace\":\"java.lang.IllegalArgumentException:Not Allowed For sue\"}}";
    entity = new StringEntity(json);
    when(response.getEntity()).thenReturn(entity);
    
    // diff 
    callback.completed(response);
    
    verify(upstream, times(1)).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(client, times(3)).execute(any(HttpUriRequest.class), 
        any(FutureCallback.class));
  }
  
  TimeSeriesDataSourceConfig setQuery() throws Exception {
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .setId("m1")
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1h-ago")
        .addExecutionGraphNode(config)
        .build();
    
    when(ctx.query()).thenReturn(query);
    return config;
  }
}
