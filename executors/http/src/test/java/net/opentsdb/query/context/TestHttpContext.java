// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
package net.opentsdb.query.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.DataMerger;
import net.opentsdb.data.DataShardMerger;
import net.opentsdb.data.DataShardsGroup;
import net.opentsdb.query.context.HttpContext;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.ClusterConfig;
import net.opentsdb.query.execution.HttpEndpoints;
import net.opentsdb.query.execution.HttpQueryV2Executor;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HttpAsyncClients.class, HttpContext.class })
public class TestHttpContext {

  private CloseableHttpAsyncClient client;
  private QueryContext context;
  private HttpEndpoints endpoints;
  private Map<String, String> headers;
  private Map<TypeToken<?>, DataMerger<?>> mergers;
  private List<String> urls;
  private HttpQueryV2Executor executor;
  
  @Before
  public void before() throws Exception {
    final HttpAsyncClientBuilder builder = mock(HttpAsyncClientBuilder.class);
    client = mock(CloseableHttpAsyncClient.class);
    context = mock(QueryContext.class);
    endpoints = mock(HttpEndpoints.class);
    headers = Maps.newHashMap();
    mergers = Maps.newHashMap();
    urls = Lists.newArrayList("http://myhost:4242", "http://otherhost:4242");
    executor = mock(HttpQueryV2Executor.class);
    
    PowerMockito.mockStatic(HttpAsyncClients.class);
    when(HttpAsyncClients.custom()).thenReturn(builder);
    when(builder.build()).thenReturn(client);
    when(endpoints.getEndpoints(null)).thenReturn(urls);
    when(endpoints.clusterTimeout()).thenReturn(60000L);
    PowerMockito.whenNew(HttpQueryV2Executor.class)
      .withAnyArguments().thenReturn(executor);
    headers.put("X-Hello", "Hello");
  }
  
  @Test
  public void ctor() throws Exception {
    HttpContext ctx = new HttpContext(context, endpoints, mergers, headers);
    assertSame(client, ctx.getClient());
    verify(client, times(1)).start();
    assertSame(headers, ctx.getHeaders());
    assertEquals(1, ctx.getHeaders().size());
    assertEquals(2, ctx.clusters().size());
    
    ctx = new HttpContext(context, endpoints, mergers, null);
    assertSame(client, ctx.getClient());
    assertNotSame(headers, ctx.getHeaders());
    assertTrue(ctx.getHeaders().isEmpty());
    assertEquals(2, ctx.clusters().size());
    
    try {
      new HttpContext(null, endpoints, mergers, headers);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new HttpContext(context, null, mergers, headers);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new HttpContext(context, endpoints, null, headers);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void clusters() throws Exception {
    final HttpContext ctx = new HttpContext(context, endpoints, mergers, headers);
    List<ClusterConfig> clusters = ctx.clusters();
    assertEquals(2, clusters.size());
    assertEquals("http://myhost:4242", clusters.get(0).id());
    assertEquals(60000, clusters.get(0).timeout());
    assertSame(executor, clusters.get(0).remoteExecutor());
    
    assertEquals("http://otherhost:4242", clusters.get(1).id());
    assertEquals(60000, clusters.get(1).timeout());
    assertSame(executor, clusters.get(1).remoteExecutor());
    
    // switch order shows that between calls it can change
    urls = Lists.newArrayList("http://otherhost:4242", "http://myhost:4242");
    when(endpoints.getEndpoints(null)).thenReturn(urls);
    clusters = ctx.clusters();
    assertEquals(2, clusters.size());
    assertEquals("http://otherhost:4242", clusters.get(0).id());
    assertEquals(60000, clusters.get(0).timeout());
    assertSame(executor, clusters.get(0).remoteExecutor());
    
    assertEquals("http://myhost:4242", clusters.get(1).id());
    assertEquals(60000, clusters.get(1).timeout());
    assertSame(executor, clusters.get(1).remoteExecutor());
    
    urls = Lists.newArrayList();
    when(endpoints.getEndpoints(null)).thenReturn(urls);
    clusters = ctx.clusters();
    assertEquals(0, clusters.size());
  }
  
  @Test
  public void dataMerger() throws Exception {
    final HttpContext ctx = new HttpContext(context, endpoints, mergers, headers);
    assertNull(ctx.dataMerger(DataShardsGroup.TYPE));
    
    final DataShardMerger merger = new DataShardMerger();
    mergers.put(DataShardsGroup.TYPE, merger);
    assertSame(merger, ctx.dataMerger(DataShardsGroup.TYPE));
  }
}
