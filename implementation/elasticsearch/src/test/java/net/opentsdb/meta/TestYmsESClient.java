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
package net.opentsdb.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ YmsESClient.class, TransportClient.class })
public class TestYmsESClient {

  private MockTSDB tsdb;
  private List<TransportClient> clients;
  private List<Settings> settings;

  @Before
  public void before() throws Exception {
    tsdb = new MockTSDB();
    clients = Lists.newArrayList();
    settings = Lists.newArrayList();
    YmsESClient.registerConfigs(tsdb);
    tsdb.config.override(YmsESClient.CLUSTERS_KEY, "esbf1,esgq1");

    PowerMockito.whenNew(TransportClient.class).withAnyArguments()
            .thenAnswer(new Answer<TransportClient>() {
              @Override
              public TransportClient answer(InvocationOnMock invocation)
                      throws Throwable {
                if (invocation.getArguments()[0] == null) {
                  return mock(TransportClient.class);
                }
                settings.add((Settings) invocation.getArguments()[0]);
                TransportClient client = mock(TransportClient.class);
                clients.add(client);
                return client;
              }
            });
  }

  @Test
  public void intialize() throws Exception {
    tsdb.config.override(YmsESClient.HOSTS_KEY,
            "localhost:4242,remotehost:9393");

    YmsESClient es = new YmsESClient();
    assertNull(es.initialize(tsdb, null).join());
    assertEquals(2, es.clients.size());
    assertEquals(2, clients.size());
    assertEquals("esbf1", settings.get(0).get("cluster.name"));
    assertEquals("esgq1", settings.get(1).get("cluster.name"));
    assertEquals(YmsESClient.PING_TIMEOUT_DEFAULT,
            settings.get(0).get("client.transport.ping_timeout"));
    assertEquals(YmsESClient.PING_TIMEOUT_DEFAULT,
            settings.get(1).get("client.transport.ping_timeout"));
    assertTrue(settings.get(0).getAsBoolean("client.transport.sniff", false));
    assertTrue(settings.get(1).getAsBoolean("client.transport.sniff", false));
    verify(clients.get(0), times(1)).addTransportAddress(
            new InetSocketTransportAddress("localhost", 4242));
    verify(clients.get(1), times(1)).addTransportAddress(
            new InetSocketTransportAddress("remotehost", 9393));
  }

  @Test
  public void intializeOverrides() throws Exception {
    tsdb.config.override(YmsESClient.PING_TIMEOUT_KEY, "60s");
    tsdb.config.override(YmsESClient.SNIFF_KEY, "false");
    tsdb.config.override(YmsESClient.HOSTS_KEY,  "localhost:4242,remotehost:9393");

    YmsESClient es = new YmsESClient();
    assertNull(es.initialize(tsdb, null).join());

    assertEquals(2, es.clients.size());
    assertEquals("esbf1", settings.get(0).get("cluster.name"));
    assertEquals("esgq1", settings.get(1).get("cluster.name"));
    assertEquals("60s", settings.get(0).get("client.transport.ping_timeout"));
    assertEquals("60s", settings.get(1).get("client.transport.ping_timeout"));
    assertFalse(settings.get(0).getAsBoolean("client.transport.sniff", false));
    assertFalse(settings.get(1).getAsBoolean("client.transport.sniff", false));
    verify(clients.get(0), times(1)).addTransportAddress(
            new InetSocketTransportAddress("localhost", 4242));
    verify(clients.get(1), times(1)).addTransportAddress(
            new InetSocketTransportAddress("remotehost", 9393));
  }

  @Test
  public void intializeDefaultPorts() throws Exception {
    tsdb.config.override(YmsESClient.HOSTS_KEY,  "localhost,remotehost");

    YmsESClient es = new YmsESClient();
    assertNull(es.initialize(tsdb, null).join());

    assertEquals(2, es.clients.size());
    assertEquals("esbf1", settings.get(0).get("cluster.name"));
    assertEquals("esgq1", settings.get(1).get("cluster.name"));
    assertEquals(YmsESClient.PING_TIMEOUT_DEFAULT,
            settings.get(0).get("client.transport.ping_timeout"));
    assertEquals(YmsESClient.PING_TIMEOUT_DEFAULT,
            settings.get(1).get("client.transport.ping_timeout"));
    assertTrue(settings.get(0).getAsBoolean("client.transport.sniff", false));
    assertTrue(settings.get(1).getAsBoolean("client.transport.sniff", false));
    verify(clients.get(0), times(1)).addTransportAddress(
            new InetSocketTransportAddress("localhost", YmsESClient.DEFAULT_PORT));
    verify(clients.get(1), times(1)).addTransportAddress(
            new InetSocketTransportAddress("remotehost", YmsESClient.DEFAULT_PORT));
  }

  @Test
  public void intializeOneHost() throws Exception {
    tsdb.config.override(YmsESClient.HOSTS_KEY,  "localhost");

    YmsESClient es = new YmsESClient();
    assertNull(es.initialize(tsdb, null).join());

    assertEquals(1, es.clients.size());
    assertEquals("esbf1", settings.get(0).get("cluster.name"));
    assertEquals(YmsESClient.PING_TIMEOUT_DEFAULT,
            settings.get(0).get("client.transport.ping_timeout"));
    assertTrue(settings.get(0).getAsBoolean("client.transport.sniff", false));
    verify(clients.get(0), times(1)).addTransportAddress(
            new InetSocketTransportAddress("localhost", YmsESClient.DEFAULT_PORT));
  }

  @Test
  public void intializeMissingHost() throws Exception {
    //tsdb.config.override(YmsESClient.HOSTS_KEY, "localhost:9300");

    YmsESClient es = new YmsESClient();
    try {
      es.initialize(tsdb, null).join();
      fail("Expected ConfigurationException");
    } catch (ConfigurationException e) { }
  }

  @Test
  public void intializeMissingClusters() throws Exception {
    tsdb.config.override(YmsESClient.CLUSTERS_KEY, null);

    YmsESClient es = new YmsESClient();
    try {
      es.initialize(tsdb, null).join();
      fail("Expected ConfigurationException");
    } catch (ConfigurationException e) { }
  }

  @Test
  public void shutdown() throws Exception {
    tsdb.config.override(YmsESClient.HOSTS_KEY,
            "localhost:4242,remotehost:9393");
    YmsESClient es = new YmsESClient();
    assertNull(es.initialize(tsdb, null).join());
    assertEquals(2, es.clients.size());
    assertNull(es.shutdown().join());
    for (final TransportClient client : clients) {
      verify(client, times(1)).close();
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void runQuery() throws Exception {
    tsdb.config.override(YmsESClient.HOSTS_KEY,
            "localhost:4242,remotehost:9393");
    YmsESClient es = new YmsESClient();
    assertNull(es.initialize(tsdb, null).join());

    SearchSourceBuilder query = mock(SearchSourceBuilder.class);
    SearchResponse response_a = mock(SearchResponse.class);
    SearchResponse response_b = mock(SearchResponse.class);
    final List<SearchRequestBuilder> builders =
            Lists.newArrayListWithCapacity(clients.size());
    final ActionListener[] listener = new ActionListener[clients.size()];
    int[] i = new int[1];
    for (final TransportClient client : clients) {
      SearchRequestBuilder builder = spy(new SearchRequestBuilder(client));
      builders.add(builder);
      when(client.prepareSearch(anyString())).thenReturn(builder);
      ListenableActionFuture laf = mock(ListenableActionFuture.class);
      doReturn(laf).when(builder).execute();
      doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          listener[i[0]++] = (ActionListener) invocation.getArguments()[0];
          return null;
        }
      }).when(laf).addListener(any(ActionListener.class));
    }

    Deferred<List<SearchResponse>> deferred = es.runQuery(query, "idx", null);
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    verify(builders.get(0), times(1)).setSearchType(SearchType.DEFAULT);
    verify(builders.get(1), times(1)).setSearchType(SearchType.DEFAULT);
    verify(builders.get(0), times(1)).setSource(query.toString());
    verify(builders.get(1), times(1)).setSource(query.toString());


    // first response
    listener[0].onResponse(response_a);
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }

    // second response completes it.
    listener[1].onResponse(response_b);
    List<SearchResponse> responses = deferred.join();
    assertEquals(2, responses.size());
    assertSame(response_a, responses.get(0));
    assertSame(response_b, responses.get(1));

    // new, first throws exception, second is ok.
    i[0] = 0; // reset callback counter
    deferred = es.runQuery(query, "idx", null);
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }

    listener[0].onFailure(new UnitTestException());
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }

    // second response completes it.
    listener[1].onResponse(response_b);
    responses = deferred.join();
    assertEquals(1, responses.size());
    assertSame(response_b, responses.get(0));

    // new, first ok, second is bad
    i[0] = 0; // reset callback counter
    deferred = es.runQuery(query, "idx",null);
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }

    listener[0].onResponse(response_a);
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }

    // second response completes it.
    listener[1].onFailure(new UnitTestException());
    responses = deferred.join();
    assertEquals(1, responses.size());
    assertSame(response_a, responses.get(0));

    // new, exception for both
    i[0] = 0; // reset callback counter
    deferred = es.runQuery(query, "idx", null);
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }

    listener[0].onFailure(new UnitTestException());
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }

    // second response completes it.
    listener[1].onFailure(new UnitTestException());
    try {
      deferred.join();
      fail("Expected UnitTestException");
    } catch (UnitTestException e) { }

    // bad queries
    try {
      es.runQuery(null, "idx",null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    try {
      es.runQuery(query, null, null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    try {
      es.runQuery(query, "", null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}