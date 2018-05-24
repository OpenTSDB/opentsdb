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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.http.Header;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.meta.ESClusterClient;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ESClusterClient.class, RestHighLevelClient.class })
public class TestESClusterClient {

  private MockTSDB tsdb;
  private RestHighLevelClient client;
  
  @Before
  public void before() throws Exception {
    client = PowerMockito.mock(RestHighLevelClient.class);
    tsdb = new MockTSDB();
    ESClusterClient.registerConfigs(tsdb);
    PowerMockito.whenNew(RestHighLevelClient.class).withAnyArguments()
     .thenReturn(client);
  }
  
  @Test
  public void intialize() throws Exception {
    tsdb.config.override(ESClusterClient.HOSTS_KEY, "https://localhost:4242");
    
    ESClusterClient es = new ESClusterClient();
    assertNull(es.initialize(tsdb).join());
    assertSame(client, es.client);
  }
  
  @Test
  public void intializeOverrides() throws Exception {
    tsdb.config.override(ESClusterClient.CONNECTION_TIMEOUT_KEY, 5000);
    tsdb.config.override(ESClusterClient.SOCKET_TIMEOUT_KEY, 1000);
    tsdb.config.override(ESClusterClient.QUERY_TIMEOUT_KEY, 500);
    tsdb.config.override(ESClusterClient.HOSTS_KEY, "http://localhost:9300");
    
    ESClusterClient es = new ESClusterClient();
    assertNull(es.initialize(tsdb).join());
    assertSame(client, es.client);
  }
  
  @Test
  public void intializeMissingHost() throws Exception {
    //tsdb.config.override(ESClusterClient.HOSTS_KEY, "localhost:9300");
    
    ESClusterClient es = new ESClusterClient();
    try {
      es.initialize(tsdb).join();
      fail("Expected ConfigurationException");
    } catch (ConfigurationException e) { }
  }
  
  @Test
  public void intializeMissingProtocol() throws Exception {
    tsdb.config.override(ESClusterClient.HOSTS_KEY, "localhost:9300");
    
    ESClusterClient es = new ESClusterClient();
    try {
      es.initialize(tsdb).join();
      fail("Expected ConfigurationException");
    } catch (ConfigurationException e) { }
  }
  
  @Test
  public void intializeDefaultPort() throws Exception {
    tsdb.config.override(ESClusterClient.HOSTS_KEY, "http://localhost");
    
    ESClusterClient es = new ESClusterClient();
    assertNull(es.initialize(tsdb).join());
    assertSame(client, es.client);
  }
  
  // TODO - fix the mocking!
//  @Test
//  public void shutdown() throws Exception {
//    tsdb.config.override(ESClusterClient.HOSTS_KEY, "localhost");
//    
//    ESClusterClient es = new ESClusterClient();
//    assertNull(es.initialize(tsdb).join());
//    assertNull(es.shutdown().join());
//    verify(client, times(1)).close();
//  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void runQuery() throws Exception {
    tsdb.config.override(ESClusterClient.HOSTS_KEY, "http://localhost");
    ESClusterClient es = new ESClusterClient();
    assertNull(es.initialize(tsdb).join());
    
    QueryBuilder query = mock(QueryBuilder.class);
    // TODO - fix these, something's wrong with the mocking.
//    SearchResponse response = mock(SearchResponse.class);
//    final ActionListener[] listener = new ActionListener[1];
//    doAnswer(new Answer<Void>() {
//      @Override
//      public Void answer(InvocationOnMock invocation) throws Throwable {
//        listener[0] = (ActionListener) invocation.getArguments()[1];
//        return null;
//      }
//    }).when(client).searchAsync(any(SearchRequest.class), 
//        any(ActionListener.class), any(Header[].class));
//    
//    Deferred<List<SearchResponse>> deferred = es.runQuery(query, "idx", null); 
//    try {
//      deferred.join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//        
//    listener[0].onResponse(null);
//    //assertSame(response, deferred.join().get(0));
//    
//    // exception returned
//    deferred = es.runQuery(query, "idx", null); 
//    try {
//      deferred.join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    listener[0].onFailure(new UnitTestException());
//    try {
//      deferred.join();
//      fail("Expected UnitTestException");
//    } catch (UnitTestException e) { }
//    
//    // exception thrown
//    doThrow(new UnitTestException()).when(client).searchAsync(
//        any(SearchRequest.class), any(ActionListener.class), 
//        any(Header[].class));
//    deferred = es.runQuery(query, "idx", null); 
//    try {
//      deferred.join();
//      fail("Expected UnitTestException");
//    } catch (UnitTestException e) { }
    
    // bad queries
    try {
      es.runQuery(null, "idx", 1, null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      es.runQuery(query, null, 1, null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      es.runQuery(query, "", 1, null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
