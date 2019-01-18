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
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.TSDB;
import net.opentsdb.exceptions.RemoteQueryExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DefaultSharedHttpClient.class, HttpAsyncClients.class, 
  HttpAsyncClientBuilder.class }) 
public class TestDefaultSharedHttpClient {

  private CloseableHttpAsyncClient client;
  
  @Before
  public void before() throws Exception {
    client = mock(CloseableHttpAsyncClient.class);
    
    PowerMockito.mockStatic(HttpAsyncClients.class);
    final HttpAsyncClientBuilder builder = 
        PowerMockito.mock(HttpAsyncClientBuilder.class);
    when(HttpAsyncClients.custom()).thenReturn(builder);
    
    PowerMockito.when(builder
        .setDefaultIOReactorConfig(any(IOReactorConfig.class)))
          .thenReturn(builder);
    when(builder.setMaxConnTotal(anyInt())).thenReturn(builder);
    when(builder.setMaxConnPerRoute(anyInt())).thenReturn(builder);
    when(builder.build()).thenReturn(client);
    
  }
  
  @Test
  public void initializeAndShutdown() throws Exception {
    TSDB tsdb = mock(TSDB.class);
    when(tsdb.getConfig()).thenReturn(mock(Configuration.class));
    DefaultSharedHttpClient shared = new DefaultSharedHttpClient();
    assertNull(shared.initialize(tsdb, null).join(250));
    assertSame(client, shared.getClient());
    assertNull(shared.shutdown().join(250));
    verify(client, times(1)).close();
  }
  
  @Test
  public void parseResponse() throws Exception {
    HttpResponse response = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);
    when(response.getStatusLine()).thenReturn(status);
    
    StringEntity entity = new StringEntity("Hello!");
    when(response.getEntity()).thenReturn(entity);
    
    // raw
    when(status.getStatusCode()).thenReturn(200);
    assertEquals("Hello!", DefaultSharedHttpClient.parseResponse(response, 0, "unknown"));
    
    // TODO - figure out how to test the compressed entities. Looks like it's a 
    // bit of a pain.
    
    // non-200 non-json
    when(status.getStatusCode()).thenReturn(400);
    try {
      DefaultSharedHttpClient.parseResponse(response, 0, "unknown");
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { 
      assertEquals("Hello!", e.getMessage());
    }
    
    // non-200 JSON
    when(status.getStatusCode()).thenReturn(400);
    entity = new StringEntity("{\"error\":{\"message\":\"Boo!\"}}");
    when(response.getEntity()).thenReturn(entity);
    try {
      DefaultSharedHttpClient.parseResponse(response, 0, "unknown");
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { 
      assertEquals("Boo!", e.getMessage());
    }
    
    // null entity
    when(status.getStatusCode()).thenReturn(200);
    when(response.getEntity()).thenReturn(null);
    try {
      DefaultSharedHttpClient.parseResponse(response, 0, "unknown");
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { }
  }
}
