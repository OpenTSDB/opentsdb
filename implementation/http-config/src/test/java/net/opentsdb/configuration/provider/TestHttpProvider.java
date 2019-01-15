// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.configuration.provider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.Future;
import net.opentsdb.configuration.Configuration;

public class TestHttpProvider {
  private final String URI = "http://localhost:4242";

  private HttpProviderFactory factory;
  private Configuration config;
  private HashedWheelTimer timer;
  private CloseableHttpAsyncClient client;
  private HttpUriRequest request;
  private FutureCallback<HttpResponse> callback;
  
  @Before
  public void before() throws Exception {
    factory = mock(HttpProviderFactory.class);
    config = mock(Configuration.class);
    timer = mock(HashedWheelTimer.class);
    client = mock(CloseableHttpAsyncClient.class);
    
    when(client.execute(any(HttpUriRequest.class), any(FutureCallback.class)))
      .thenAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          request = (HttpUriRequest) invocation.getArguments()[0];
          callback = (FutureCallback<HttpResponse>) invocation.getArguments()[1];
          return null;
        }
      });
    when(factory.client()).thenReturn(client);
  }
  
  @Test
  public void success() throws Exception {
    String json = "{\"key\":\"value\"}";
    HttpResponse response = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);
    HttpEntity entity = new StringEntity(json);
    when(response.getStatusLine()).thenReturn(status);
    when(response.getEntity()).thenReturn(entity);
    when(status.getStatusCode()).thenReturn(200);
    Future<HttpResponse> future = mock(Future.class);
    when(client.execute(any(HttpUriRequest.class), any(FutureCallback.class)))
      .thenAnswer(new Answer<Future<HttpResponse>>() {
        @Override
        public Future<HttpResponse> answer(InvocationOnMock invocation) throws Throwable {
          request = (HttpUriRequest) invocation.getArguments()[0];
          return future;
        }
      });
    when(future.get()).thenReturn(response);
    
    HttpProvider provider = new HttpProvider(factory, config, timer, URI);
    assertEquals(1, provider.cache.size());
  }
  
  @Test
  public void badStatusCode() throws Exception {
    String json = "{\"key\":\"value\"}";
    HttpResponse response = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);
    HttpEntity entity = new StringEntity(json);
    when(response.getStatusLine()).thenReturn(status);
    when(response.getEntity()).thenReturn(entity);
    when(status.getStatusCode()).thenReturn(400);
    Future<HttpResponse> future = mock(Future.class);
    when(client.execute(any(HttpUriRequest.class), any(FutureCallback.class)))
      .thenAnswer(new Answer<Future<HttpResponse>>() {
        @Override
        public Future<HttpResponse> answer(InvocationOnMock invocation) throws Throwable {
          request = (HttpUriRequest) invocation.getArguments()[0];
          return future;
        }
      });
    when(future.get()).thenReturn(response);
    
    HttpProvider provider = new HttpProvider(factory, config, timer, URI);
    assertEquals(0, provider.cache.size());
  }
}
