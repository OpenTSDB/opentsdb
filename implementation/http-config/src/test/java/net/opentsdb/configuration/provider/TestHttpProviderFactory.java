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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestHttpProviderFactory {

  private MockedStatic<HttpAsyncClients> mockedHttpAsyncClients;

private CloseableHttpAsyncClient client;
  
  @Before
  public void before() throws Exception {
    mockedHttpAsyncClients = Mockito.mockStatic(HttpAsyncClients.class);
    client = mock(CloseableHttpAsyncClient.class);
    final HttpAsyncClientBuilder builder = 
        Mockito.mock(HttpAsyncClientBuilder.class);
    mockedHttpAsyncClients.when(HttpAsyncClients::custom).thenReturn(builder);
    
    Mockito.when(builder
        .setDefaultIOReactorConfig(any(IOReactorConfig.class)))
          .thenReturn(builder);
    when(builder.setMaxConnTotal(anyInt())).thenReturn(builder);
    when(builder.setMaxConnPerRoute(anyInt())).thenReturn(builder);
    when(builder.build()).thenReturn(client);
  }

  @After
  public void tearDownStaticMocks() {
    mockedHttpAsyncClients.closeOnDemand();
  }
  
  @Test
  public void initAndShutdown() throws Exception {
    HttpProviderFactory factory = new HttpProviderFactory();
    assertSame(client, factory.client);
    factory.close();
    verify(client, times(1)).close();
  }
  
  @Test
  public void handlesProtocol() throws Exception {
    HttpProviderFactory factory = new HttpProviderFactory();
    assertTrue(factory.handlesProtocol("http://localhost"));
    assertTrue(factory.handlesProtocol("Http://localhost"));
    assertTrue(factory.handlesProtocol("https://localhost"));
    assertTrue(factory.handlesProtocol("HtTpS://localhost"));
    assertFalse(factory.handlesProtocol("git://localhost"));
  }
}
