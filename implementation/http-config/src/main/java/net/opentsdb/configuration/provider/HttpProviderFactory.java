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

import java.io.IOException;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.Configuration;

/**
 * A factory that handles fetching configs from a web server using HTTP or 
 * TLS over HTTP.
 * 
 * @since 3.0
 */
public class HttpProviderFactory implements ProtocolProviderFactory {

  /** The shared client. */
  protected CloseableHttpAsyncClient client;
  
  public HttpProviderFactory() {
    // TODO - configs!
    client = HttpAsyncClients.custom()
      .setDefaultIOReactorConfig(IOReactorConfig.custom()
          .setIoThreadCount(1).build())
      .setMaxConnTotal(16)
      .setMaxConnPerRoute(16)
      .build();
    client.start();
  }
  
  @Override
  public void close() throws IOException {
    client.close();
  }

  @Override
  public Provider newInstance(final Configuration config, 
                              final HashedWheelTimer timer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Provider newInstance(final Configuration config, 
                              final HashedWheelTimer timer,
                              final String uri) {
    return new HttpProvider(this, config, timer, uri);
  }
  
  @Override
  public boolean isReloadable() {
    return true;
  }

  @Override
  public String description() {
    return "Fetches configuration information from an HTTP source.";
  }

  @Override
  public String simpleName() {
    return "http";
  }

  @Override
  public String protocol() {
    return "http";
  }

  @Override
  public boolean handlesProtocol(String uri) {
    uri = uri.toLowerCase();
    if (uri.startsWith("http://") ||
        uri.startsWith("https://")) {
      return true;
    }
    return false;
  }
  
  CloseableHttpAsyncClient client() {
    return client;
  }
}
