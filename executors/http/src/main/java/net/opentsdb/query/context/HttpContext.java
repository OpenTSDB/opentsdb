// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.DataMerger;
import net.opentsdb.query.execution.ClusterConfig;
import net.opentsdb.query.execution.HttpEndpoints;
import net.opentsdb.query.execution.HttpQueryV2Executor;
import net.opentsdb.query.execution.QueryExecutor;

/**
 * TODO - stub
 *
 * @since 3.0
 */
public class HttpContext implements RemoteContext {

  /** The query context this belongs to. */
  private final QueryContext context;
  
  /** The endpoints that we'll hit with queries. VIPs for now. */
  private final HttpEndpoints endpoints;
  
  /** The merger registry to use for data. */
  private final Map<TypeToken<?>, DataMerger<?>> mergers;
  
  /** The headers to pass on downstream. */
  private final Map<String, String> headers;
  
  /**
   * Default ctor.
   * @param context The query context this belongs to.
   * @param endpoints The endpoints we'll hit with queries.
   * @param mergers The merger registry to use for multi-colo queries.
   * @param headers The headers to pass downstream. May be null.
   * @throws IllegalArgumentException If the context, endpoints or mergers map
   * was null.
   */
  public HttpContext(final QueryContext context, 
                     final HttpEndpoints endpoints,
                     final Map<TypeToken<?>, DataMerger<?>> mergers,
                     final Map<String, String> headers) {
    if (context == null) {
      throw new IllegalArgumentException("Context cannot be null.");
    }
    if (endpoints == null) {
      throw new IllegalArgumentException("Endpoints cannot be null.");
    }
    if (mergers == null) {
      throw new IllegalArgumentException("Mergers map cannot be null.");
    }
    this.context = context;
    this.endpoints = endpoints;
    this.mergers = mergers;
    this.headers = headers == null ? 
        Collections.<String, String>emptyMap() : headers;
  }
  
  /** @return The headers to pass downstream. May be an empty map. */
  public Map<String, String> getHeaders() {
    return headers;
  }
  
  /** @return An HTTP client. Callers are responsible for closing the client. */
  public CloseableHttpAsyncClient getClient() {
    final CloseableHttpAsyncClient client = HttpAsyncClients.custom()
        .setDefaultIOReactorConfig(IOReactorConfig.custom()
            .setIoThreadCount(1).build())
        .setMaxConnTotal(1)
        .setMaxConnPerRoute(1)
        .build();
    client.start();
    return client;
  }

  /** @return The cluster config sorted on priority. */
  @Override
  public List<ClusterConfig> clusters() {
    final List<String> urls = endpoints.getEndpoints(null); // default
    final List<ClusterConfig> configs = 
        Lists.newArrayListWithExpectedSize(urls.size());
    for (int i = 0; i < urls.size(); i++) {
      configs.add(new HttpCluster(i, urls));
    }
    return configs;
  }

  /** @return A data merger if registered, otherwise null. */
  @Override
  public DataMerger<?> dataMerger(final TypeToken<?> type) {
    return mergers.get(type);
  }
  
  /** A class that implements a ClusterConfig based on the endpoints config. */
  class HttpCluster implements ClusterConfig {
    private final int index;
    private final List<String> clusters;
    
    HttpCluster(final int index, final List<String> clusters) {
      this.index = index;
      this.clusters = clusters;
    }
    
    @Override
    public String id() {
      return clusters.get(index);
    }

    @Override
    public long timeout() {
      return endpoints.clusterTimeout();
    }

    @Override
    public QueryExecutor<?> remoteExecutor() {
      // TODO - clean up to allow for other executors.
      return new HttpQueryV2Executor(context, clusters.get(index));
    }
    
  }
}
