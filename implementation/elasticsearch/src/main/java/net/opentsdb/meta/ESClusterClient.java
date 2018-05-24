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

import java.util.List;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig.Builder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.stats.Span;

/**
 * A single cluster client.
 * 
 * @since 3.0
 */
public class ESClusterClient implements ESClient, TSDBPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(
      ESClusterClient.class);
  
  public static final String HOSTS_KEY = "es.hosts";
  public static final String QUERY_TIMEOUT_KEY = "es.client.timeout.query";
  public static final String CONNECTION_TIMEOUT_KEY = "es.client.timeout.connection";
  public static final String SOCKET_TIMEOUT_KEY = "es.client.timeout.socket";
  public static final String ENABLE_COMPRESSION_KEY = "es.client.compression.enable";
  public static final int QUERY_TIMEOUT_DEFAULT = 5000;
  public static final int CONNECTION_TIMEOUT_DEFAULT = 2000;
  public static final int SOCKET_TIMEOUT_DEFAULT = 2000;
  public static final int DEFAULT_PORT = 9200;
  public static final Header[] EMPTY_HEADERS = new Header[0];
  
  /** TSDB for stats. */
  private TSDB tsdb;
  
  /** The client. */
  protected RestHighLevelClient client;
  
  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    this.tsdb = tsdb;
    registerConfigs(tsdb);
    
    String raw_hosts = tsdb.getConfig().getString(ESClusterClient.HOSTS_KEY);
    if (Strings.isNullOrEmpty(raw_hosts)) {
      return Deferred.fromError(new ConfigurationException(
          "Missing the hosts config '" + ESClusterClient.HOSTS_KEY + "'"));
    }
    final String[] hosts = raw_hosts.split(",");
    if (hosts.length < 1) {
      return Deferred.fromError(new ConfigurationException(
          "Must have at least one host in '" + ESClusterClient.HOSTS_KEY 
          + "'"));
    }
    
    try {
    
      final HttpHost[] http_hosts = new HttpHost[hosts.length];
      int i = 0;
      for (final String host : hosts) {
        final String[] host_and_port = host.split(":");
        if (host_and_port.length < 2) {
          return Deferred.fromError(new ConfigurationException(
              "Failed to parse host. Must be of the format "
              + "'https://localhost:9200' " + host));
        }
        
        boolean tls = host_and_port[0].toLowerCase().trim().startsWith("https");
        if (!host_and_port[0].toLowerCase().trim().startsWith("http")) {
          return Deferred.fromError(new ConfigurationException(
              "Failed to parse host. Must be of the format "
              + "'https://localhost:9200' " + host));
        }
        
        http_hosts[i++] = new HttpHost(
            host_and_port[1].trim().substring(host_and_port[1].trim().indexOf("//") + 2),
            host_and_port.length < 3 ? DEFAULT_PORT : Integer.parseInt(host_and_port[2]),
            tls ? "https" : "http");
      }
      
      class RCCB implements RequestConfigCallback {
        @Override
        public Builder customizeRequestConfig(final Builder config) {
          config
              .setConnectionRequestTimeout(tsdb.getConfig().getInt(CONNECTION_TIMEOUT_KEY))
              .setConnectTimeout(tsdb.getConfig().getInt(CONNECTION_TIMEOUT_KEY))
              .setSocketTimeout(tsdb.getConfig().getInt(SOCKET_TIMEOUT_KEY));
          if (tsdb.getConfig().getBoolean(ENABLE_COMPRESSION_KEY)) {
            config.setContentCompressionEnabled(true)
                  .setDecompressionEnabled(true);
          }
          return config;
        }
      }
      
      final RestClientBuilder builder = RestClient.builder(http_hosts)
        .setRequestConfigCallback(new RCCB());
        //.setHttpClientConfigCallback(httpClientConfigCallback); // TODO
      
      client = new RestHighLevelClient(builder);
      LOG.info("Finished initializing ES client.");
      return Deferred.fromResult(null);
    } catch (Exception e) {
      LOG.error("Failed to initialize ES client", e);
      return Deferred.fromError(e);
    }
  }

  @Override
  public Deferred<Object> shutdown() {
    try {
      client.close();
      LOG.info("Finished shutting down ES clients.");
      return Deferred.fromResult(null);
    } catch (Exception e) {
      LOG.error("Failed to close ES client", e);
      return Deferred.fromError(e);
    }
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public Deferred<List<SearchResponse>> runQuery(final QueryBuilder query,
                                                 final String index,
                                                 final int size,
                                                 final Span span) {
    if (query == null) {
      return Deferred.fromError(new IllegalArgumentException(
          "Query cannot be null."));
    }
    if (Strings.isNullOrEmpty(index)) {
      return Deferred.fromError(new IllegalArgumentException(
          "Index cannot be null or empty."));
    }
    
    final Span child;
    if (span != null) {
      child = span.newChild(getClass() + "runQuery").start();
    } else {
      child = null;
    }
    
    final Deferred<List<SearchResponse>> deferred = 
        new Deferred<List<SearchResponse>>();
    
    class FutureCB implements ActionListener<SearchResponse> {        
      @Override
      public void onFailure(final Exception e) {
        tsdb.getStatsCollector().incrementCounter("es.client.query.exception");
        LOG.error("Unexpected failure from ES client", e);
        if (child != null) {
          child.setErrorTags(e).finish();
        }
        deferred.callback(e);
      }
      
      @Override
      public void onResponse(final SearchResponse response) {
        tsdb.getStatsCollector().incrementCounter("es.client.query.success");
        if (child != null) {
          child.setSuccessTags().finish();
        }
        deferred.callback(Lists.newArrayList(response));
      }
    }

    try {
      final SearchRequest search_request = new SearchRequest(); 
      final SearchSourceBuilder search_builder = new SearchSourceBuilder()
          .timeout(TimeValue.timeValueMillis(
            tsdb.getConfig().getLong(QUERY_TIMEOUT_KEY)))
          .size(size)
          .query(query);  
      search_request.source(search_builder);
      search_request.indices(index);
      
      client.searchAsync(search_request, new FutureCB(), EMPTY_HEADERS);
    } catch (Exception e) {
      LOG.error("Failed to execute query: " + query, e);
      deferred.callback(e);
    }
    
    return deferred;
  }
  
  /**
   * Package private helper for clients to register their configs.
   * @param tsdb A non-null TSDB.
   */
  static void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(HOSTS_KEY)) {
      tsdb.getConfig().register(HOSTS_KEY, null, false, 
          "A list of hosts in the ES cluster with the format of "
          + "'<protocol>://<host>[:<port>]'. E.g. "
          + "'https://localhost:9200'. Multiple entries should be "
          + "separated by a comma. If no port is present the default "
          + "port of '" + DEFAULT_PORT + "' is assumed.");
    }
    if (!tsdb.getConfig().hasProperty(CONNECTION_TIMEOUT_KEY)) {
      tsdb.getConfig().register(CONNECTION_TIMEOUT_KEY, 
          CONNECTION_TIMEOUT_DEFAULT,
          true, "The timeout in milliseconds before the connection "
              + "attempt is canceled.");
    }
    if (!tsdb.getConfig().hasProperty(SOCKET_TIMEOUT_KEY)) {
      tsdb.getConfig().register(SOCKET_TIMEOUT_KEY, SOCKET_TIMEOUT_DEFAULT,
          true, "The amount of time spent waiting on data from the "
              + "socket in milliseconds.");
    }
    if (!tsdb.getConfig().hasProperty(QUERY_TIMEOUT_KEY)) {
      tsdb.getConfig().register(QUERY_TIMEOUT_KEY, QUERY_TIMEOUT_DEFAULT,
          true, "The number of milliseconds to wait on a query execution.");
    }
    if (!tsdb.getConfig().hasProperty(ENABLE_COMPRESSION_KEY)) {
      tsdb.getConfig().register(ENABLE_COMPRESSION_KEY, true, true,
          "Whether or not compression is enabled for HTTP calls.");
    }
  }
}
