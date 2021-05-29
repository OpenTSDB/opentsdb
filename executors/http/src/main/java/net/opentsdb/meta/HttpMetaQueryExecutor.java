// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.exceptions.QueryExecutionCanceled;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.meta.BatchMetaQuery;
import net.opentsdb.meta.BatchMetaQuery.QueryType;
import net.opentsdb.meta.DefaultMetaQuery;
import net.opentsdb.meta.MetaDataStorageResult;
import net.opentsdb.meta.MetaDataStorageSchema;
import net.opentsdb.meta.MetaQuery;
import net.opentsdb.meta.NamespacedKey;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.DefaultSharedHttpClient;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.SharedHttpClient;

/**
 * Simple meta data query forwarder. For now it just sends to the endpoint given
 * and in the future we'll add some smart routing to point different namespaces
 * to different endpoints.
 * 
 * @since 3.0
 */
public class HttpMetaQueryExecutor extends BaseTSDBPlugin implements MetaDataStorageSchema {
  private static final Logger LOG = LoggerFactory.getLogger(HttpMetaQueryExecutor.class);
  
  public static final String TYPE = "HttpMetaExecutor";
  public static final String ENDPOINT = "http.meta.executor.endpoint";
  public static final String CLIENT_KEY = "http.meta.executor.client.id";
  
  /** The client to query. */
  private CloseableHttpAsyncClient client;
  
  @Override
  public Deferred<Map<NamespacedKey, MetaDataStorageResult>> runQuery(
      final BatchMetaQuery query, 
      final Span span) {
    final long start = DateTime.nanoTime();
    final Deferred<Map<NamespacedKey, MetaDataStorageResult>> deferred = 
        new Deferred<Map<NamespacedKey, MetaDataStorageResult>>();
    final HttpPost post = new HttpPost(tsdb.getConfig().getString(ENDPOINT));
    post.addHeader("Content-Type", "application/json");
    final String json;
    try {
      json = JSON.serializeToString(query);
      post.setEntity(new StringEntity(json));
    } catch (UnsupportedEncodingException e) {
      return Deferred.fromError(new RejectedExecutionException(
          "Failed to generate request", e));
    }
    
    class ResponseCallback implements FutureCallback<HttpResponse> {

      @Override
      public void completed(final HttpResponse response) {
        try {
          if (response.getStatusLine().getStatusCode() != 200) {
            // bad. Try getting a useful exception to hand up.
            final String parsed;
            try {
              parsed = DefaultSharedHttpClient.parseResponse(response, 0, 
                  tsdb.getConfig().getString(ENDPOINT));
              // TODO - no catch so just bubble up for now.
              deferred.callback(new QueryExecutionException(parsed,
                  response.getStatusLine().getStatusCode()));
              return;
            } catch (Exception e) {
              LOG.warn("Exception from endpoint in response: " 
                  + this, e);
              deferred.callback(e);
            }
          } else {
            // good
            final String json = DefaultSharedHttpClient.parseResponse(response, 0, 
                tsdb.getConfig().getString(ENDPOINT));
            final JsonNode root = JSON.getMapper().readTree(json);
            JsonNode results = root.get("results");
            if (results == null) {
              // could be an error, parse it.
              LOG.error("No JSON results from: " + json);
              deferred.callback(new QueryExecutionException("No JSON results from: " + json,
                  500));
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Successful response from [" 
                    + tsdb.getConfig().getString(ENDPOINT) + "] after " 
                    + DateTime.msFromNanoDiff(DateTime.nanoTime(), start) + "ms");
              }
              
              final Map<NamespacedKey, MetaDataStorageResult> map = Maps.newHashMap();
              for (final JsonNode result : results) {
                final NamespacedKey key;
                JsonNode namespaces = result.get("namespaces");
                if (namespaces == null || namespaces.size() < 1) {
                  // TODO - throw an error most likely, for now use "null"
                  key = new NamespacedKey("null", "null");
                } else {
                  key = new NamespacedKey(namespaces.get(0).asText(), 
                      namespaces.get(0).asText());
                }
                
                map.put(key, new HttpMetaResult(result));
              }
              deferred.callback(map);
            }
          }
        } catch (Exception e) {
          LOG.warn("Exception thrown when calling deferred on response: " 
              + this, e);
        }
      }

      @Override
      public void failed(final Exception ex) {
        // TODO - retries
        try {
          deferred.callback(ex);
        } catch (Exception e) {
          LOG.warn("Exception thrown when calling deferred on error handling: " 
              + this, e);
        }
      }

      @Override
      public void cancelled() {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Http query was canceled: " + this);
        }
        try {
          final Exception e = new QueryExecutionCanceled(
              "Query was canceled: " + tsdb.getConfig().getString(ENDPOINT), 400, 0); 
          deferred.callback(e);
        } catch (Exception e) {
          LOG.warn("Exception thrown when calling deferred on cancel: " 
              + this, e);
        }
      }
      
    }
    client.execute(post, new ResponseCallback());
    return deferred;
  }

  @Override
  public Deferred<MetaDataStorageResult> runQuery(final QueryPipelineContext context, 
                                                  final TimeSeriesDataSourceConfig config,
      Span span) {
    return Deferred.fromError(new UnsupportedOperationException(
        "Not forwarding data queries at this time."));
  }

  @Override
  public MetaQuery parse(final TSDB tsdb, 
                         final ObjectMapper mapper, 
                         final JsonNode jsonNode, 
                         final QueryType type) {
    return DefaultMetaQuery.parse(tsdb, mapper, jsonNode, type).build();
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? null : id;
    this.tsdb = tsdb;
    
    if (!tsdb.getConfig().hasProperty(ENDPOINT)) {
      tsdb.getConfig().register(ENDPOINT, null, true, 
          "The HTTP(s) endpoint to forward queries to.");
    }
    if (!tsdb.getConfig().hasProperty(CLIENT_KEY)) {
      tsdb.getConfig().register(CLIENT_KEY, 
          null, false,
          "The ID of the SharedHttpClient plugin to use. Defaults to `null`.");
    }
    
    final String client_id = tsdb.getConfig().getString(CLIENT_KEY);
    final SharedHttpClient shared_client = tsdb.getRegistry().getPlugin(
        SharedHttpClient.class, client_id);
    if (shared_client == null) {
      throw new IllegalArgumentException("No shared HTTP client found "
          + "for ID: " + (Strings.isNullOrEmpty(client_id) ? 
              "Default" : client_id));
    } else {
      client = shared_client.getClient();
    }
    
    return Deferred.fromResult(null);
  }
  
  @Override
  public String type() {
    return TYPE;
  }

}
