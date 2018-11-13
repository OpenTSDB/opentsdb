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
package net.opentsdb.query.execution;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.exceptions.QueryExecutionCanceled;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.SourceNode;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.SharedHttpClient;

/**
 * An executor that fires an HTTP query against a V3 endpoint for a metric,
 * serializing the semantic query and parsing a V3 response.
 * 
 * @since 3.0
 */
public class HttpQueryV3Source extends AbstractQueryNode implements SourceNode {
  private static final Logger LOG = LoggerFactory.getLogger(
      HttpQueryV3Source.class);
  
  /** The query to execute. */
  private final TimeSeriesDataSourceConfig config;
  
  /** The client to query. */
  private final CloseableHttpAsyncClient client;
  
  /** The full URL to POST to including protocol, host, port and endpoint */
  private final String endpoint;
  
  /**
   * Default ctor.
   * @param factory The non-null factory.
   * @param context The non-null query context.
   * @param config The non-null config to send (with pushdowns if required)
   * @param client The non-null client to use.
   * @param endpoint The non-null endpoint with protocol, host, port and 
   * path.
   */
  public HttpQueryV3Source(final QueryNodeFactory factory,
                           final QueryPipelineContext context,
                           final TimeSeriesDataSourceConfig config,
                           final CloseableHttpAsyncClient client,
                           final String endpoint) {
    super(factory, context);
    this.config = config;
    this.client = client;
    this.endpoint = endpoint;
  }
  
  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onNext(final QueryResult next) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fetchNext(final Span span) {
    SemanticQuery.Builder builder = SemanticQuery.newBuilder()
        .setStart(context.query().getStart())
        .setEnd(context.query().getEnd())
        .setMode(context.query().getMode())
        .setTimeZone(context.query().getTimezone());
    
    // TODO - we need to confirm the graph links.
    Map<String, QueryNodeConfig> pushdowns = Maps.newHashMap();
    pushdowns.put(config.getId(), 
        ((TimeSeriesDataSourceConfig.Builder) config.toBuilder())
          .setPushDownNodes(Collections.emptyList())
          .build());
    for (final QueryNodeConfig pushdown : config.getPushDownNodes()) {
      pushdowns.put(pushdown.getId(), pushdown);
    }
    
    for (final QueryNodeConfig pushdown : pushdowns.values()) {
      if (pushdown.getSources().isEmpty()) {
        // just the source, add it
        builder.addExecutionGraphNode(pushdown);
      } else {
        List<String> sources = pushdown.getSources();
        for (final String source : sources) {
          if (pushdowns.containsKey(source)) {
            builder.addExecutionGraphNode(pushdown.toBuilder()
                .setSources(Lists.newArrayList(source))
                .build());
            break;
          }
        }
      }
    }
    
    final HttpPost post = new HttpPost(endpoint);
    post.setHeader("Content-Type", "application/json");
    
    // may need to pass down a cookie.
    if (context.queryContext().authState() != null && 
        !Strings.isNullOrEmpty(context.queryContext().authState().getTokenType()) &&
        context.queryContext().authState().getTokenType().equalsIgnoreCase("cookie")) {
      post.setHeader("Cookie", new String(
          context.queryContext().authState().getToken(), Const.UTF8_CHARSET));
    }
    
    try {
      post.setEntity(new StringEntity(
          JSON.serializeToString(builder.build())));
    } catch (UnsupportedEncodingException e) {
      try {
        final Exception ex = new RejectedExecutionException(
            "Failed to generate request", e);
        sendUpstream(ex);
      } catch (IllegalStateException ex) {
        LOG.error("Unexpected state halting execution with a failed "
            + "conversion: " + this);
        sendUpstream(ex);
      } catch (Exception ex) {
        LOG.error("Unexpected exception halting execution with a failed "
            + "conversion: " + this);
        sendUpstream(ex);
      }
      return;
    }
    
    /** Does the fun bit of parsing the response and calling the deferred. */
    class ResponseCallback implements FutureCallback<HttpResponse> {      
      @Override
      public void cancelled() {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Http query was canceled: " + this);
        }
        try {
          final Exception e = new QueryExecutionCanceled(
              "Query was canceled: " + endpoint, 400, 0); 
          sendUpstream(e);
        } catch (IllegalStateException e) {
          // already called, ignore it.
        } catch (Exception e) {
          LOG.warn("Exception thrown when calling deferred on cancel: " 
              + this, e);
        }
      }

      @Override
      public void completed(final HttpResponse response) {
        final Header header = response.getFirstHeader("X-Served-By");
        String host = "unknown";
        if (header != null && header.getValue() != null) {
          host = header.getValue();
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Response from endpoint: " + endpoint + " (" + host 
              + ") received");
        }

        String json = null;
        try {
          json = SharedHttpClient.parseResponse(response, 0, host);
          final JsonNode root = JSON.getMapper().readTree(json);
          JsonNode results = root.get("results");
          if (results == null) {
            // could be an error, parse it.
          } else {
            for (final JsonNode result : results) {
              sendUpstream(new HttpQueryV3Result(HttpQueryV3Source.this, result));
            }
          }

        } catch (IllegalStateException caught) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Callback was already called but we recieved "
                + "a result: " + this);
          }
        } catch (Exception e) {
          LOG.error("Failure handling response: " + response + "\nQuery: " 
              + json, e);
          if (e instanceof QueryExecutionException) {
            try {
              sendUpstream(e);
            } catch (IllegalStateException caught) {
              // ignore;
            } catch (Exception ex) {
              LOG.warn("Unexpected exception when handling exception: " 
                  + this, e);
            }
          } else {
            try {
              final Exception ex = new QueryExecutionException(
                  "Unexepected exception: " + endpoint, 500, 0, e);
              sendUpstream(ex);
            } catch (IllegalStateException caught) {
              // ignore;
            } catch (Exception ex) {
              LOG.warn("Unexpected exception when handling exception: " 
                  + this, e);
            }
          }
        }
      }

      @Override
      public void failed(final Exception e) {
        // TODO possibly retry?
        sendUpstream(e);
      }
    }
    client.execute(post, new ResponseCallback());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sent Http query to a TSD: " + endpoint);
    }
  }

  @Override
  public TimeStamp sequenceEnd() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Schema schema() {
    // TODO Auto-generated method stub
    return null;
  }
  
}
