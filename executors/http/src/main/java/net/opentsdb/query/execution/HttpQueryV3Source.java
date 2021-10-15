// This file is part of OpenTSDB.
// Copyright (C) 2018-2021  The OpenTSDB Authors.
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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.RejectedExecutionException;
import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.exceptions.QueryExecutionCanceled;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.exceptions.RemoteQueryExecutionException;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.BadQueryResult;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesDataSourceConfig.Builder;
import net.opentsdb.query.filter.DefaultNamedFilter;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.StatsCollector.StatsTimer;
import net.opentsdb.storage.SourceNode;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.DefaultSharedHttpClient;
import net.opentsdb.utils.JSON;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An executor that fires an HTTP query against a V3 endpoint for a metric,
 * serializing the semantic query and parsing a V3 response.
 * 
 * @since 3.0
 */
public class HttpQueryV3Source extends AbstractQueryNode implements SourceNode, Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(
      HttpQueryV3Source.class);
  
  public static final String REMOTE_LATENCY_METRIC = "tsdb.executor.httpv3.latency";
  
  /** The query to execute. */
  private final TimeSeriesDataSourceConfig<? extends Builder, ? 
      extends TimeSeriesDataSourceConfig> config;
  
  /** The client to query. */
  private final CloseableHttpAsyncClient client;
  
  /** The hostname to post to with protocol, host and port. */
  private String host;
  
  /** The URL endpoint. */
  private final String endpoint;

  private Exception exception;
  private QueryResult result;
  
  /**
   * Default ctor.
   * @param factory The non-null factory.
   * @param context The non-null query context.
   * @param config The non-null config to send (with pushdowns if required)
   * @param client The non-null client to use.
   * @param host The non-null and non-empty host name with protocol and port if
   * required.
   * @param endpoint The non-null and non-empty endpoint, e.g. `api/query/graph`
   */
  public HttpQueryV3Source(final QueryNodeFactory factory,
                           final QueryPipelineContext context,
                           final TimeSeriesDataSourceConfig config,
                           final CloseableHttpAsyncClient client,
                           final String host,
                           final String endpoint) {
    super(factory, context);
    this.config = config;
    this.client = client;
    this.host = host;
    this.endpoint = endpoint;
  }
  
  @Override
  public TimeSeriesDataSourceConfig config() {
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
    if (Strings.isNullOrEmpty(host)) {
      // we didn't have a valid host. Check one more time as a last chance
      host = ((BaseHttpExecutorFactory) factory).nextHost();
      if (Strings.isNullOrEmpty(host)) {
        context.queryContext().logWarn(this, "No valid remote " +
                "endpoint found.");
        sendUpstream(BadQueryResult.newBuilder()
                .setNode(HttpQueryV3Source.this)
                .setException(new QueryDownstreamException("No valid remote endpoint found."))
                .setDataSource(pickId())
                .build());
        return;
      }

      // we can move forward!
      context.queryContext().logInfo(this, "Null host initially, " +
              "found [" + host + "] on the actual fetch attempt.");
      LOG.debug("Null host initially, found [" + host + "] on the actual fetch attempt.");
    }

    final long start = DateTime.nanoTime();
    SemanticQuery.Builder builder = SemanticQuery.newBuilder()
        // always use the absolute time here so the queries match.
        .setStart(Long.toString(config.startTimestamp().msEpoch()))
        .setEnd(Long.toString(config.endTimestamp().msEpoch()))
        .setMode(context.query().getMode())
        .setTimeZone(context.query().getTimezone())
        .setLogLevel(context.query().getLogLevel());

    // TEMP! Breakfix. Find the real solution.
    final boolean event_query = config.getTypes() != null && 
        config.getTypes().contains("events");
    if (event_query) {
      if (config.getPushDownNodes() != null && !config.getPushDownNodes().isEmpty()) {
        TimeSeriesDataSourceConfig.Builder source_builder =
            (Builder) config.toBuilder()
            .setPushDownNodes(null)
            .setSourceId(null) // TODO - we may want to make this configurable
            // TODO - flip flop shouldn't be required.
//            .setDataSourceId(config.getDataSourceId())
//            .setId(config.getDataSourceId())
            .setType("TimeSeriesDataSource");
        
        builder.addExecutionGraphNode(source_builder.build());
      } else {
        TimeSeriesDataSourceConfig.Builder source_builder =
            (Builder) config.toBuilder()
            .setPushDownNodes(null)
            .setSourceId(null) // TODO - we may want to make this configurable
            // TODO - flip flop shouldn't be required.
            .setType("TimeSeriesDataSource");
        
        builder.addExecutionGraphNode(source_builder.build());
      }
      if (config.getPushDownNodes() != null) {
        for (int i = 0; i < config.getPushDownNodes().size(); i++) {
          builder.addExecutionGraphNode(config.getPushDownNodes().get(i));
        }
      }
    } else {
      TimeSeriesDataSourceConfig.Builder source_builder =
          (Builder) config.toBuilder()
          .setPushDownNodes(null)
          .setSourceId(null) // TODO - we may want to make this configurable
          .setType("TimeSeriesDataSource");
      
      builder.addExecutionGraphNode(source_builder.build());
      for (QueryNodeConfig c : config.getPushDownNodes()) {
        if (c instanceof DownsampleConfig) {
          DownsampleConfig downsampleConfig = (DownsampleConfig) c;
          DownsampleConfig.Builder newBuilder = DownsampleConfig.newBuilder();
          DownsampleConfig.cloneBuilder(downsampleConfig, newBuilder);
          c = newBuilder.setStart(context.query().getStart())
              .setEnd(context.query().getEnd())
              .setId(c.getId()).build();
        }
        builder.addExecutionGraphNode(c);
      }
    }

    if (!Strings.isNullOrEmpty(config.getFilterId())) {
      builder.addFilter(DefaultNamedFilter.newBuilder()
          .setId(config.getFilterId())
          .setFilter(context.query().getFilter(config.getFilterId()))
          .build());
    }
    
    final HttpPost post = new HttpPost(host + endpoint);
    post.addHeader("Content-Type", "application/json");
    if (factory instanceof HttpQueryV3Factory) {
      final String user_header_key = context.tsdb().getConfig().getString(
          ((HttpQueryV3Factory) factory).getConfigKey(BaseHttpExecutorFactory.HEADER_USER_KEY));
      if (!Strings.isNullOrEmpty(user_header_key) && 
          context.queryContext().authState() != null) {
        post.addHeader(user_header_key, context.queryContext().authState().getUser());
      }
    }
    
    // may need to pass down a cookie.
    if (context.queryContext().authState() != null && 
        !Strings.isNullOrEmpty(context.queryContext().authState().getTokenType()) &&
        context.queryContext().authState().getTokenType().equalsIgnoreCase("cookie")) {
      post.addHeader("Cookie", new String(
          context.queryContext().authState().getToken(), Const.UTF8_CHARSET));
    }
    
    final String json;
    try {
      json = JSON.serializeToString(builder.build());
      post.setEntity(new StringEntity(json));
    } catch (UnsupportedEncodingException e) {
      try {
        final Exception ex = new RejectedExecutionException(
            "Failed to generate request", e);
        exception = ex;
        context.tsdb().getQueryThreadPool().submit(this);
        //sendUpstream(ex);
      } catch (IllegalStateException ex) {
        LOG.error("Unexpected state halting execution with a failed "
            + "conversion: " + this);
        exception = ex;
        context.tsdb().getQueryThreadPool().submit(this);
        //sendUpstream(ex);
      } catch (Exception ex) {
        LOG.error("Unexpected exception halting execution with a failed "
            + "conversion: " + this);
        exception = ex;
        context.tsdb().getQueryThreadPool().submit(this);
        //sendUpstream(ex);
      }
      return;
    }
    
    final StatsTimer[] timer = new StatsTimer[] { 
        pipelineContext().tsdb().getStatsCollector().startTimer(
            REMOTE_LATENCY_METRIC, ChronoUnit.MILLIS) };
    final String[] current_host = new String[] { HttpQueryV3Source.this.host };
    
    /** Does the fun bit of parsing the response and calling the deferred. */
    class ResponseCallback implements FutureCallback<HttpResponse> {
      int retries = 0;
      RemoteQueryExecutionException previous_ex = null;
      
      @Override
      public void cancelled() {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Http query was canceled: " + this);
        }
        try {
          final Exception e = new QueryExecutionCanceled(
              "Query was canceled: " + host + endpoint, 400, 0);
          exception = e;
          context.tsdb().getQueryThreadPool().submit(HttpQueryV3Source.this);
          //sendUpstream(e);
        } catch (Exception e) {
          LOG.warn("Exception thrown when calling deferred on cancel: " 
              + this, e);
        }
      }

      @Override
      public void completed(final HttpResponse response) {
        try {
          timer[0].stop("remote", current_host[0], "status", 
              Integer.toString(response.getStatusLine().getStatusCode()));

          final Header header = response.getFirstHeader("X-Served-By");
          String host = current_host[0];
          if (header != null && header.getValue() != null) {
            host = header.getValue();
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Response from endpoint: " + host + endpoint +" received: " 
                + response.getStatusLine());
          }

          String json = null;
          
          if (response.getStatusLine().getStatusCode() != 200) {
            try {
              DefaultSharedHttpClient.parseResponse(response, 0, host);
            } catch (Exception e) {
              if (e instanceof RemoteQueryExecutionException) {
                if (!((BaseHttpExecutorFactory) factory).retriable(
                    response.getStatusLine().getStatusCode())) {
                  result = BadQueryResult.newBuilder()
                          .setNode(HttpQueryV3Source.this)
                          .setException(e)
                          .setDataSource(pickId())
                          .build();
                  context.tsdb().getQueryThreadPool().submit(HttpQueryV3Source.this);
                  return;
                } else if (previous_ex != null && 
                    previous_ex.getStatusCode() == 
                      ((RemoteQueryExecutionException) e).getStatusCode() &&
                    previous_ex.getMessage().equals(
                        ((RemoteQueryExecutionException) e).getMessage())) {
                  // in this case we've tried up to two hosts and got the same
                  // error so it could be either a query issue or a node issue
                  // so we don't need to bother.
                  result = BadQueryResult.newBuilder()
                      .setNode(HttpQueryV3Source.this)
                      .setException(e)
                      .setDataSource(pickId())
                      .build();
                  context.tsdb().getQueryThreadPool().submit(HttpQueryV3Source.this);
                  return;
                }
                previous_ex = (RemoteQueryExecutionException) e;
              }
            }
            
            if (factory instanceof BaseHttpExecutorFactory) {
              ((BaseHttpExecutorFactory) factory).markHostAsBad(
                  HttpQueryV3Source.this.host, 
                  response.getStatusLine().getStatusCode());
              if (((BaseHttpExecutorFactory) factory).retries() > 0 && 
                  retries < ((BaseHttpExecutorFactory) factory).retries()) {
                retries++;
                try {
                  current_host[0] = ((BaseHttpExecutorFactory) factory).nextHost();
                  if (current_host[0] == null) {
                    context.queryContext().logWarn(HttpQueryV3Source.this,
                            "No valid remote endpoint found.");
                    sendUpstream(BadQueryResult.newBuilder()
                            .setNode(HttpQueryV3Source.this)
                            .setException(new QueryDownstreamException(
                                    "No valid remote endpoint found."))
                            .setDataSource(pickId())
                            .build());
                    return;
                  }

                  post.setURI(URI.create(current_host[0] + endpoint));
                  EntityUtils.consume(response.getEntity());
                  timer[0] = pipelineContext().tsdb().getStatsCollector().startTimer(
                      REMOTE_LATENCY_METRIC, ChronoUnit.MILLIS);
                  client.execute(post, this);
                  context.queryContext().logWarn(HttpQueryV3Source.this, 
                      "Retrying query to [" + current_host[0] + endpoint + "] after " 
                      + DateTime.msFromNanoDiff(DateTime.nanoTime(), start) + "ms");
                  if (LOG.isTraceEnabled()) {
                    LOG.trace("Retrying Http query to a TSD: " + current_host[0] + endpoint);
                  }
                  return;
                } catch (IllegalStateException e) {
                  // can't retry as we don't have any good hosts.
                  try {
                    DefaultSharedHttpClient.parseResponse(response, 0, host);
                  } catch (RemoteQueryExecutionException rqee) {
                    result = BadQueryResult.newBuilder()
                        .setNode(HttpQueryV3Source.this)
                        .setException(rqee)
                        .setDataSource(pickId())
                        .build();
                    context.tsdb().getQueryThreadPool().submit(HttpQueryV3Source.this);
                    return;
                  }
                }
              }
            }
            
            result = BadQueryResult.newBuilder()
                .setNode(HttpQueryV3Source.this)
                .setException(new QueryExecutionException("Unexpected exception: " 
                    + EntityUtils.toString(response.getEntity()), 
                    response.getStatusLine().getStatusCode()))
                .setDataSource(pickId())
                .build();
            context.tsdb().getQueryThreadPool().submit(HttpQueryV3Source.this);
            return;
          }
          
          json = DefaultSharedHttpClient.parseResponse(response, 0, host);
          if (LOG.isTraceEnabled()) {
            LOG.trace("Response from host [" + host + endpoint + "]\n" + json);
          }
          final JsonNode root = JSON.getMapper().readTree(json);
          JsonNode results = root.get("results");
          if (results == null) {
            // could be an error, parse it.
            LOG.error("No JSON results from: " + json);
            result = BadQueryResult.newBuilder()
                .setNode(HttpQueryV3Source.this)
                .setException(new QueryExecutionException(
                    "No JSON results from: " + json, 500))
                .setDataSource(pickId())
                .build();
            context.tsdb().getQueryThreadPool().submit(HttpQueryV3Source.this);
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Successful response from [" + host + endpoint + "] after " 
                    + DateTime.msFromNanoDiff(DateTime.nanoTime(), start) + "ms");
            }
            if (context.query().isDebugEnabled()) {
              context.queryContext().logDebug(HttpQueryV3Source.this, 
                  "Successful response from [" + host + endpoint + "] after " 
                  + DateTime.msFromNanoDiff(DateTime.nanoTime(), start) + "ms");
            }
            
            JsonNode n = root.get("log");
            if (n != null && !n.isNull()) {
              for (final JsonNode node : n) {
                final String line = node.asText();
                // TODO may be a better way
                if (line.contains(" ERROR ")) {
                  context.queryContext().logError(HttpQueryV3Source.this, line);
                } else if (line.contains(" WARN ")) {
                  context.queryContext().logWarn(HttpQueryV3Source.this, line);
                } else if (line.contains(" INFO ")) {
                  context.queryContext().logInfo(HttpQueryV3Source.this, line);
                } else if (line.contains(" DEBUG ")) {
                  context.queryContext().logDebug(HttpQueryV3Source.this, line);
                } else if (line.contains(" TRACE ")) {
                  context.queryContext().logTrace(HttpQueryV3Source.this, line);
                }
              }
            }
            if (context.queryContext().stats() != null) {
              context.queryContext().stats().incrementRawDataSize(json.length());
            }
            
            int sent = 0;
            for (final JsonNode result : results) {
              final HttpQueryV3Result series_result = new HttpQueryV3Result(
                  HttpQueryV3Source.this, result, 
                    ((TimeSeriesDataSourceFactory) factory).rollupConfig());
              if (context.queryContext().stats() != null) {
                context.queryContext().stats().incrementRawTimeSeriesCount(
                    series_result.timeSeries().size());
              }
              HttpQueryV3Source.this.result = series_result;
              context.tsdb().getQueryThreadPool().submit(HttpQueryV3Source.this);
              sent++;
            }
            if (sent == 0) {
              HttpQueryV3Source.this.result = new HttpQueryV3Result(
                      HttpQueryV3Source.this, null, null);
              context.tsdb().getQueryThreadPool().submit(HttpQueryV3Source.this);
            }
          }
        } catch (Throwable t) {
          String content = null;
          try {
            content = EntityUtils.toString(response.getEntity());
          } catch (Exception e1) {
            LOG.error("Failed to handle the error...", e1);
          }
          
          if (t instanceof QueryExecutionException) {
            try {
              context.queryContext().logError(HttpQueryV3Source.this, 
                  "Error sending query to [" + host + endpoint + "] after " 
                  + DateTime.msFromNanoDiff(DateTime.nanoTime(), start) + "ms: " 
                      + t.getMessage());
              if (context.query().isTraceEnabled()) {
                context.queryContext().logTrace(HttpQueryV3Source.this, 
                    "Original content: " + response + " => " + content);
              }
              HttpQueryV3Source.this.result = BadQueryResult.newBuilder()
                  .setNode(HttpQueryV3Source.this)
                  .setException(t)
                  .setDataSource(pickId())
                  .build();
              context.tsdb().getQueryThreadPool().submit(HttpQueryV3Source.this);
            } catch (Exception ex) {
              LOG.warn("Unexpected exception when handling exception: " 
                  + this, ex);
            }
          } else {
            try {
              final Exception ex = new QueryExecutionException(
                  "Unexepected exception: " + host + endpoint, 500, 0, t);
              context.queryContext().logError(HttpQueryV3Source.this, 
                  "Error sending query to [" + host + endpoint + "] after " 
                  + DateTime.msFromNanoDiff(DateTime.nanoTime(), start) + "ms: " 
                      + t.getMessage());
              if (context.query().isTraceEnabled()) {
                context.queryContext().logTrace(HttpQueryV3Source.this, 
                    "Original content: " + response + " => " + content);
              }
              HttpQueryV3Source.this.result = BadQueryResult.newBuilder()
                  .setNode(HttpQueryV3Source.this)
                  .setException(ex)
                  .setDataSource(pickId())
                  .build();
              context.tsdb().getQueryThreadPool().submit(HttpQueryV3Source.this);
            } catch (Exception ex) {
              LOG.warn("Unexpected exception when handling exception: " 
                  + this, ex);
            }
          }
        }
      }

      @Override
      public void failed(final Exception e) {
        try {
          timer[0].stop("remote", current_host[0], "status", "0");
          
          if (factory instanceof BaseHttpExecutorFactory) {
            ((BaseHttpExecutorFactory) factory).markHostAsBad(
                current_host[0], 0);
            if (((BaseHttpExecutorFactory) factory).retries() > 0 && 
                retries < ((BaseHttpExecutorFactory) factory).retries()) {
              retries++;
              current_host[0] = ((BaseHttpExecutorFactory) factory).nextHost();
              if (current_host[0] == null) {
                context.queryContext().logWarn(HttpQueryV3Source.this,
                        "No valid remote endpoint found.");
                sendUpstream(BadQueryResult.newBuilder()
                        .setNode(HttpQueryV3Source.this)
                        .setException(new QueryDownstreamException(
                                "No valid remote endpoint found."))
                        .setDataSource(pickId())
                        .build());
                return;
              }

              post.setURI(URI.create(current_host[0] + endpoint));
              timer[0] = pipelineContext().tsdb().getStatsCollector().startTimer(
                  REMOTE_LATENCY_METRIC, ChronoUnit.MILLIS);
              client.execute(post, this);
              context.queryContext().logWarn(HttpQueryV3Source.this, 
                  "Retrying query to [" + current_host[0] + endpoint + "] after " 
                  + DateTime.msFromNanoDiff(DateTime.nanoTime(), start) + "ms");
              if (LOG.isTraceEnabled()) {
                LOG.trace("Retrying Http query to a TSD: " + current_host[0] + endpoint);
              }
              return;
            }
          }
          
          LOG.error("Failed response from: [" + current_host[0] + endpoint + "]", e);
          context.queryContext().logError(HttpQueryV3Source.this, 
              "Error sending query to [" + host + endpoint + "] after " 
              + DateTime.msFromNanoDiff(DateTime.nanoTime(), start) + "ms: " 
                  + e.getMessage());
          HttpQueryV3Source.this.result = BadQueryResult.newBuilder()
              .setNode(HttpQueryV3Source.this)
              .setException(e)
              .setDataSource(pickId())
              .build();
          context.tsdb().getQueryThreadPool().submit(HttpQueryV3Source.this);
        } catch (Throwable t) {
          LOG.error("Unexpected exception processing query", t);
          sendUpstream(BadQueryResult.newBuilder()
              .setNode(HttpQueryV3Source.this)
              .setException(t)
              .setDataSource(pickId())
              .build());
        }
      }
    }
    
    client.execute(post, new ResponseCallback());
    if (context.query().isTraceEnabled()) {
      context.queryContext().logTrace(this, "Compiled and sent query to [" 
          + host + endpoint + "] in " 
          + DateTime.msFromNanoDiff(DateTime.nanoTime(), start) + "ms: " + json);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sent Http query to a TSD: " + host + endpoint + ": " + json);
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

  @Override
  public String[] setIntervals() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void run() {
    if (result != null) {
      sendUpstream(result);
    } else {
      sendUpstream(exception);
    }
  }

  protected QueryResultId pickId() {
    if (config.getPushDownNodes() != null && !config.getPushDownNodes().isEmpty()) {
      return (QueryResultId) config.getPushDownNodes()
              .get(config.getPushDownNodes().size() - 1).resultIds().get(0);
    }
    return config.resultIds().get(0);
  }
}
