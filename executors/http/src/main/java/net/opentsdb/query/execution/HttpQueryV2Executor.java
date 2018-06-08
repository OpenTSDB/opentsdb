// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.Map.Entry;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.entity.DeflateDecompressingEntity;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.stumbleupon.async.Callback;

import io.opentracing.Span;
import net.opentsdb.core.Const;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.iterators.DefaultIteratorGroup;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryExecutionCanceled;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.exceptions.RemoteQueryExecutionException;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.TSQuery;
import net.opentsdb.query.TSSubQuery;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.JSONException;

/**
 * An executor that converts {@link TimeSeriesQuery}s to OpenTSDB v2.x {@link TSQuery}s
 * and sends them over HTTP to a 2.x API via /api/query. 
 * <p>
 * Currently a new HTTP async client is instantiated per query (inefficient)
 * and it will pull headers from the query context session object that should
 * be forwarded to clients (e.g. Cookies, trace, etc).
 * 
 * @since 3.0
 */
public class HttpQueryV2Executor extends QueryExecutor<IteratorGroups> {
  private static final Logger LOG = LoggerFactory.getLogger(
      HttpQueryV2Executor.class);

  /** A key for the session object used by this executor. */
  public final static String SESSION_HEADERS_KEY = "tsdb_http_executor_headers";
  
  /** The default endpoint. */
  private final String default_endpoint;
  
  /**
   * Default Ctor
   * @param node The graph node this executor refers to.
   * @throws IllegalArgumentException if the query context or config were null.
   * @throws IllegalStateException if the remote context was not an instance
   * of HttpContext.
   */
  public HttpQueryV2Executor(final ExecutionGraphNode node) {
    super(node);
    default_endpoint = "";
//    if (((Config) node.getConfig()) == null) {
//      throw new IllegalArgumentException("Config connot be null.");
//    }
//    if (Strings.isNullOrEmpty(((Config) node.getConfig()).endpoint)) {
//      default_endpoint = null;
//    } else {
//      default_endpoint = ((Config) node.getConfig()).endpoint + "/api/query";
//    }
  }
  
  @Override
  public QueryExecution<IteratorGroups> executeQuery(
      final QueryContext context,
      final TimeSeriesQuery query,
      final Span upstream_span) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    
    final Execution exec = new Execution(context, query, upstream_span);
    outstanding_executions.add(exec);
    exec.execute();
    return exec;
  }
  
  /**
   * Parses the HTTP response making sure it's a 200 with a content body.
   * The body is returned as a string if it's successful. If the response code
   * is NOT 200 or the body is null then it throws an exception to pass upstream.
   * @param response A non-null response to parse.
   * @param order The order of the response.
   * @param host The host that sent the response.
   * @return a non-null string if the body contained content.
   * @throws RuntimeException if the response is not 200 or the body couldn't be
   * read into a string.
   */
  @VisibleForTesting
  String parseResponse(final HttpResponse response, final int order, 
      final String host) {
    final String content;
    if (response.getEntity() == null) {
      throw new RemoteQueryExecutionException("Content for http response "
          + "was null: " + response, host, order, 500);
    }
    
    try {
      final String encoding = (response.getEntity().getContentEncoding() != null &&
          response.getEntity().getContentEncoding().getValue() != null ?
              response.getEntity().getContentEncoding().getValue().toLowerCase() :
                "");
      if (encoding.equals("gzip") || encoding.equals("x-gzip")) {
        content = EntityUtils.toString(
            new GzipDecompressingEntity(response.getEntity()));
      } else if (encoding.equals("deflate")) {
        content = EntityUtils.toString(
            new DeflateDecompressingEntity(response.getEntity()));
      } else if (encoding.equals("")) {
        content = EntityUtils.toString(response.getEntity());
      } else {
        throw new RemoteQueryExecutionException("Unhandled content encoding [" 
            + encoding + "] : " + response, host, 500, order);
      }
    } catch (ParseException e) {
      LOG.error("Failed to parse content from HTTP response: " + response, e);
      throw new RemoteQueryExecutionException("Content parsing failure for: " 
          + response, host, 500, order, e);
    } catch (IOException e) {
      LOG.error("Failed to parse content from HTTP response: " + response, e);
      throw new RemoteQueryExecutionException("Content parsing failure for: " 
          + response, host, 500, order, e);
    }
  
    if (response.getStatusLine().getStatusCode() == 200) {
      return content;
    }
    // TODO - parse out the exception
    throw new RemoteQueryExecutionException(content, host, 
        response.getStatusLine().getStatusCode(), order);
  }
  
  /**
   * Parses a TSDB v2.x query response returned via HTTP. Note that this will
   * not parse the 2.3 expression query output, just the /api/query response.
   * @param query A non-null query to parse.
   * @param node A non-null JSON node set to the root of one of the response
   * objects in the response array.
   * @param tracer_span An optional span to write stats to.
   * @param groups The map of groups to write to (matches on metric).
   * @return A non-null data shards set.
   * @throws IllegalArgumentException if the query or node was null.
   */
  @VisibleForTesting
  void parseTSQuery(final TimeSeriesQuery query, 
                          final JsonNode node,
                          final Span tracer_span,
                          final Map<String, IteratorGroup> groups) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    final BaseTimeSeriesStringId.Builder id = 
        BaseTimeSeriesStringId.newBuilder();
    
    final String metric;
    if (node.has("metric")) {
      if (node.path("metric").isNull()) {
        throw new JSONException("Metric was null for query result: " + node);
      }
      metric = node.path("metric").asText();
      id.setMetric(metric);
    } else {
      throw new JSONException("No metric found for the series: " + node);
    }
    IteratorGroup group = null;
    for (final Metric m : query.getMetrics()) {
      if (m.getMetric().equals(metric)) {
        group = groups.get(m.getId());
        break;
      }
    }
    if (group == null) {
      throw new IllegalStateException("Couldn't find a group for metric: " 
          + metric);
    }
    
    parseTags(id, node.path("tags"));
    parseAggTags(id, node.path("aggregateTags"));
    
    final NumericMillisecondShard shard = new NumericMillisecondShard(id.build(), 
        query.getTime().startTime(), query.getTime().endTime());
    
    int nans = 0;
    int values = 0;
    final JsonNode dps = node.path("dps");
    if (dps != null) {
      final Iterator<Entry<String, JsonNode>> it = dps.fields();
      while (it.hasNext()) {
        final Entry<String, JsonNode> value = it.next();
        if (value.getKey() == null || value.getValue().isNull()) {
          throw new JSONException("Null in the data points map: " + value);
        }
        // Jackson's canConvertToLong() doesn't work well.
        final String v = value.getValue().asText();
        if (v.startsWith("N") || v.startsWith("n")) {
          // nulls and NaNs.
          shard.add(Long.parseLong(value.getKey()), Double.NaN);
        } else if (NumericType.looksLikeInteger(v)) {
          shard.add(Long.parseLong(value.getKey()), NumericType.parseLong(v));
        } else {
          final double parsed = Double.parseDouble(v);
          shard.add(Long.parseLong(value.getKey()), parsed);
          if (!Double.isFinite(parsed)) {
            ++nans;
          }
        }
        ++values;
        // TODO ^^ just storing a 1 for now since we don't know from TSDB v2's
        // API which values are real or not.
      }
    } else {
      throw new JSONException("No data points found for the series: " 
          + node);
    }
    
    if (tracer_span != null) {
      tracer_span.setTag("nonFiniteValues", nans);
      tracer_span.setTag("totalValues", values);
    }
    
    //group.addIterator(shard);
  }
  
  /**
   * Parses out the tag pairs from a JSON response.
   * @param id The ID to populate.
   * @param tags A pointer to the tags node in the JSON tree.
   * @throws JSONException if any key or value in the tag map is null.
   */
  private void parseTags(final BaseTimeSeriesStringId.Builder id, 
      final JsonNode tags) {
    if (tags != null) {
      final Iterator<Entry<String, JsonNode>> pairs = tags.fields();
      while (pairs.hasNext()) {
        final Entry<String, JsonNode> pair = pairs.next();
        if (pair.getKey() == null || pair.getValue() == null || 
            pair.getValue().isNull()) {
          throw new JSONException("Tag pair had a null key or value: " + pair);
        }
        id.addTags(pair.getKey(), pair.getValue().asText());
      }
    }
  }
  
  /**
   * Parses out the aggregated tag keys from a JSON response.
   * @param id The ID to populate.
   * @param agg_tags A pointer to the aggregated tags node in the JSON tree.
   * @throws JSONException if any of the tags are null.
   */
  private void parseAggTags(final BaseTimeSeriesStringId.Builder id, 
      final JsonNode agg_tags) {
    if (agg_tags != null) {
      for (final JsonNode tag : agg_tags) {
        if (tag == null || tag.isNull()) {
          throw new JSONException("Agg tags had a null value: " + agg_tags);
        }
        id.addAggregatedTag(tag.asText());
      }
    }
  }

  /** An implementation that allows for canceling the future. */
  class Execution extends QueryExecution<IteratorGroups> {
    final QueryContext context;
    
    /** The client used for communications. */
    private final CloseableHttpAsyncClient client;
    
    /** The Future returned by the client so we can cancel it if we need to. */
    private Future<HttpResponse> future;
    
    /** The endpoint to hit. */
    private final String endpoint;
    
    public Execution(final QueryContext context, 
        final TimeSeriesQuery query, final Span upstream_span) {
      super(query);
      this.context = context;
      
      // TODO - tune this sucker and share a bit.
      client = HttpAsyncClients.custom()
          .setDefaultIOReactorConfig(IOReactorConfig.custom()
              .setIoThreadCount(1).build())
          .setMaxConnTotal(1)
          .setMaxConnPerRoute(1)
          .build();
      client.start();
      
      deferred.addCallback(new FutureRemover(future))
              .addErrback(new FutureExceptionRemover(future));
      if (context.getTracer() != null) {
        setSpan(context, 
            HttpQueryV2Executor.this.getClass().getSimpleName(), 
            upstream_span,
            TsdbTrace.addTags(
                "order", Integer.toString(query.getOrder()),
                "query", JSON.serializeToString(query),
                "startThread", Thread.currentThread().getName()));
      }

      final Config override = null; 
          //(Config) context.getConfigOverride(node.getId());
      if (override != null && !Strings.isNullOrEmpty(override.endpoint)) {
        endpoint = override.endpoint + "/api/query";
      } else {
        endpoint = default_endpoint;
      }
      if (Strings.isNullOrEmpty(endpoint)) {
        throw new IllegalStateException("No endpoint was provided via default "
            + "or override for: " + this);
      }
    }
    
    @SuppressWarnings("unchecked")
    public void execute() {
      final String json;
      try {
      json = JSON.serializeToString(convertQuery(query));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Sending JSON to http endpoint: " + json);
        }
      } catch (Exception e) {
        try {
          final Exception ex = new RejectedExecutionException(
              "Failed to convert query", e);
          callback(ex, TsdbTrace.exceptionTags(ex));
        } catch (IllegalStateException ex) {
           LOG.error("Unexpected state halting execution with a failed "
               + "conversion: " + this);
        } catch (Exception ex) {
          LOG.error("Unexpected exception halting execution with a failed "
              + "conversion: " + this);
        }
        return;
      }
      
      final HttpPost post = new HttpPost(endpoint);
      final Object session_headers = context.getSessionObject(
          SESSION_HEADERS_KEY);
      if (session_headers != null) {
        try {
          for (final Entry<String, String> header :
              ((Map<String, String>) session_headers).entrySet()) {
            post.setHeader(header.getKey(), header.getValue());
          }
        } catch (ClassCastException ex) {
          final Exception ise = new IllegalStateException(
            "Session headers were not a map: " + session_headers.getClass(),
            ex);
          try {
            callback(ise, TsdbTrace.exceptionTags(ise));
          } catch (final IllegalStateException ignored) {
            LOG.error("Unexpected state halting execution with mistyped "
                + "session headers: " + this);
          } catch (final Exception ignored) {
            LOG.error("Unexpected exception halting execution with mistyped "
                + "session headers: " + this);
          }
          return;
        }
      }
      try {
        post.setEntity(new StringEntity(json));
      } catch (UnsupportedEncodingException e) {
        try {
          final Exception ex = new RejectedExecutionException(
              "Failed to generate request", e);
          callback(ex, TsdbTrace.exceptionTags(ex));
        } catch (IllegalStateException ex) {
            LOG.error("Unexpected state halting execution with a failed "
                + "conversion: " + this);
         } catch (Exception ex) {
           LOG.error("Unexpected exception halting execution with a failed "
               + "conversion: " + this);
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
          if (completed.get()) {
            return;
          }
          try {
            final Exception e = new QueryExecutionCanceled(
                "Query was canceled: " + endpoint, 400, query.getOrder()); 
            callback(e,
                TsdbTrace.canceledTags(e));
          } catch (IllegalStateException e) {
            // already called, ignore it.
          } catch (Exception e) {
            LOG.warn("Exception thrown when calling deferred on cancel: " 
                + this, e);
          }
        }

        @Override
        public void completed(final HttpResponse response) {
          String host = "unknown";
          for (final Header header : response.getAllHeaders()) {
            if (header.getName().equals("X-Served-By")) {
              host = header.getValue();
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Response from endpoint: " + endpoint + " (" + host 
                + ") received");
          }
          if (completed.get()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Successfully received response [" + response 
                  + "] but execution was already marked complete. " + this);
            }
            try {
              EntityUtils.consume(response.getEntity());
            } catch (Exception e) {
              LOG.error("Unexpected exception consuming response: " + this, e);
            }
            return;
          }
          try {
            final String json = parseResponse(response, query.getOrder(), host);
            final JsonNode root = JSON.getMapper().readTree(json);
            final Map<String, IteratorGroup> groups = 
                Maps.newHashMapWithExpectedSize(query.getMetrics().size());
            for (final Metric metric : query.getMetrics()) {
              groups.put(metric.getId(), new DefaultIteratorGroup(
                  new SimpleStringGroupId(metric.getId())));
            }
            for (final JsonNode node : root) {
              parseTSQuery(query, node, tracer_span, groups);
            }
            final IteratorGroups results = new DefaultIteratorGroups();
            for (final IteratorGroup group : groups.values()) {
              results.addGroup(group);
            }
            callback(results, 
                TsdbTrace.successfulTags("remoteHost", host));
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
                callback(e,
                    TsdbTrace.exceptionTags(e, "remoteHost", host),
                    TsdbTrace.exceptionAnnotation(e));
              } catch (IllegalStateException caught) {
                // ignore;
              } catch (Exception ex) {
                LOG.warn("Unexpected exception when handling exception: " 
                    + this, e);
              }
            } else {
              try {
                final Exception ex = new QueryExecutionException(
                    "Unexepected exception: " + endpoint, 500, query.getOrder(), e);
                callback(ex,
                    TsdbTrace.exceptionTags(ex, "remoteHost", host),
                    TsdbTrace.exceptionAnnotation(ex));
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
          if (!completed.get()) {
            LOG.error("Exception from HttpPost: " + endpoint, e);
            final Exception ex = new QueryExecutionException(
                "Unexepected exception: " + endpoint, 500, query.getOrder(), e);
            try {
              callback(ex,
                  TsdbTrace.exceptionTags(ex),
                  TsdbTrace.exceptionAnnotation(ex));
            } catch (IllegalStateException caught) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Callback was already called but we recieved "
                    + "an exception: " + this, e);
              }
            } catch (Exception caught) {
              LOG.error("Unexpected exception calling back failed query", caught);
            }
          }
        }
      }
      
      future = client.execute(post, new ResponseCallback());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sent Http query to a TSD: " + this);
      }
    }
    
    @Override
    public void cancel() {
      try {
        // race condition here.
        final Exception e = new QueryExecutionCanceled(
            "Query was cancelled upstream.", 400, query.getOrder());
        callback(e,
            TsdbTrace.canceledTags(e));
      } catch (IllegalStateException caught) {
        // ignore if already called
      } catch (Exception e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Callback may have already been called", e);
        }
      }
      synchronized (this) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Canceling query: " + this);
        }
        if (future != null) {
          try {
            future.cancel(true);
          } catch (Exception e) {
            LOG.error("Failed cancelling future: " + future 
                + " on execution: " + this, e);
          } finally {
            future = null;
          }
        }
      }
    }
    
    /** Helper to remove the future once it's complete but had an exception. */
    class FutureExceptionRemover implements Callback<Object, Exception> {
      final Future<HttpResponse> future;
      public FutureExceptionRemover(final Future<HttpResponse> future) {
        this.future = future;
      }
      @Override
      public Object call(final Exception e) throws Exception {
        cleanup();
        throw e;
      }
    }
    
    /** Helper to remove the future once it's complete. */
    class FutureRemover implements Callback<Object, IteratorGroups> {
      final Future<HttpResponse> future;
      public FutureRemover(final Future<HttpResponse> future) {
        this.future = future;
      }
      @Override
      public Object call(final IteratorGroups results) throws Exception {
        cleanup();
        return results;
      }
    }
    
    /** Helper to close the client and remove this from the outstanding executions. */
    void cleanup() {
      outstanding_executions.remove(this);
      try {
        /** For some reason, closing the async client can take over a second!.
         * Since we don't really care *when* it's closed, we'll just give it to
         * a cleanup pool to get rid of.
         * TODO - this is suboptimal. If we have a threadpool based async client
         * then we can avoid this alltogether.  */
        class ClientCloser implements Runnable {
          @Override
          public void run() {
            try {
              client.close();
            } catch (IOException e) {
              LOG.error("Exception while closing the HTTP client", e);
            }
          }
        }
        context.getTSDB()
          .getRegistry().cleanupPool().execute(new ClientCloser());
      } catch (Exception ex) {
        LOG.error("Exception while scheduling the client for closing.", ex);
      }
    }
  
    @Override
    public String toString() {
      final StringBuilder buf = new StringBuilder()
          .append("query=")
          .append(query.toString())
          .append(", completed=")
          .append(completed.get())
          .append(", tracerSpan=")
          .append(tracer_span)
          .append(", endpoint=")
          .append(endpoint)
          .append(", context=")
          .append(context)
          .append(", executor=")
          .append(HttpQueryV2Executor.this);
      return buf.toString();
    }
  }
  
  /**
   * Converts the time series query into a {@link TSQuery}. Note that since 
   * TSQueries only dealt with metrics and basic aggregation, this method will
   * drop expressions and outputs. Only filters and metrics are passed through.
   * @param query The source query.
   * @return A validated time series query.
   * @throws IllegalArgumentException if one or more of the query parameters
   * could not compile properly into a {@link TSQuery}.
   */
  public static TSQuery convertQuery(final TimeSeriesQuery query) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    if (query.getMetrics() == null || query.getMetrics().size() < 1) {
      throw new IllegalArgumentException("Query must contain at least "
          + "one metric.");
    }
    final TSQuery ts_query = new TSQuery();
    ts_query.setMsResolution(true); // always get times in ms now
    
    ts_query.setStart(query.getTime().getStart());
    ts_query.setEnd(query.getTime().getEnd());
    
    ts_query.setTimezone(query.getTime().getTimezone());
    
    final List<TSSubQuery> subs = new ArrayList<TSSubQuery>(
        query.getMetrics().size());
    
    for (final Metric metric : query.getMetrics()) {
      final TSSubQuery sub = new TSSubQuery();
      sub.setMetric(metric.getMetric());
      sub.setAggregator(metric.getAggregator() == null || 
          metric.getAggregator().isEmpty() ? 
          query.getTime().getAggregator() : metric.getAggregator());
      
      if (!Strings.isNullOrEmpty(metric.getFilter())) {
        boolean matched = false;
        if (query.getFilters() == null) {
          if (!matched) {
            throw new IllegalArgumentException("Source query was missing filter : " 
                + metric.getFilter());
          }
        }
        for (final Filter set : query.getFilters()) {
          if (set.getId().equals(metric.getFilter())) {
            final List<TagVFilter> filters = 
                new ArrayList<TagVFilter>(set.getTags().size());
            for (final TagVFilter filter : set.getTags()) {
              filters.add(TagVFilter.newBuilder()
                .setFilter(filter.getFilter())
                .setType(filter.getType())
                .setTagk(filter.getTagk())
                .setGroupBy(filter.isGroupBy())
                .build());
            }
            sub.setFilters(filters);
            sub.setExplicitTags(set.getExplicitTags());
            matched = true;
            break;
          }
        }
        if (!matched) {
          throw new IllegalArgumentException("Source query was missing filter : " 
              + metric.getFilter());
        }
      }
      
      if (metric.getDownsampler() != null) {
        sub.setDownsample(metric.getDownsampler().getInterval() + "-" + 
            metric.getDownsampler().getAggregator() +
            (metric.getDownsampler().getFillPolicy() != null ?
                "-" + metric.getDownsampler().getFillPolicy().getPolicy().getName() : ""));
      } else if (query.getTime().getDownsampler() != null) {
        sub.setDownsample(query.getTime().getDownsampler().getInterval() + "-" + 
            query.getTime().getDownsampler().getAggregator() +
            (query.getTime().getDownsampler().getFillPolicy() != null ?
                "-" + query.getTime().getDownsampler().getFillPolicy().getPolicy().getName() : ""));
      }
      
      // TODO - may need Guava's Optional here. Otherwise timespan makes everyone
      // a rate.
      if (metric.isRate() || query.getTime().isRate()) {
        sub.setRate(true);
      }
      if (metric.getRateOptions() != null) {
        sub.setRateOptions(metric.getRateOptions());
      } else if (query.getTime().getRateOptions() != null) {
        sub.setRateOptions(query.getTime().getRateOptions());
      }
      
      subs.add(sub);
    }
  
    ts_query.setQueries(subs);
    ts_query.validateAndSetQuery();
    return ts_query;
  }
  
  /**
   * The config for this executor.
   */
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonDeserialize(builder = Config.Builder.class)
  public static class Config extends BaseQueryNodeConfig {
    private String endpoint;
    
    /**
     * Default ctor.
     * @param builder A non-null builder.
     */
    private Config(final Builder builder) {
      super(builder);
      endpoint = builder.endpoint;
    }
    
    /** @return The endpoint to use. */
    public String getEndpoint() {
      return endpoint;
    }
    
    @Override
    public String toString() {
      return new StringBuilder()
          .append("id=")
          .append(id)
          .append(", endpoint=")
          .append(endpoint)
          .toString();
    }
    
    /** @return A new builder. */
    public static Builder newBuilder() {
      return new Builder();
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Config config = (Config) o;
      return Objects.equal(id, config.id)
          && Objects.equal(endpoint, config.endpoint);
    }

    @Override
    public int hashCode() {
      return buildHashCode().asInt();
    }

    @Override
    public HashCode buildHashCode() {
      return Const.HASH_FUNCTION().newHasher()
          .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
          .putString(Strings.nullToEmpty(endpoint), Const.UTF8_CHARSET)
          .hash();
    }

    @Override
    public int compareTo(QueryNodeConfig config) {
      return ComparisonChain.start()
          .compare(id, config.getId(), 
              Ordering.natural().nullsFirst())
          .compare(endpoint, ((Config) config).endpoint, 
              Ordering.natural().nullsFirst())
          .result();
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Builder extends BaseQueryNodeConfig.Builder {
      @JsonProperty
      private String endpoint;
      
      /**
       * Sets the endpoint for the executor.
       * @param endpoint A non-null HTTP endpoint.
       * @return The builder.
       */
      public Builder setEndpoint(final String endpoint) {
        this.endpoint = endpoint;
        return this;
      }
      
      /** @return An instantiated configuration. */
      public Config build() {
        return new Config(this);
      }
    }

    @Override
    public String getId() {
      // TODO Auto-generated method stub
      return null;
    }

  }
}
