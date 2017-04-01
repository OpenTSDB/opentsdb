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
package net.opentsdb.query.execution;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.stumbleupon.async.Callback;

import io.opentracing.Span;
import net.opentsdb.data.DataShards;
import net.opentsdb.data.DataShardsGroup;
import net.opentsdb.data.DefaultDataShards;
import net.opentsdb.data.DefaultDataShardsGroup;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.RemoteQueryExecutionException;
import net.opentsdb.query.TSQuery;
import net.opentsdb.query.TSSubQuery;
import net.opentsdb.query.context.HttpContext;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.JSONException;

/**
 * An executor that converts {@link TimeSeriesQuery}s to OpenTSDB v2.x {@link TSQuery}s
 * and sends them over HTTP to a 2.x API via /api/query. The client is fetched
 * from an {@link HttpContext} as are any headers that must be forwarded 
 * downstream.
 * 
 * @since 3.0
 */
public class HttpQueryV2Executor extends QueryExecutor<DataShardsGroup> {
  private static final Logger LOG = LoggerFactory.getLogger(
      HttpQueryV2Executor.class);
  
  /** The root host and port to send data to. E.g. http://localhost:4242. */
  private final String endpoint;
  
  /** A non-null HTTP context to use for fetching a client and headers. */
  private final HttpContext http_context;

  /**
   * Default Ctor
   * @param context A non-null query context.
   * @param endpoint A non-null endpoint such as "http://localhost:4242". The
   * ctor will append "/api/query".
   * @throws IllegalArgumentException if the endpoint was null/empty, the 
   * remote context was null or the group ID was null.
   * @throws IllegalStateException if the remote context was not an instance
   * of HttpContext.
   */
  public HttpQueryV2Executor(final QueryContext context, final String endpoint) {
    super(context);
    if (Strings.isNullOrEmpty(endpoint)) {
      throw new IllegalArgumentException("Endpoint cannot be null or empty.");
    }
    if (context.getRemoteContext() == null) {
      throw new IllegalArgumentException("Remote context cannot be null.");
    }
    if (!(context.getRemoteContext() instanceof HttpContext)) {
      throw new IllegalStateException("Remote context was not an HttpContext.");
    }
    this.endpoint = endpoint + "/api/query";
    http_context = (HttpContext) context.getRemoteContext();
  }
  
  @Override
  public QueryExecution<DataShardsGroup> executeQuery(final TimeSeriesQuery query,
                                                      final Span upstream_span) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    if (query.groupId() == null) {
      throw new IllegalArgumentException("GroupID was not set in the Query.");
    }
    
    final Execution exec = new Execution(query, upstream_span);
    outstanding_executions.add(exec);
    exec.execute();
    return exec;
  }
  
  /**
   * Parses the HTTP response making sure it's a 200 with a content body.
   * The body is returned as a string if it's successful. If the response code
   * is NOT 200 or the body is null then it throws an exception to pass upstream.
   * @param response A non-null response to parse.
   * @return a non-null string if the body contained content.
   * @throws RuntimeException if the response is not 200 or the body couldn't be
   * read into a string.
   */
  @VisibleForTesting
  String parseResponse(final HttpResponse response, final int order) {
    final String content;
    if (response.getEntity() == null) {
      throw new RemoteQueryExecutionException("Content for http response "
          + "was null: " + response, order, 500);
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
            + encoding + "] : " + response, order, 500);
      }
    } catch (ParseException e) {
      LOG.error("Failed to parse content from HTTP response: " + response, e);
      throw new RemoteQueryExecutionException("Content parsing failure for: " 
          + response, order, 500, e);
    } catch (IOException e) {
      LOG.error("Failed to parse content from HTTP response: " + response, e);
      throw new RemoteQueryExecutionException("Content parsing failure for: " 
          + response, order, 500, e);
    }
  
    if (response.getStatusLine().getStatusCode() == 200) {
      return content;
    }
    // TODO - parse out the exception
    throw new RemoteQueryExecutionException(content, order, 
        response.getStatusLine().getStatusCode());
  }
  
  /**
   * Parses a TSDB v2.x query response returned via HTTP. Note that this will
   * not parse the 2.3 expression query output, just the /api/query response.
   * @param query A non-null query to parse.
   * @param node A non-null JSON node set to the root of one of the response
   * objects in the response array.
   * @return A non-null data shards set.
   * @throws IllegalArgumentException if the query or node was null.
   */
  @VisibleForTesting
  DataShards parseTSQuery(final TimeSeriesQuery query, final JsonNode node) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    final SimpleStringTimeSeriesId.Builder id = 
        SimpleStringTimeSeriesId.newBuilder();
    
    if (node.has("metric")) {
      if (node.path("metric").isNull()) {
        throw new JSONException("Metric was null for query result: " + node);
      }
      id.addMetric(node.path("metric").asText());
    } else {
      throw new JSONException("No metric found for the series: " + node);
    }
    
    parseTags(id, node.path("tags"));
    parseAggTags(id, node.path("aggregateTags"));
    
    final NumericMillisecondShard shard = new NumericMillisecondShard(id.build(), 
        query.getTime().startTime(), query.getTime().endTime());
    
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
        if (NumericType.looksLikeInteger(v)) {
          shard.add(Long.parseLong(value.getKey()), NumericType.parseLong(v), 1);
        } else {
          shard.add(Long.parseLong(value.getKey()), Double.parseDouble(v), 1);
        }
        
        // TODO ^^ just storing a 1 for now since we don't know from TSDB v2's
        // API which values are real or not.
      }
    } else {
      throw new JSONException("No data points found for the series: " 
          + node);
    }
    
    final DataShards shards = new DefaultDataShards(id.build());
    shards.addShard(shard);
    return shards;
  }
  
  /**
   * Parses out the tag pairs from a JSON response.
   * @param id The ID to populate.
   * @param tags A pointer to the tags node in the JSON tree.
   * @throws JSONException if any key or value in the tag map is null.
   */
  private void parseTags(final SimpleStringTimeSeriesId.Builder id, 
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
  private void parseAggTags(final SimpleStringTimeSeriesId.Builder id, 
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
  class Execution extends QueryExecution<DataShardsGroup> {
    /** The client used for communications. */
    private final CloseableHttpAsyncClient client;
    
    /** The Future returned by the client so we can cancel it if we need to. */
    private Future<HttpResponse> future;
    
    public Execution(final TimeSeriesQuery query, final Span upstream_span) {
      super(query);
      client = http_context.getClient();
      deferred.addCallback(new FutureRemover(future))
              .addErrback(new FutureExceptionRemover(future));
      if (context.getTracer() != null) {
        setSpan(context, HttpQueryV2Executor.this.getClass().getSimpleName(), 
            upstream_span,
            new ImmutableMap.Builder<String, String>()
              .put("order", Integer.toString(query.getOrder()))
              .put("query", JSON.serializeToString(query))
              .put("startThread", Thread.currentThread().getName())
              .build());
      }
    }
    
    public void execute() {
      final String json;
      try {
      json = JSON.serializeToString(convertQuery(query));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Sending JSON to http endpoint: " + json);
        }
      } catch (Exception e) {
        callback(new RejectedExecutionException("Failed to convert query", e));
        return;
      }
      
      final HttpPost post = new HttpPost(endpoint);
      if (http_context.getHeaders() != null) {
        for (final Entry<String, String> header : 
            http_context.getHeaders().entrySet()) {
          post.setHeader(header.getKey(), header.getValue());
        }
      }
      try {
        post.setEntity(new StringEntity(json));
      } catch (UnsupportedEncodingException e) {
        callback(new RejectedExecutionException("Failed to generate request", e));
        return;
      }
      
      /** Does the fun bit of parsing the response and calling the deferred. */
      class ResponseCallback implements FutureCallback<HttpResponse> {      
        @Override
        public void cancelled() {
          LOG.error("HttpPost cancelled: " + query);
          if (completed.get()) {
            LOG.warn("Received HTTP response despite being cancelled.");
          }
          try {
            callback(new RemoteQueryExecutionException(
                "Query was cancelled: " + endpoint, query.getOrder(), 500),
                new ImmutableMap.Builder<String, String>()
                  .put("status", "Cancelled")
                  .put("finalThread", Thread.currentThread().getName())
                  .build());
          } catch (Exception e) {
            LOG.warn("Exception thrown when calling deferred on cancel", e);
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
            LOG.warn("Told to stop running but we had a response: " + response);
            try {
              EntityUtils.consume(response.getEntity());
            } catch (Exception e) {
              LOG.error("Error consuming response", e);
            }
          }
          try {
            final String json = parseResponse(response, query.getOrder());
            final JsonNode root = JSON.getMapper().readTree(json);
            final DataShardsGroup group = new DefaultDataShardsGroup(query.groupId());
            for (final JsonNode node : root) {
              group.addShards(parseTSQuery(query, node));
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("Calling back upstream.");
            }
            callback(group, new ImmutableMap.Builder<String, String>()
                .put("remoteHost", host)
                .put("status", "ok")
                .put("finalThread", Thread.currentThread().getName())
                .build());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Called back upstream.");
            }
          } catch (Exception e) {
            LOG.error("Failure handling response: " + response + "\nQuery: " 
                + json, e);
            callback(new RemoteQueryExecutionException(
                "Unexepected exception: " + endpoint, query.getOrder(), 500, e));
          }
        }

        @Override
        public void failed(final Exception e) {
          // TODO possibly retry?
          if (!completed.get()) {
            LOG.error("Exception from HttpPost: " + endpoint, e);
            callback(new RemoteQueryExecutionException(
                "Unexepected exception: " + endpoint, query.getOrder(), 500, e),
                new ImmutableMap.Builder<String, String>()
                  .put("status", "Error")
                  .put("error", e.getMessage())
                  .put("finalThread", Thread.currentThread().getName())
                  .build());
          }
        }
      }
      
      future = client.execute(post, new ResponseCallback());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sent query to endpoint: " + endpoint);
      }
    }
    
    @Override
    public void cancel() {
      synchronized (this) {
        if (future != null) {
          try {
            future.cancel(true);
          } catch (Exception e) {
            LOG.error("Failed cancelling future: " + future, e);
          } finally {
            future = null;
          }
        }
      }
        try {
          // race condition here.
          callback(new RemoteQueryExecutionException(
              "Query was cancelled upstream.", query.getOrder()));
        } catch (Exception e) {
          LOG.error("Callback may have already been called", e);
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
    class FutureRemover implements Callback<Object, DataShardsGroup> {
      final Future<HttpResponse> future;
      public FutureRemover(final Future<HttpResponse> future) {
        this.future = future;
      }
      @Override
      public Object call(final DataShardsGroup result) throws Exception {
        cleanup();
        return result;
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
      
      if (query.getTime().isRate()) {
        sub.setRate(true);
      }
      if (query.getTime().getRateOptions() != null) {
        sub.setRateOptions(query.getTime().getRateOptions());
      }
      
      subs.add(sub);
    }
  
    ts_query.setQueries(subs);
    ts_query.validateAndSetQuery();
    return ts_query;
  }
}
