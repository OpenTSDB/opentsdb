// This file is part of OpenTSDB.
// Copyright (C) 2018 The OpenTSDB Authors.
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
package net.opentsdb.servlet.resources;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import net.opentsdb.auth.AuthState;
import net.opentsdb.auth.Authentication;
import net.opentsdb.auth.AuthState.AuthStatus;
import net.opentsdb.core.TSDB;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.query.execution.serdes.JsonV2QuerySerdesOptions;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.servlet.exceptions.GenericExceptionMapper;
import net.opentsdb.servlet.filter.AuthFilter;
import net.opentsdb.servlet.resources.QueryRpc.RunTSDQuery;
import net.opentsdb.servlet.sinks.ServletSinkConfig;
import net.opentsdb.servlet.sinks.ServletSinkFactory;
import net.opentsdb.stats.DefaultQueryStats;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.StatsCollector.StatsTimer;
import net.opentsdb.threadpools.TSDTask;
import net.opentsdb.stats.Trace;
import net.opentsdb.stats.Tracer;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.YAML;

@Path("query/graph")
public class RawQueryRpc {
  private static final Logger LOG = LoggerFactory.getLogger(RawQueryRpc.class);
  
  /** Request key used for the V3 TSDB query. */
  public static final String QUERY_KEY = "TSDQUERY";
  
  /** Request key for the query context. */
  public static final String CONTEXT_KEY = "CONTEXT";
  
  /** Request key for the tracer. */
  public static final String TRACE_KEY = "TRACE";
  
  class RunTSDQuery implements Runnable {

    final Span query_span;
    final AsyncContext async;
    final SemanticQuery query;

    final SemanticQueryContext context;

    public RunTSDQuery(Span query_span, AsyncContext async, SemanticQuery query,
        SemanticQueryContext context) {
      this.query_span = query_span;
      this.async = async;
      this.query = query;
      this.context = context;
    }

    @Override
    public void run() {
      asyncRun(query_span, async, query, context);
    }
  }
  
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response post(final @Context ServletConfig servlet_config, 
                       final @Context HttpServletRequest request/*,
                       final @Context HttpServletResponse response*/) throws Exception {
    final Object stream = request.getAttribute("DATA");
    if (stream != null) {
      return Response.ok()
          .entity(((ByteArrayOutputStream) stream).toByteArray())
          .header("Content-Type", "application/json")
          .build();
    }
    
    return handleQuery(servlet_config, request);
  }
  
  private Response handleQuery(final ServletConfig servlet_config, final HttpServletRequest request)
      throws IOException, InterruptedException, ExecutionException {
    Object obj = servlet_config.getServletContext()
        .getAttribute(OpenTSDBApplication.TSD_ATTRIBUTE);
    if (obj == null) {
      throw new WebApplicationException("Unable to pull TSDB instance from "
          + "servlet context.",
          Response.Status.INTERNAL_SERVER_ERROR);
    } else if (!(obj instanceof TSDB)) {
      throw new WebApplicationException("Object stored for as the TSDB was "
          + "of the wrong type: " + obj.getClass(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    final TSDB tsdb = (TSDB) obj;
    
    final StatsTimer timer;
    if (tsdb.getStatsCollector() != null) {
      tsdb.getStatsCollector().incrementCounter("query.new", "endpoint", "3x");
      timer = tsdb.getStatsCollector().startTimer("query.user.latency", ChronoUnit.MILLIS);
    } else {
      timer = null;
    }
    
    // check auth. 
    final AuthState auth_state;
    if (tsdb.getConfig().getBoolean(Authentication.AUTH_ENABLED_KEY)) {
      if (request.getAttribute(AuthFilter.AUTH_STATE_KEY) == null || 
          ((AuthState) request.getAttribute(AuthFilter.AUTH_STATE_KEY))
            .getStatus() != AuthStatus.SUCCESS) {
        throw new QueryExecutionException("Autentication failed.", 403);
      }
      auth_state = (AuthState) request.getAttribute(AuthFilter.AUTH_STATE_KEY);
    } else {
      auth_state = null; // TODO - add an "unknown" auth user.
    }
    
    // initiate the tracer
    final Trace trace;
    final Span query_span;
    final Tracer tracer = (Tracer) tsdb.getRegistry().getDefaultPlugin(Tracer.class);
    if (tracer != null) {
      trace = tracer.newTrace(true, true);
      query_span = trace.newSpanWithThread(this.getClass().getSimpleName())
          .withTag("endpoint", "/api/query/graph")
          .withTag("user", auth_state != null ? auth_state.getUser() : "Unkown")
          // TODO - more useful info
          .start();
    } else {
      trace = null;
      query_span = null;
    }
    Span parse_span = null;
    if (query_span != null) {
      parse_span = trace.newSpanWithThread("parseAndValidate")
          .withTag("startThread", Thread.currentThread().getName())
          .asChildOf(query_span)
          .start();
    }
    
    // parse the query
    final String content_type = request.getHeader("Content-Type");
    final SemanticQuery.Builder query_builder;
    if (content_type != null && content_type.toLowerCase().contains("yaml")) {
      final JsonNode node = YAML.getMapper().readTree(request.getInputStream());
      query_builder = SemanticQuery.parse(tsdb, node);
    } else {
      final JsonNode node = JSON.getMapper().readTree(request.getInputStream());
      query_builder = SemanticQuery.parse(tsdb, node);
    }
    
    // TODO validate
    if (parse_span != null) {
      parse_span.setSuccessTags()
                .finish();
    }
    
    Map<String, String> log_headers = null;
    Enumeration<String> keys = request.getHeaderNames();
    while (keys.hasMoreElements()) {
      final String name = keys.nextElement();
      if (name.toLowerCase().startsWith("x-") &&
          !Strings.isNullOrEmpty(request.getHeader(name))) {
        if (log_headers == null) {
          log_headers = Maps.newHashMap();
        }
        log_headers.put(name, request.getHeader(name));
      }
    }
    
    final SemanticQuery query = query_builder.build();
    LOG.info("Executing new query=" + JSON.serializeToString(
        ImmutableMap.<String, Object>builder()
        .put("headers", log_headers == null ? "null" : log_headers)
        .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
        //.put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
        .put("traceId", trace != null ? trace.traceId() : "")
        .put("user", auth_state != null ? auth_state.getUser() : "Unkown")
        .put("query", query)
        .build()));
    Span setup_span = null;
    if (query_span != null) {
      setup_span = trace.newSpanWithThread("setupContext")
          .withTag("startThread", Thread.currentThread().getName())
          .asChildOf(query_span)
          .start();
    }
    
    SerdesOptions serdes = query.getSerdesConfigs().isEmpty() ? null :
      query.getSerdesConfigs().get(0);
    if (serdes == null) {
      serdes = JsonV2QuerySerdesOptions.newBuilder()
          .setId("JsonV3QuerySerdes")
          .build();
    }
    
    final AsyncContext async = request.startAsync();
    async.setTimeout((Integer) servlet_config.getServletContext()
        .getAttribute(OpenTSDBApplication.ASYNC_TIMEOUT_ATTRIBUTE));
    
    SemanticQueryContext context = (SemanticQueryContext) SemanticQueryContext.newBuilder()
        .setTSDB(tsdb)
        .setQuery(query)
        .setStats(DefaultQueryStats.newBuilder()
            .setTrace(trace)
            .setQuerySpan(query_span)
            .build())
        .setAuthState(auth_state)
        .addSink(ServletSinkConfig.newBuilder()
            .setId(ServletSinkFactory.TYPE)
            .setSerdesOptions(serdes)
            .setRequest(request)
            .setAsync(async)
            .setStatsTimer(timer)
            .build())
        .build();
    tsdb.registerRunningQuery(Long.parseLong(request.getHeader(
        OpenTSDBApplication.INTERNAL_HASH_HEADER)), context);
    
    if (trace != null && query.isDebugEnabled()) {
      context.logDebug("Trace ID: " + trace.traceId());
    }
    
    RunTSDQuery runTsdQuery = new RunTSDQuery(query_span, async, query, context);
    
    LOG.info("Creating async query");

    tsdb.getQueryThreadPool().submit(runTsdQuery, null, TSDTask.QUERY).get();
    
    return null;
  }

  private void asyncRun(final Span query_span, final AsyncContext async, final SemanticQuery query,
      SemanticQueryContext context) {
    class AsyncTimeout implements AsyncListener {

      @Override
      public void onComplete(final AsyncEvent event) throws IOException {
        // no-op
      }

      @Override
      public void onTimeout(final AsyncEvent event) throws IOException {
        LOG.error("The query has timed out");
        GenericExceptionMapper.serialize(
            new QueryExecutionException("The query has exceeded "
            + "the timeout limit.", 504), event.getAsyncContext().getResponse());
        event.getAsyncContext().complete();
      }

      @Override
      public void onError(final AsyncEvent event) throws IOException {
        LOG.error("WTF? An error for the AsyncTimeout?: " + event);
      }

      @Override
      public void onStartAsync(final AsyncEvent event) throws IOException {
        // no-op
      }

    }

    async.addListener(new AsyncTimeout());
    async.start(new Runnable() {
      public void run() {
        try {
          context.initialize(query_span).join();
          
          if (query.getMode() == QueryMode.VALIDATE) {
            async.getResponse().setContentType("application/json");
            // TODO - here it would be better to write the query plan.
            async.getResponse().getWriter().write("{\"status\":\"OK\"}");
            ((HttpServletResponse) async.getResponse()).setStatus(200);
            async.complete();
            if (query_span != null) {
              query_span.setSuccessTags().finish();
            }
            return;
          }
          context.fetchNext(query_span);
        } catch (Throwable t) {
          LOG.error("Unexpected exception triggering query.", t);
          GenericExceptionMapper.serialize(t, async.getResponse());
    
          async.complete();
          if (query_span != null) {
            query_span.setErrorTags(t)
                       .finish();
          }
          throw new QueryExecutionException("Unexpected expection", 500, t);
        }
      }
    });
  }
  
}
