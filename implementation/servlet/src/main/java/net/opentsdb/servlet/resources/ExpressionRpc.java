// This file is part of OpenTSDB.
// Copyright (C) 2016-2018  The OpenTSDB Authors.
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import net.opentsdb.auth.AuthState;
import net.opentsdb.auth.Authentication;
import net.opentsdb.auth.AuthState.AuthStatus;
import net.opentsdb.core.TSDB;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.execution.serdes.JsonV2QuerySerdesOptions;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.servlet.exceptions.GenericExceptionMapper;
import net.opentsdb.servlet.filter.AuthFilter;
import net.opentsdb.servlet.resources.RawQueryRpc.RunTSDQuery;
import net.opentsdb.servlet.sinks.ServletSinkConfig;
import net.opentsdb.servlet.sinks.ServletSinkFactory;
import net.opentsdb.stats.DefaultQueryStats;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.Trace;
import net.opentsdb.stats.Tracer;
import net.opentsdb.stats.StatsCollector.StatsTimer;
import net.opentsdb.threadpools.TSDTask;
import net.opentsdb.utils.JSON;

@Path("query/exp")
public class ExpressionRpc {
  private static final Logger LOG = LoggerFactory.getLogger(QueryRpc.class);
  
  /** Request key used for the V3 TSDB query. */
  public static final String QUERY_KEY = "TSDQUERY";
  
  /** Request key used for the V2 TSDB expression query. */
  public static final String V2EXP_QUERY_KEY = "V2TSDQUERY";
  
  /** Request key for the query context. */
  public static final String CONTEXT_KEY = "CONTEXT";
  
  /** Request key for the tracer. */
  public static final String TRACE_KEY = "TRACE";

  /**
   * Handles POST requests for parsing TSDB v2 queries from JSON.
   * @param servlet_config The servlet config to fetch the TSDB from.
   * @param request The request.
   * @return A response on success.
   * @throws Exception if something went pear shaped.
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response post(final @Context ServletConfig servlet_config, 
                       final @Context HttpServletRequest request/*,
                       final @Context HttpServletResponse response*/) throws Exception {
    return handleQuery(servlet_config, request);
  }

  class RunTSDQuery implements Runnable {
    final Trace trace;
    final Span query_span;
    final Span setup_span;
    final AsyncContext async;

    final SemanticQueryContext context;

    public RunTSDQuery(Trace trace, Span query_span, Span setup_span, AsyncContext async,
        SemanticQueryContext ctx) {
      this.trace = trace;
      this.query_span = query_span;
      this.setup_span = setup_span;
      this.async = async;
      this.context = ctx;
    }

    @Override
    public void run() {
      asyncRun(trace, query_span, setup_span, async, context);
    }
  }
  
  private Response handleQuery(final ServletConfig servlet_config, final HttpServletRequest request) throws InterruptedException, ExecutionException {
    final Object stream = request.getAttribute("DATA");
    if (stream != null) {
      return Response.ok()
          .entity(((ByteArrayOutputStream) stream).toByteArray())
          .header("Content-Type", "application/json")
          .build();
    }
    
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
      tsdb.getStatsCollector().incrementCounter("query.new", "endpoint", "2x");
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
          .withTag("endpoint", "/api/query")
          .withTag("startThread", Thread.currentThread().getName())
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
    final net.opentsdb.query.pojo.TimeSeriesQuery ts_query;
    try {
      ts_query = JSON.parseToObject(request.getInputStream(),
          net.opentsdb.query.pojo.TimeSeriesQuery.class);
      // throws an exception if invalid.
      ts_query.validate(tsdb);
    } catch (Exception e) {
      throw new QueryExecutionException("Invalid query", 400, e);
    }
    
    if (parse_span != null) {
      parse_span.setTag("Status", "OK")
                .setTag("finalThread", Thread.currentThread().getName())
                .finish();
    }
    
    Span convert_span = null;
    if (query_span != null) {
      convert_span = trace.newSpanWithThread("convertAndValidate")
          .withTag("startThread", Thread.currentThread().getName())
          .asChildOf(query_span)
          .start();
    }
    // copy the required headers.
    // TODO - break this out into a helper function.
    final Enumeration<String> headers = request.getHeaderNames();
    final Map<String, String> headers_copy = new HashMap<String, String>();
    while (headers.hasMoreElements()) {
      final String header = headers.nextElement();
      if (header.toUpperCase().startsWith("X") || header.equals("Cookie")) {
        headers_copy.put(header, request.getHeader(header));
      }
    }
    
    final SemanticQuery.Builder query = ts_query.convert(tsdb);
    
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
    
    LOG.info("Executing new query=" + JSON.serializeToString(
        ImmutableMap.<String, Object>builder()
        .put("headers", log_headers == null ? "null" : log_headers)
        //.put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
        //.put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes())) // TODO
        .put("traceId", trace != null ? trace.traceId() : "")
        .put("user", auth_state != null ? auth_state.getUser() : "Unkown")
        .put("query", ts_query)
        .build()));
    if (convert_span != null) {
      convert_span.setTag("Status", "OK")
                  .setTag("finalThread", Thread.currentThread().getName())
                  .finish();
    }
    
    final Span setup_span;
    if (query_span != null) {
      setup_span = trace.newSpanWithThread("setupContext")
          .withTag("startThread", Thread.currentThread().getName())
          .asChildOf(query_span)
          .start();
    } else {
      setup_span = null;
    }
    
    // start the Async context and pass it around. 
    // WARNING After this point, make sure to catch all exceptions and dispatch
    // the context, otherwise the client will wait for the timeout handler.
    final AsyncContext async = request.startAsync();
    async.setTimeout((Integer) servlet_config.getServletContext()
        .getAttribute(OpenTSDBApplication.ASYNC_TIMEOUT_ATTRIBUTE));
    
    TimeSeriesQuery q = query.build();
    SerdesOptions serdes = q.getSerdesConfigs().isEmpty() ? null :
      q.getSerdesConfigs().get(0);
    if (serdes == null) {
      serdes = JsonV2QuerySerdesOptions.newBuilder()
          .setId("JsonV2ExpQuerySerdes")
          .build();
    }
    
    SemanticQueryContext context = (SemanticQueryContext) SemanticQueryContext.newBuilder()
        .setTSDB(tsdb)
        .setQuery(q)
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
    
    RunTSDQuery runTsdQuery = new RunTSDQuery(trace, query_span, setup_span, async, context);
    
    LOG.info("Creating async query");

    tsdb.getQueryThreadPool().submit(runTsdQuery, null, TSDTask.QUERY);
    
    return null;
  }


  private void asyncRun(final Trace trace, final Span query_span, final Span setup_span,
      final AsyncContext async, SemanticQueryContext context) {
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
        if (setup_span != null) {
          setup_span.setTag("Status", "OK")
                    .setTag("finalThread", Thread.currentThread().getName())
                    .finish();
        }
        
        Span execute_span = null;
        if (query_span != null) {
          execute_span = trace.newSpanWithThread("startExecution")
              .withTag("startThread", Thread.currentThread().getName())
              .asChildOf(query_span)
              .start();
        }
        
        try {
          context.initialize(query_span).join();
          context.fetchNext(query_span);
        } catch (Throwable t) {
          LOG.error("Unexpected exception triggering query.", t);
          if (execute_span != null) {
            execute_span.setErrorTags(t)
                        .finish();
          }
          //GenericExceptionMapper.serialize(t, response);
          async.complete();
          if (query_span != null) {
            query_span.setErrorTags(t)
                       .finish();
          }
          throw new QueryExecutionException("Unexpected expection", 500, t);
        }
        
        if (execute_span != null) {
          execute_span.setSuccessTags()
                      .finish();
        }
      }
    });
  }
  
}
