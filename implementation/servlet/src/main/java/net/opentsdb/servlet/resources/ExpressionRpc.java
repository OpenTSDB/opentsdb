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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

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
import javax.ws.rs.core.StreamingOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import jersey.repackaged.com.google.common.collect.ImmutableMap;
import net.opentsdb.auth.AuthState;
import net.opentsdb.auth.Authentication;
import net.opentsdb.auth.AuthState.AuthStatus;
import net.opentsdb.core.TSDB;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.execution.serdes.JsonV2ExpQuerySerdes;
import net.opentsdb.query.execution.serdes.JsonV2QuerySerdesOptions;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.servlet.filter.AuthFilter;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.Trace;
import net.opentsdb.stats.Tracer;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.JSON;

@Path("api/query/exp")
public class ExpressionRpc {
  private static final Logger LOG = LoggerFactory.getLogger(QueryRpc.class);
  private static final Logger QUERY_LOG = LoggerFactory.getLogger("QueryLog");
  
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
                       final @Context HttpServletRequest request) throws Exception {
    if (request.getAttribute(OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE) != null) {
      return handleException(request);
    } else if (request.getAttribute(
        OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE) != null) {
      return handeResponse(servlet_config, request);
    } else {
      return handleQuery(servlet_config, request, false);
    }
  }

  /**
   * Method that parses the query and triggers it asynchronously.
   * @param servlet_config The servlet config to fetch the TSDB from.
   * @param request The request.
   * @param is_get Whether or not it was a get request. Lets us determine whether
   * or not to read from the input stream.
   * @return Always null.
   * @throws Exception if something went pear shaped.
   */
  @VisibleForTesting
  Response handleQuery(final ServletConfig servlet_config, 
                       final HttpServletRequest request,
                       final boolean is_get) throws Exception {
    
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
    
    if (tsdb.getStatsCollector() != null) {
      tsdb.getStatsCollector().incrementCounter("query.new", "endpoint", "2x");
    }
    
    // check auth. 
    final AuthState auth_state;
    if (tsdb.getConfig().getBoolean(Authentication.AUTH_ENABLED_KEY)) {
      if (request.getAttribute(AuthFilter.AUTH_STATE_KEY) == null || 
          ((AuthState) request.getAttribute(AuthFilter.AUTH_STATE_KEY))
            .getStatus() != AuthStatus.SUCCESS) {
        throw new WebApplicationException("Access denied.", 
            Response.Status.FORBIDDEN);
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
      request.setAttribute(TRACE_KEY, trace);
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
      ts_query.validate();
      request.setAttribute(V2EXP_QUERY_KEY, ts_query);
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
    
    final SemanticQuery.Builder query = ts_query.convert();
    
    LOG.info("Executing new query=" + JSON.serializeToString(
        ImmutableMap.<String, Object>builder()
        // TODO - possible upstream headers
        //.put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
        //.put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes())) // TODO
        .put("traceId", trace != null ? trace.traceId() : "")
        .put("query", ts_query)
        .build()));
    if (convert_span != null) {
      convert_span.setTag("Status", "OK")
                  .setTag("finalThread", Thread.currentThread().getName())
                  .finish();
    }
    
    Span setup_span = null;
    if (query_span != null) {
      setup_span = trace.newSpanWithThread("setupContext")
          .withTag("startThread", Thread.currentThread().getName())
          .asChildOf(query_span)
          .start();
    }
    
    class LocalSink implements QuerySink {
      final AsyncContext async;
      
      LocalSink(final AsyncContext async) {
        this.async = async;
      }
      
      @Override
      public void onComplete() {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Yay, all done!");
        }
      }

      @Override
      public void onNext(final QueryResult next) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Successful response for query=" 
              + JSON.serializeToString(
                  ImmutableMap.<String, Object>builder()
                  // TODO - possible upstream headers
                  //.put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
                  //.put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes())) /// TODO
                  .put("traceId", trace != null ? trace.traceId() : "")
                  .put("query", ts_query)
                  .build()));
        }
       
        request.setAttribute(OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE, next);
        try {
          async.dispatch();
        } catch (Exception e) {
          LOG.error("Unexpected exception dispatching async request for "
              + "query: " + ts_query, e);
        }
      }

      @Override
      public void onError(Throwable t) {
        LOG.error("Exception for query: " 
            //+ Bytes.byteArrayToString(query.buildHashCode().asBytes()), t);
            ,t);
        request.setAttribute(OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE, t);
        try {
          async.dispatch();
        } catch (Exception e) {
          LOG.error("WFT? Dispatch may have already been called", e);
        }
      }
    }
    
    // start the Async context and pass it around. 
    // WARNING After this point, make sure to catch all exceptions and dispatch
    // the context, otherwise the client will wait for the timeout handler.
    final AsyncContext async = request.startAsync();
    async.setTimeout((Integer) servlet_config.getServletContext()
        .getAttribute(OpenTSDBApplication.ASYNC_TIMEOUT_ATTRIBUTE));
    
    query.addSink(new LocalSink(async));
    TimeSeriesQuery q = query.build();
    SemanticQueryContext context = (SemanticQueryContext) SemanticQueryContext.newBuilder()
        .setTSDB(tsdb)
        .setQuery(q)
        .build();
    request.setAttribute(QUERY_KEY, q);
    request.setAttribute(CONTEXT_KEY, context);
    class AsyncTimeout implements AsyncListener {

      @Override
      public void onComplete(AsyncEvent event) throws IOException {
        // TODO Auto-generated method stub
        LOG.debug("Yay the async was all done!");
      }

      @Override
      public void onTimeout(AsyncEvent event) throws IOException {
        // TODO Auto-generated method stub
        LOG.error("The query has timed out");
        try {
          context.close();
        } catch (Exception e) {
          LOG.error("Failed to close the query: ", e);
        }
        throw new QueryExecutionException("The query has exceeded "
            + "the timeout limit.", 504);
      }

      @Override
      public void onError(AsyncEvent event) throws IOException {
        // TODO Auto-generated method stub
        LOG.error("WTF? An error for the AsyncTimeout?: " + event);
      }

      @Override
      public void onStartAsync(AsyncEvent event) throws IOException {
        // TODO Auto-generated method stub
        LOG.debug("Starting an async something or other");
      }

    }

    async.addListener(new AsyncTimeout());
    
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
      context.fetchNext(query_span);
    } catch (Exception e) {
      LOG.error("Unexpected exception adding callbacks to deferred.", e);
      request.setAttribute(OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE, e);
      try {
        async.dispatch();
      } catch (Exception ex) {
        LOG.error("WFT? Dispatch may have already been called", ex);
      }
    }
    
    if (execute_span != null) {
      execute_span.setTag("Status", "OK")
                  .setTag("finalThread", Thread.currentThread().getName())
                  .finish();
    }
    return null;
  }

  /**
   * Method for serializing a successful query response.
   * @param servlet_config The servlet config to fetch the TSDB from.
   * @param request The request.
   * @return a Response object with a 200 status code
   */
  @VisibleForTesting
  Response handeResponse(final ServletConfig servlet_config, 
                         final HttpServletRequest request) {
    final QueryResult result = (QueryResult) request.getAttribute(
        OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE);
    final QueryContext context = (QueryContext) request.getAttribute(CONTEXT_KEY);
//    if (context.stats().trace() != null) {
//      response_span = context.stats().trace().newSpanWithThread("responseHandler")
//          .withTag("startThread", Thread.currentThread().getName())
//          .asChildOf(context.stats().trace().firstSpan())
//          .start();
//    } else {
//      response_span = null;
//      trace = null;
//    }
    
    final TimeSeriesQuery ts_query = (TimeSeriesQuery) request.getAttribute(V2EXP_QUERY_KEY);
    final SerdesOptions options = JsonV2QuerySerdesOptions.newBuilder()
        .setMsResolution(false)
//        .setShowQuery(ts_query.getShowQuery())
//        .setShowStats(ts_query.getShowStats())
//        .setShowSummary(ts_query.getShowSummary())
//        .setStart(((net.opentsdb.query.pojo.TimeSeriesQuery) ts_query).getTime().startTime())
//        .setEnd(((net.opentsdb.query.pojo.TimeSeriesQuery) ts_query).getTime().endTime())
        .setId("Json")
        .build();
    
    /** The stream to write to. */
    final StreamingOutput stream = new StreamingOutput() {
      @Override
      public void write(final OutputStream output)
          throws IOException, WebApplicationException {
        Span serdes_span = null;

        final JsonV2ExpQuerySerdes serdes = new JsonV2ExpQuerySerdes();
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
        try {
          serdes.initialize(tsdb).join();
        } catch (InterruptedException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        } catch (Exception e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
        try {
          // TODO - ug ug ugggg!!!
          serdes.serialize(context, options, output, result, serdes_span).join();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

      }
    };
    
    return Response.ok().entity(stream)
        .type(MediaType.APPLICATION_JSON)
        .build();
    // all done!
  }
  
  /**
   * Handles exceptions returned by the query execution.
   * @param request The request.
   * @return Always null.
   * @throws Exception if something goes pear shaped.
   */
  @VisibleForTesting
  Response handleException(final HttpServletRequest request) throws Exception {
    final QueryContext context = (QueryContext) request.getAttribute(CONTEXT_KEY);
    final TimeSeriesQuery query = (TimeSeriesQuery) request.getAttribute(QUERY_KEY);
    final Trace trace;
    if (context != null && context.stats().trace() != null) {
      trace = context.stats().trace();
    } else {
      trace = null;
    }
    
    final Exception e = (Exception) request.getAttribute(
            OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE);
    LOG.info("Completing query=" 
      + JSON.serializeToString(ImmutableMap.<String, Object>builder()
      // TODO - possible upstream headers
      .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
      //.put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
      .put("traceId", trace != null ? trace.traceId() : "")
      .put("status", Response.Status.OK)
      .put("query", request.getAttribute(V2EXP_QUERY_KEY))
      .build()));
    
    QUERY_LOG.info("Completing query=" 
       + JSON.serializeToString(ImmutableMap.<String, Object>builder()
      
      // TODO - possible upstream headers
      .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
      //.put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
      .put("traceId", trace != null ? trace.traceId() : "")
      .put("status", Response.Status.OK)
      //.put("trace", trace.serializeToString())
      .put("query", request.getAttribute(V2EXP_QUERY_KEY))
      .build()));
    
    if (trace != null && trace.firstSpan() != null) {
      trace.firstSpan()
        .setTag("status", "Error")
        .setTag("finalThread", Thread.currentThread().getName())
        .setTag("error", e.getMessage() == null ? "null" : e.getMessage())
        .log("exception", e)
        .finish();
    }
    //query_exceptions.incrementAndGet();
    throw e;
  }

}
