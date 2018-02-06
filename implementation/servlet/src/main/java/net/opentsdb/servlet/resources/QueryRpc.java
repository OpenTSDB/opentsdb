// This file is part of OpenTSDB.
// Copyright (C) 2013-2017 The OpenTSDB Authors.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
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

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import jersey.repackaged.com.google.common.collect.ImmutableMap;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.DefaultQueryContextBuilder;
import net.opentsdb.query.TSQuery;
import net.opentsdb.query.TSSubQuery;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.execution.serdes.JsonV2QuerySerdes;
import net.opentsdb.query.execution.serdes.JsonV2QuerySerdesOptions;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.RateOptions;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.stats.DefaultQueryStats;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.stats.Trace;
import net.opentsdb.stats.Tracer;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.StringUtils;

/**
 * Handles OpenTSDB version 2.0 JSON queries from the /api/query endpoint.
 * Also supports the legacy URI parameter query format.
 * <p>
 * This has been modified to run under a servlet instead of using Netty directly
 * as TSDB used to. It also uses the servlet asynchronously.
 * 
 * @since 2.0
 */
@Path("api/query")
final public class QueryRpc {
  private static final Logger LOG = LoggerFactory.getLogger(QueryRpc.class);
  private static final Logger QUERY_LOG = LoggerFactory.getLogger("QueryLog");
  
  /** Request key used for the V3 TSDB query. */
  public static final String QUERY_KEY = "TSDQUERY";
  
  /** Request key used for the V2 TSDB non-expression query. */
  public static final String V2_QUERY_KEY = "V2TSDQUERY";
  
  /** Request key for the query context. */
  public static final String CONTEXT_KEY = "CONTEXT";
  
  /** Request key for the tracer. */
  public static final String TRACE_KEY = "TRACE";
  
  /** Various counters and metrics for reporting query stats */
  private final AtomicLong query_invalid = new AtomicLong();
  private final AtomicLong query_exceptions = new AtomicLong();
  private final AtomicLong query_success = new AtomicLong();
  
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
   * Handles GET requests for converting URI parameters to a v2 query for
   * backwards compatibility.
   * @param servlet_config The servlet config to fetch the TSDB from.
   * @param request The request.
   * @return A response on success.
   * @throws Exception if something went pear shaped.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response get(final @Context ServletConfig servlet_config, 
                      final @Context HttpServletRequest request) throws Exception {
    if (request.getAttribute(OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE) != null) {
      return handleException(request);
    } else if (request.getAttribute(
        OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE) != null) {
      return handeResponse(servlet_config, request);
    } else {
      return handleQuery(servlet_config, request, true);
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
  @SuppressWarnings({ "unchecked" })
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
    } else if (!(obj instanceof DefaultTSDB)) {
      throw new WebApplicationException("Object stored for as the TSDB was "
          + "of the wrong type: " + obj.getClass(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    final DefaultTSDB tsdb = (DefaultTSDB) obj;
    
    // initiate the tracer
    final Trace trace;
    final Span query_span;
    final Tracer tracer = (Tracer) tsdb.getRegistry().getDefaultPlugin(Tracer.class);
    if (tracer != null) {
      trace = tracer.newTrace(true, true);
      query_span = trace.newSpanWithThread(this.getClass().getSimpleName())
          .withTag("endpoint", "/api/query")
          .withTag("startThread", Thread.currentThread().getName())
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
    final TSQuery ts_query;
    try {
      if (is_get) {
        ts_query = parseQuery(tsdb, request, null);
      } else {
        ts_query = JSON.parseToObject(request.getInputStream(), TSQuery.class);
      }
      // throws an exception if invalid.
      ts_query.validateAndSetQuery();
      request.setAttribute(V2_QUERY_KEY, ts_query);
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
    
    final TimeSeriesQuery query = TSQuery.convertQuery(ts_query);
    if (query_span != null) {
      query_span.setTag("queryId", 
          Bytes.byteArrayToString(query.buildHashCode().asBytes()));
    }
    query.validate();
    request.setAttribute(QUERY_KEY, query);
    LOG.info("Executing new query=" + JSON.serializeToString(
        ImmutableMap.<String, Object>builder()
        // TODO - possible upstream headers
        .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
        .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
        .put("traceId", trace != null ? trace.traceId() : null)
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
    // setup the context and copy headers for downstream use.
//    final QueryContext context = new DefaultQueryContext(tsdb, 
//        tsdb.getRegistry().getDefaultExecutionGraph(), 
//        trace == null ? null : trace.tracer());
//    context.addSessionObject(HttpQueryV2Executor.SESSION_HEADERS_KEY, 
//        headers_copy);
//    request.setAttribute(CONTEXT_KEY, context);
//    
//    obj = context.executionGraph().sinkExecutor();
//    if (obj == null) {
//      throw new WebApplicationException("Execution graph returned a null "
//          + "sink executor: " + tsdb.getRegistry().getDefaultExecutionGraph(),
//          Response.Status.INTERNAL_SERVER_ERROR);
//    }
    
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
                  .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
                  .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
                  .put("traceId", trace != null ? trace.traceId() : null)
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
            + Bytes.byteArrayToString(query.buildHashCode().asBytes()), t);
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
    
    final QueryContext ctx = DefaultQueryContextBuilder.newBuilder(tsdb)
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .addQuerySink(new LocalSink(async))
        .setStats(DefaultQueryStats.newBuilder()
            .setTrace(trace)
            .build())
        // TODO - stats
        .build();
    request.setAttribute(CONTEXT_KEY, ctx);
    
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
      ctx.fetchNext();
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
    final TimeSeriesQuery query = (TimeSeriesQuery) request.getAttribute(QUERY_KEY);
    final Span response_span;
    final Trace trace;
    if (context.stats().trace() != null) {
      response_span = context.stats().trace().newSpanWithThread("responseHandler")
          .withTag("startThread", Thread.currentThread().getName())
          .asChildOf(context.stats().trace().firstSpan())
          .start();
    } else {
      response_span = null;
      trace = null;
    }
    
    final TSQuery ts_query = (TSQuery) request.getAttribute(V2_QUERY_KEY);
    final JsonV2QuerySerdesOptions options = JsonV2QuerySerdesOptions.newBuilder()
        .setMsResolution(ts_query.getMsResolution())
        .setShowQuery(ts_query.getShowQuery())
        .setShowStats(ts_query.getShowStats())
        .setShowSummary(ts_query.getShowSummary())
        .build();
    
    /** The stream to write to. */
    final StreamingOutput stream = new StreamingOutput() {
      @Override
      public void write(OutputStream output)
          throws IOException, WebApplicationException {
        Span serdes_span = null;
        if (response_span != null) {
          serdes_span = context.stats().trace().newSpanWithThread("serdes")
              .withTag("startThread", Thread.currentThread().getName())
              .asChildOf(response_span)
              .start();
        }
        final JsonGenerator json = JSON.getFactory().createGenerator(output);
        json.writeStartArray();
        
        final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(json);
        serdes.serialize(context, options, output, result);
        
        if (options.showSummary()) {
          json.writeObjectFieldStart("summary");
          json.writeStringField("queryHash", Bytes.byteArrayToString(
              query.buildTimelessHashCode().asBytes()));
          json.writeStringField("queryId", Bytes.byteArrayToString(
              query.buildHashCode().asBytes()));
          json.writeStringField("traceId", context.stats().trace() == null ? "null" : 
            context.stats().trace().traceId());
          if (context.stats().trace() != null) {
            //trace.serializeJSON("trace", json);
          }
          json.writeEndObject();
        }
        
        // final
        json.writeEndArray();
        json.close();
        
        // TODO - trace, other bits.
        if (serdes_span != null) {
          serdes_span.setTag("finalThread", Thread.currentThread().getName())
                     .setTag("status", "OK")
                     .finish();
        }
        
        query_success.incrementAndGet();
        LOG.info("Completing query=" 
            + JSON.serializeToString(ImmutableMap.<String, Object>builder()
            // TODO - possible upstream headers
            .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
            .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
            //.put("traceId", trace != null ? trace.getTraceId() : null)
            .put("status", Response.Status.OK)
            .put("query", request.getAttribute(V2_QUERY_KEY))
            .build()));
          
          QUERY_LOG.info("Completing query=" 
            + JSON.serializeToString(ImmutableMap.<String, Object>builder()
            // TODO - possible upstream headers
            .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
            .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
            //.put("traceId", trace != null ? trace.getTraceId() : null)
            .put("status", Response.Status.OK)
            //.put("trace", trace.serializeToString())
            .put("query", request.getAttribute(V2_QUERY_KEY))
            .build()));
         
          
          if (response_span != null) {
            response_span.setTag("finalThread", Thread.currentThread().getName())
                         .setTag("status", "OK")
                         .finish();
          }
          if (context.stats().trace() != null && 
              context.stats().trace().firstSpan() != null) {
            context.stats().trace().firstSpan()
              .setTag("status", "OK")
              .setTag("finalThread", Thread.currentThread().getName())
              .finish();
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
      .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
      .put("traceId", trace != null ? trace.traceId() : null)
      .put("status", Response.Status.OK)
      .put("query", request.getAttribute(V2_QUERY_KEY))
      .build()));
    
    QUERY_LOG.info("Completing query=" 
       + JSON.serializeToString(ImmutableMap.<String, Object>builder()
      
      // TODO - possible upstream headers
      .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
      .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
      .put("traceId", trace != null ? trace.traceId() : null)
      .put("status", Response.Status.OK)
      //.put("trace", trace.serializeToString())
      .put("query", request.getAttribute(V2_QUERY_KEY))
      .build()));
    
    if (trace != null && trace.firstSpan() != null) {
      trace.firstSpan()
        .setTag("status", "Error")
        .setTag("finalThread", Thread.currentThread().getName())
        .setTag("error", e.getMessage())
        .log("exception", e)
        .finish();
    }
    query_exceptions.incrementAndGet();
    throw e;
  }
//  /**
//   * Implements the /api/query endpoint to fetch data from OpenTSDB.
//   * @param tsdb The TSDB to use for fetching data
//   * @param query The HTTP query for parsing and responding
//   */
//  @Override
//  public void execute(final TSDB tsdb, final HttpQuery query) 
//    throws IOException {
//    
//    // only accept GET/POST/DELETE
//    if (query.method() != HttpMethod.GET && query.method() != HttpMethod.POST &&
//        query.method() != HttpMethod.DELETE) {
//      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
//          "Method not allowed", "The HTTP method [" + query.method().getName() +
//          "] is not permitted for this endpoint");
//    }
//    if (query.method() == HttpMethod.DELETE && 
//        !tsdb.getConfig().getBoolean("tsd.http.query.allow_delete")) {
//      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
//               "Bad request",
//               "Deleting data is not enabled (tsd.http.query.allow_delete=false)");
//    }
//    
//    final String[] uri = query.explodeAPIPath();
//    final String endpoint = uri.length > 1 ? uri[1] : ""; 
//    
//    if (endpoint.toLowerCase().equals("last")) {
//      handleLastDataPointQuery(tsdb, query);
//    } else if (endpoint.toLowerCase().equals("gexp")){
//      handleQuery(tsdb, query, true);
//    } else if (endpoint.toLowerCase().equals("exp")) {
//      handleExpressionQuery(tsdb, query);
//      return;
//    } else {
//      handleQuery(tsdb, query, false);
//    }
//  }
//
//  /**
//   * Processing for a data point query
//   * @param tsdb The TSDB to which we belong
//   * @param query The HTTP query to parse/respond
//   * @param allow_expressions Whether or not expressions should be parsed
//   * (based on the endpoint)
//   */
//  private void handleQuery(final TSDB tsdb, final HttpQuery query, 
//      final boolean allow_expressions) {
//    final long start = DateTime.currentTimeMillis();
//    final TSQuery data_query;
//    final List<ExpressionTree> expressions;
//    if (query.method() == HttpMethod.POST) {
//      switch (query.apiVersion()) {
//      case 0:
//      case 1:
//        data_query = query.serializer().parseQueryV1();
//        break;
//      default:
//        query_invalid.incrementAndGet();
//        throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
//            "Requested API version not implemented", "Version " + 
//            query.apiVersion() + " is not implemented");
//      }
//      expressions = null;
//    } else {
//      expressions = new ArrayList<ExpressionTree>();
//      data_query = parseQuery(tsdb, query, expressions);
//    }
//    
//    if (query.getAPIMethod() == HttpMethod.DELETE &&
//        tsdb.getConfig().getBoolean("tsd.http.query.allow_delete")) {
//      data_query.setDelete(true);
//    }
//    
//    // validate and then compile the queries
//    try {
//      LOG.debug(data_query.toString());
//      data_query.validateAndSetQuery();
//    } catch (Exception e) {
//      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
//          e.getMessage(), data_query.toString(), e);
//    }
//    
//    // if the user tried this query multiple times from the same IP and src port
//    // they'll be rejected on subsequent calls
//    final QueryStats query_stats = 
//        new QueryStats(query.getRemoteAddress(), data_query, 
//            query.getPrintableHeaders());
//    data_query.setQueryStats(query_stats);
//    query.setStats(query_stats);
//    
//    final int nqueries = data_query.getQueries().size();
//    final ArrayList<DataPoints[]> results = new ArrayList<DataPoints[]>(nqueries);
//    final List<Annotation> globals = new ArrayList<Annotation>();
//    
//    /** This has to be attached to callbacks or we may never respond to clients */
//    class ErrorCB implements Callback<Object, Exception> {
//      public Object call(final Exception e) throws Exception {
//        Throwable ex = e;
//        try {
//          LOG.error("Query exception: ", e);
//          if (ex instanceof DeferredGroupException) {
//            ex = e.getCause();
//            while (ex != null && ex instanceof DeferredGroupException) {
//              ex = ex.getCause();
//            }
//            if (ex == null) {
//              LOG.error("The deferred group exception didn't have a cause???");
//            }
//          } 
//
//          if (ex instanceof RpcTimedOutException) {
//            query_stats.markSerialized(HttpResponseStatus.REQUEST_TIMEOUT, ex);
//            query.badRequest(new BadRequestException(
//                HttpResponseStatus.REQUEST_TIMEOUT, ex.getMessage()));
//            query_exceptions.incrementAndGet();
//          } else if (ex instanceof HBaseException) {
//            query_stats.markSerialized(HttpResponseStatus.FAILED_DEPENDENCY, ex);
//            query.badRequest(new BadRequestException(
//                HttpResponseStatus.FAILED_DEPENDENCY, ex.getMessage()));
//            query_exceptions.incrementAndGet();
//          } else if (ex instanceof QueryException) {
//            query_stats.markSerialized(((QueryException)ex).getStatus(), ex);
//            query.badRequest(new BadRequestException(
//                ((QueryException)ex).getStatus(), ex.getMessage()));
//            query_exceptions.incrementAndGet();
//          } else if (ex instanceof BadRequestException) {
//            query_stats.markSerialized(((BadRequestException)ex).getStatus(), ex);
//            query.badRequest((BadRequestException)ex);
//            query_invalid.incrementAndGet();
//          } else if (ex instanceof NoSuchUniqueName) {
//            query_stats.markSerialized(HttpResponseStatus.BAD_REQUEST, ex);
//            query.badRequest(new BadRequestException(ex));
//            query_invalid.incrementAndGet();
//          } else {
//            query_stats.markSerialized(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
//            query.badRequest(new BadRequestException(ex));
//            query_exceptions.incrementAndGet();
//          }
//          
//        } catch (RuntimeException ex2) {
//          LOG.error("Exception thrown during exception handling", ex2);
//          query_stats.markSerialized(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex2);
//          query.sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
//              ex2.getMessage().getBytes());
//          query_exceptions.incrementAndGet();
//        }
//        return null;
//      }
//    }
//    
//    /**
//     * After all of the queries have run, we get the results in the order given
//     * and add dump the results in an array
//     */
//    class QueriesCB implements Callback<Object, ArrayList<DataPoints[]>> {
//      public Object call(final ArrayList<DataPoints[]> query_results) 
//        throws Exception {
//        if (allow_expressions) {
//          // process each of the expressions into a new list, then merge it
//          // with the original. This avoids possible recursion loops.
//          final List<DataPoints[]> expression_results = 
//              new ArrayList<DataPoints[]>(expressions.size());
//          // let exceptions bubble up
//          for (final ExpressionTree expression : expressions) {
//            expression_results.add(expression.evaluate(query_results));
//          }
//          results.addAll(expression_results);
//        } else {
//          results.addAll(query_results);
//        }
//        
//        /** Simply returns the buffer once serialization is complete and logs it */
//        class SendIt implements Callback<Object, ChannelBuffer> {
//          public Object call(final ChannelBuffer buffer) throws Exception {
//            query.sendReply(buffer);
//            query_success.incrementAndGet();
//            return null;
//          }
//        }
//
//        switch (query.apiVersion()) {
//        case 0:
//        case 1:
//            query.serializer().formatQueryAsyncV1(data_query, results, 
//               globals).addCallback(new SendIt()).addErrback(new ErrorCB());
//          break;
//        default: 
//          query_invalid.incrementAndGet();
//          throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
//              "Requested API version not implemented", "Version " + 
//              query.apiVersion() + " is not implemented");
//        }
//        return null;
//      }
//    }
//    
//    /**
//     * Callback executed after we have resolved the metric, tag names and tag
//     * values to their respective UIDs. This callback then runs the actual 
//     * queries and fetches their results.
//     */
//    class BuildCB implements Callback<Deferred<Object>, Query[]> {
//      @Override
//      public Deferred<Object> call(final Query[] queries) {
//        final ArrayList<Deferred<DataPoints[]>> deferreds = 
//            new ArrayList<Deferred<DataPoints[]>>(queries.length);
//        for (final Query query : queries) {
//          deferreds.add(query.runAsync());
//        }
//        return Deferred.groupInOrder(deferreds).addCallback(new QueriesCB());
//      }
//    }
//    
//    /** Handles storing the global annotations after fetching them */
//    class GlobalCB implements Callback<Object, List<Annotation>> {
//      public Object call(final List<Annotation> annotations) throws Exception {
//        globals.addAll(annotations);
//        return data_query.buildQueriesAsync(tsdb).addCallback(new BuildCB());
//      }
//    }
// 
//    // if we the caller wants to search for global annotations, fire that off 
//    // first then scan for the notes, then pass everything off to the formatter
//    // when complete
//    if (!data_query.getNoAnnotations() && data_query.getGlobalAnnotations()) {
//      Annotation.getGlobalAnnotations(tsdb, 
//        data_query.startTime() / 1000, data_query.endTime() / 1000)
//          .addCallback(new GlobalCB()).addErrback(new ErrorCB());
//    } else {
//      data_query.buildQueriesAsync(tsdb).addCallback(new BuildCB())
//        .addErrback(new ErrorCB());
//    }
//  }
//  
//  /**
//   * Handles an expression query
//   * @param tsdb The TSDB to which we belong
//   * @param query The HTTP query to parse/respond
//   * @since 2.3
//   */
//  private void handleExpressionQuery(final TSDB tsdb, final HttpQuery query) {
//    final net.opentsdb.query.pojo.Query v2_query = 
//        JSON.parseToObject(query.getContent(), net.opentsdb.query.pojo.Query.class);
//    v2_query.validate();
//    final QueryExecutor executor = new QueryExecutor(tsdb, v2_query);
//    executor.execute(query);
//  }
//  
//  /**
//   * Processes a last data point query
//   * @param tsdb The TSDB to which we belong
//   * @param query The HTTP query to parse/respond
//   */
//  private void handleLastDataPointQuery(final TSDB tsdb, final HttpQuery query) {
//    
//    final LastPointQuery data_query;
//    if (query.method() == HttpMethod.POST) {
//      switch (query.apiVersion()) {
//      case 0:
//      case 1:
//        data_query = query.serializer().parseLastPointQueryV1();
//        break;
//      default: 
//        throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
//            "Requested API version not implemented", "Version " + 
//            query.apiVersion() + " is not implemented");
//      }
//    } else {
//      data_query = this.parseLastPointQuery(tsdb, query);
//    }
//    
//    if (data_query.sub_queries == null || data_query.sub_queries.isEmpty()) {
//      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
//          "Missing sub queries");
//    }
//    
//    // a list of deferreds to wait on
//    final ArrayList<Deferred<Object>> calls = new ArrayList<Deferred<Object>>();
//    // final results for serialization
//    final List<IncomingDataPoint> results = new ArrayList<IncomingDataPoint>();
//    
//
//    /**
//     * Called after scanning the tsdb-meta table for TSUIDs that match the given
//     * metric and/or tags. If matches were found, it fires off a number of
//     * getLastPoint requests, adding the deferreds to the calls list
//     */
//    final class TSUIDQueryCB implements Callback<Deferred<Object>, ByteMap<Long>> {
//      public Deferred<Object> call(final ByteMap<Long> tsuids) throws Exception {
//        if (tsuids == null || tsuids.isEmpty()) {
//          return null;
//        }
//        final ArrayList<Deferred<IncomingDataPoint>> deferreds =
//            new ArrayList<Deferred<IncomingDataPoint>>(tsuids.size());
//        for (Map.Entry<byte[], Long> entry : tsuids.entrySet()) {
//          deferreds.add(TSUIDQuery.getLastPoint(tsdb, entry.getKey(), 
//              data_query.getResolveNames(), data_query.getBackScan(), 
//              entry.getValue()));
//        }
//        return Deferred.group(deferreds).addCallbackDeferring(new FetchCB());
//      }
//      @Override
//      public String toString() {
//        return "TSMeta scan CB";
//      }
//    }

//    /**
//     * Used to wait on the list of data point deferreds. Once they're all done
//     * this will return the results to the call via the serializer
//     */
//    final class FinalCB implements Callback<Object, ArrayList<Object>> {
//      public Object call(final ArrayList<Object> done) throws Exception {
//        query.sendReply(query.serializer().formatLastPointQueryV1(results));
//        return null;
//      }
//      @Override
//      public String toString() {
//        return "Final CB";
//      }
//    }
//    
//    try {   
//      // start executing the queries
//      for (final LastPointSubQuery sub_query : data_query.getQueries()) {
//        final ArrayList<Deferred<IncomingDataPoint>> deferreds =
//            new ArrayList<Deferred<IncomingDataPoint>>();
//        // TSUID queries take precedence so if there are any TSUIDs listed, 
//        // process the TSUIDs and ignore the metric/tags
//        if (sub_query.getTSUIDs() != null && !sub_query.getTSUIDs().isEmpty()) {
//          for (final String tsuid : sub_query.getTSUIDs()) {
//            final TSUIDQuery tsuid_query = new TSUIDQuery(tsdb, 
//                UniqueId.stringToUid(tsuid));
//            deferreds.add(tsuid_query.getLastPoint(data_query.getResolveNames(), 
//                data_query.getBackScan()));
//          }
//        } else {
//          @SuppressWarnings("unchecked")
//          final TSUIDQuery tsuid_query = 
//              new TSUIDQuery(tsdb, sub_query.getMetric(), 
//                  sub_query.getTags() != null ? 
//                      sub_query.getTags() : Collections.EMPTY_MAP);
//          if (data_query.getBackScan() > 0) {
//            deferreds.add(tsuid_query.getLastPoint(data_query.getResolveNames(), 
//                data_query.getBackScan()));
//          } else {
//            calls.add(tsuid_query.getLastWriteTimes()
//                .addCallbackDeferring(new TSUIDQueryCB()));
//          }
//        }
//        
//        if (deferreds.size() > 0) {
//          calls.add(Deferred.group(deferreds).addCallbackDeferring(new FetchCB()));
//        }
//      }
//      
//      Deferred.group(calls)
//        .addCallback(new FinalCB())
//        .addErrback(new ErrBack())
//        .joinUninterruptibly();
//      
//    } catch (Exception e) {
//      Throwable ex = e;
//      while (ex.getClass().equals(DeferredGroupException.class)) {
//        if (ex.getCause() == null) {
//          LOG.warn("Unable to get to the root cause of the DGE");
//          break;
//        }
//        ex = ex.getCause();
//      }
//      if (ex instanceof RuntimeException) {
//        throw new BadRequestException(ex);
//      } else {
//        throw new RuntimeException("Shouldn't be here", e);
//      }
//    }
//  }
  
//  /**
//   * Parses a query string legacy style query from the URI
//   * @param tsdb The TSDB we belong to
//   * @param query The HTTP Query for parsing
//   * @return A TSQuery if parsing was successful
//   * @throws BadRequestException if parsing was unsuccessful
//   * @since 2.3
//   */
//  public static TSQuery parseQuery(final TSDB tsdb, final HttpQuery query) {
//    return parseQuery(tsdb, query, null);
//  }
  
  /**
   * Parses a query string legacy style query from the URI
   * @param tsdb The TSDB we belong to
   * @param request The HTTP request for parsing
   * @param expressions A list of parsed expression trees filled from the URI.
   * If this is null, it means any expressions in the URI will be skipped.
   * @return A TSQuery if parsing was successful
   * @throws WebApplicationException if parsing was unsuccessful
   * @since 2.3
   */
  public static TSQuery parseQuery(final DefaultTSDB tsdb, 
                                   final HttpServletRequest request,
                                   final List<Object> expressions) {
    final TSQuery data_query = new TSQuery();
    
    String temp = request.getParameter("start");
    if (Strings.isNullOrEmpty(temp)) {
      throw new WebApplicationException("The 'start' query parameter was "
          + "missing.", Response.Status.BAD_REQUEST);
    }
    data_query.setStart(temp);
    data_query.setEnd(request.getParameter("end"));
    
    if (!Strings.isNullOrEmpty(request.getParameter("padding"))) {
      data_query.setPadding(true);
    }
    
    if (!Strings.isNullOrEmpty(request.getParameter("no_annotations"))) {
      data_query.setNoAnnotations(true);
    }
    
    if (!Strings.isNullOrEmpty(request.getParameter("global_annotations"))) {
      data_query.setGlobalAnnotations(true);
    }
    
    if (!Strings.isNullOrEmpty(request.getParameter("show_tsuids"))) {
      data_query.setShowTSUIDs(true);
    }
    
    if (!Strings.isNullOrEmpty(request.getParameter("ms"))) {
      data_query.setMsResolution(true);
    }
    
    if (!Strings.isNullOrEmpty(request.getParameter("show_query"))) {
      data_query.setShowQuery(true);
    }  
    
    if (!Strings.isNullOrEmpty(request.getParameter("show_stats"))) {
      data_query.setShowStats(true);
    }    
    
    if (!Strings.isNullOrEmpty(request.getParameter("show_summary"))) {
        data_query.setShowSummary(true);
    }
    
    // handle tsuid queries first
    final String[] tsuids = request.getParameterMap().get("tsuids");
    if (tsuids != null) {
      for (String q : tsuids) {
        parseTsuidTypeSubQuery(q, data_query);
      }
    }
    
    final String[] m_queries = request.getParameterMap().get("m");
    if (m_queries != null) {
      for (String q : m_queries) {
        parseMTypeSubQuery(q, data_query);
      }
    }
    
    // TODO - testing out the graphite style expressions here with the "exp" 
    // param that could stand for experimental or expression ;)
//    if (expressions != null) {
//      if (query.hasQueryStringParam("exp")) {
//        final List<String> uri_expressions = query.getQueryStringParams("exp");
//        final List<String> metric_queries = new ArrayList<String>(
//            uri_expressions.size());
//        // parse the expressions into their trees. If one or more expressions 
//        // are improper then it will toss an exception up
//        expressions.addAll(Expressions.parseExpressions(
//            uri_expressions, data_query, metric_queries));
//        // iterate over each of the parsed metric queries and store it in the
//        // TSQuery list so that we fetch the data for them.
//        for (final String mq: metric_queries) {
//          parseMTypeSubQuery(mq, data_query);
//        }
//      }
//    } else {
//      if (LOG.isDebugEnabled()) {
//        LOG.debug("Received a request with an expression but at the "
//            + "wrong endpoint: " + query);
//      }
//    }
    
    if (data_query.getQueries() == null || data_query.getQueries().size() < 1) {
      throw new WebApplicationException("Missing sub queries", 
          Response.Status.BAD_REQUEST);
    }

    // Filter out duplicate queries
    final Set<TSSubQuery> query_set = Sets.newHashSet(data_query.getQueries());
    data_query.getQueries().clear();
    data_query.getQueries().addAll(query_set);

    return data_query;
  }
  
  /**
   * Parses a query string "m=..." type query and adds it to the TSQuery.
   * This will generate a TSSubQuery and add it to the TSQuery if successful
   * @param query_string The value of the m query string parameter, i.e. what
   * comes after the equals sign
   * @param data_query The query we're building
   * @throws WebApplicationException if we are unable to parse the query or it is
   * missing components
   */
  private static void parseMTypeSubQuery(final String query_string, 
                                         final TSQuery data_query) {
    if (query_string == null || query_string.isEmpty()) {
      throw new WebApplicationException("The query string was empty",
          Response.Status.BAD_REQUEST);
    }
    
    // m is of the following forms:
    // agg:[interval-agg:][rate:]metric[{tag=value,...}]
    // where the parts in square brackets `[' .. `]' are optional.
    final String[] parts = StringUtils.splitString(query_string, ':');
    int i = parts.length;
    if (i < 2 || i > 5) {
      throw new WebApplicationException("Invalid parameter m=" + query_string + " ("
          + (i < 2 ? "not enough" : "too many") + " :-separated parts)",
          Response.Status.BAD_REQUEST);
    }
    final TSSubQuery sub_query = new TSSubQuery();
    
    // the aggregator is first
    sub_query.setAggregator(parts[0]);
    
    i--; // Move to the last part (the metric name).
    List<TagVFilter> filters = new ArrayList<TagVFilter>();
    sub_query.setMetric(Tags.parseWithMetricAndFilters(parts[i], filters));
    sub_query.setFilters(filters);
    
    // parse out the rate and downsampler 
    for (int x = 1; x < parts.length - 1; x++) {
      if (parts[x].toLowerCase().startsWith("rate")) {
        sub_query.setRate(true);
        if (parts[x].indexOf("{") >= 0) {
          sub_query.setRateOptions(QueryRpc.parseRateOptions(true, parts[x]));
        }
      } else if (Character.isDigit(parts[x].charAt(0))) {
        sub_query.setDownsample(parts[x]);
      } else if (parts[x].equalsIgnoreCase("pre-agg")) {
        sub_query.setPreAggregate(true);
      } /*else if (parts[x].toLowerCase().startsWith("rollup_")) {
TODO - restore!
        sub_query.setRollupUsage(parts[x]);
      } */else if (parts[x].toLowerCase().startsWith("explicit_tags")) {
        sub_query.setExplicitTags(true);
      }
    }
    
    if (data_query.getQueries() == null) {
      final ArrayList<TSSubQuery> subs = new ArrayList<TSSubQuery>(1);
      data_query.setQueries(subs);
    }
    data_query.getQueries().add(sub_query);
  }
  
  /**
   * Parses a "tsuid=..." type query and adds it to the TSQuery.
   * This will generate a TSSubQuery and add it to the TSQuery if successful
   * @param query_string The value of the m query string parameter, i.e. what
   * comes after the equals sign
   * @param data_query The query we're building
   * @throws WebApplicationException if we are unable to parse the query or it is
   * missing components
   */
  private static void parseTsuidTypeSubQuery(final String query_string, 
                                             final TSQuery data_query) {
    if (query_string == null || query_string.isEmpty()) {
      throw new WebApplicationException("The tsuid query string was empty.",
          Response.Status.BAD_REQUEST);
    }
    
    // tsuid queries are of the following forms:
    // agg:[interval-agg:][rate:]tsuid[,s]
    // where the parts in square brackets `[' .. `]' are optional.
    final String[] parts = StringUtils.splitString(query_string, ':');
    int i = parts.length;
    if (i < 2 || i > 5) {
      throw new WebApplicationException("Invalid parameter m=" + query_string + " ("
          + (i < 2 ? "not enough" : "too many") + " :-separated parts)",
          Response.Status.BAD_REQUEST);
    }
    
    final TSSubQuery sub_query = new TSSubQuery();
    
    // the aggregator is first
    sub_query.setAggregator(parts[0]);
    
    i--; // Move to the last part (the metric name).
    final List<String> tsuid_array = Arrays.asList(parts[i].split(","));
    sub_query.setTsuids(tsuid_array);
    
    // parse out the rate and downsampler 
    for (int x = 1; x < parts.length - 1; x++) {
      if (parts[x].toLowerCase().startsWith("rate")) {
        sub_query.setRate(true);
        if (parts[x].indexOf("{") >= 0) {
          sub_query.setRateOptions(QueryRpc.parseRateOptions(true, parts[x]));
        }
      } else if (Character.isDigit(parts[x].charAt(0))) {
        sub_query.setDownsample(parts[x]);
      }
    }
    
    if (data_query.getQueries() == null) {
      final ArrayList<TSSubQuery> subs = new ArrayList<TSSubQuery>(1);
      data_query.setQueries(subs);
    }
    data_query.getQueries().add(sub_query);
  }
  
  /**
   * Parses the "rate" section of the query string and returns an instance
   * of the RateOptions class that contains the values found.
   * <p>
   * The format of the rate specification is rate[{counter[,#[,#]]}].
   * @param rate If true, then the query is set as a rate query and the rate
   * specification will be parsed. If false, a default RateOptions instance
   * will be returned and largely ignored by the rest of the processing
   * @param spec The part of the query string that pertains to the rate
   * @return An initialized RateOptions instance based on the specification
   * @throws WebApplicationException if the parameter is malformed
   * @since 2.0
   */
   static final public RateOptions parseRateOptions(final boolean rate,
       final String spec) {
     if (!rate || spec.length() == 4) {
       return RateOptions.newBuilder()
           .setCounter(false)
           .setCounterMax(Long.MAX_VALUE)
           .setResetValue(RateOptions.DEFAULT_RESET_VALUE)
           .build();
     }

     if (spec.length() < 6) {
       throw new WebApplicationException("Invalid rate options specification: "
           + spec, Response.Status.BAD_REQUEST);
     }

     String[] parts = StringUtils
         .splitString(spec.substring(5, spec.length() - 1), ',');
     if (parts.length < 1 || parts.length > 3) {
       throw new WebApplicationException(
           "Incorrect number of values in rate options specification, must be " +
           "counter[,counter max value,reset value], recieved: "
               + parts.length + " parts",
           Response.Status.BAD_REQUEST);
     }

     final boolean counter = parts[0].endsWith("counter");
     try {
       final long max = (parts.length >= 2 && parts[1].length() > 0 ? Long
           .parseLong(parts[1]) : Long.MAX_VALUE);
       try {
         final long reset = (parts.length >= 3 && parts[2].length() > 0 ? Long
             .parseLong(parts[2]) : RateOptions.DEFAULT_RESET_VALUE);
         final boolean drop_counter = parts[0].equals("dropcounter");
         return RateOptions.newBuilder()
             .setCounter(counter)
             .setCounterMax(max)
             .setResetValue(reset)
             .setDropResets(drop_counter)
             .build();
       } catch (NumberFormatException e) {
         throw new WebApplicationException(
             "Reset value of counter was not a number, received '" + parts[2]
                 + "'",
             Response.Status.BAD_REQUEST);
       }
     } catch (NumberFormatException e) {
       throw new WebApplicationException(
           "Max value of counter was not a number, received '" + parts[1] + "'",
           Response.Status.BAD_REQUEST);
     }
   }

//  /**
//   * Parses a last point query from the URI string
//   * @param tsdb The TSDB to which we belong
//   * @param http_query The HTTP query to work with
//   * @return A LastPointQuery object to execute against
//   * @throws BadRequestException if parsing failed
//   */
//  private LastPointQuery parseLastPointQuery(final TSDB tsdb, 
//      final HttpQuery http_query) {
//    final LastPointQuery query = new LastPointQuery();
//    
//    if (http_query.hasQueryStringParam("resolve")) {
//      query.setResolveNames(true);
//    }
//    
//    if (http_query.hasQueryStringParam("back_scan")) {
//      try {
//        query.setBackScan(Integer.parseInt(http_query.getQueryStringParam("back_scan")));
//      } catch (NumberFormatException e) {
//        throw new BadRequestException("Unable to parse back_scan parameter");
//      }
//    }
//    
//    final List<String> ts_queries = http_query.getQueryStringParams("timeseries");
//    final List<String> tsuid_queries = http_query.getQueryStringParams("tsuids");
//    final int num_queries = 
//      (ts_queries != null ? ts_queries.size() : 0) +
//      (tsuid_queries != null ? tsuid_queries.size() : 0);
//    final List<LastPointSubQuery> sub_queries = 
//      new ArrayList<LastPointSubQuery>(num_queries);
//    
//    if (ts_queries != null) {
//      for (String ts_query : ts_queries) {
//        sub_queries.add(LastPointSubQuery.parseTimeSeriesQuery(ts_query));
//      }
//    }
//    
//    if (tsuid_queries != null) {
//      for (String tsuid_query : tsuid_queries) {
//        sub_queries.add(LastPointSubQuery.parseTSUIDQuery(tsuid_query));
//      }
//    }
//    
//    query.setQueries(sub_queries);
//    return query;
//  }
  
  /** @param collector Populates the collector with statistics */
  public void collectStats(final StatsCollector collector) {
    collector.record("http.query.invalid_requests", query_invalid);
    collector.record("http.query.exceptions", query_exceptions);
    collector.record("http.query.success", query_success);
  }
  
//  public static class LastPointQuery {
//    
//    private boolean resolve_names;
//    private int back_scan;
//    private List<LastPointSubQuery> sub_queries;
//    
//    /**
//     * Default Constructor necessary for de/serialization
//     */
//    public LastPointQuery() {
//      
//    }
//    
//    /** @return Whether or not to resolve the UIDs to names */
//    public boolean getResolveNames() {
//      return resolve_names;
//    }
//    
//    /** @return Number of hours to scan back in time looking for data */
//    public int getBackScan() {
//      return back_scan;
//    }
//    
//    /** @return A list of sub queries */
//    public List<LastPointSubQuery> getQueries() {
//      return sub_queries;
//    }
//    
//    /** @param resolve_names Whether or not to resolve the UIDs to names */
//    public void setResolveNames(final boolean resolve_names) {
//      this.resolve_names = resolve_names;
//    }
//    
//    /** @param back_scan Number of hours to scan back in time looking for data */
//    public void setBackScan(final int back_scan) {
//      this.back_scan = back_scan;
//    }
//    
//    /** @param queries A list of sub queries to execute */
//    public void setQueries(final List<LastPointSubQuery> queries) {
//      this.sub_queries = queries;
//    }
//  }
//  
//  public static class LastPointSubQuery {
//    
//    private String metric;
//    private HashMap<String, String> tags;
//    private List<String> tsuids;
//    
//    /**
//     * Default constructor necessary for de/serialization
//     */
//    public LastPointSubQuery() {
//      
//    }
//    
//    public static LastPointSubQuery parseTimeSeriesQuery(final String query) {
//      final LastPointSubQuery sub_query = new LastPointSubQuery();
//      sub_query.tags = new HashMap<String, String>();
//      sub_query.metric = Tags.parseWithMetric(query, sub_query.tags);
//      return sub_query;
//    }
//    
//    public static LastPointSubQuery parseTSUIDQuery(final String query) {
//      final LastPointSubQuery sub_query = new LastPointSubQuery();
//      final String[] tsuids = query.split(",");
//      sub_query.tsuids = new ArrayList<String>(tsuids.length);
//      for (String tsuid : tsuids) {
//        sub_query.tsuids.add(tsuid);
//      }
//      return sub_query;
//    }
//    
//    /** @return The name of the metric to search for */
//    public String getMetric() {
//      return metric;
//    }
//    
//    /** @return A map of tag names and values */
//    public Map<String, String> getTags() {
//      return tags;
//    }
//    
//    /** @return A list of TSUIDs to get the last point for */
//    public List<String> getTSUIDs() {
//      return tsuids;
//    }
//    
//    /** @param metric The metric to search for */
//    public void setMetric(final String metric) {
//      this.metric = metric;
//    }
//    
//    /** @param tags A map of tag name/value pairs */
//    public void setTags(final Map<String, String> tags) {
//      this.tags = (HashMap<String, String>) tags;
//    }
//    
//    /** @param tsuids A list of TSUIDs to get data for */
//    public void setTSUIDs(final List<String> tsuids) {
//      this.tsuids = tsuids;
//    }
//  }
}

