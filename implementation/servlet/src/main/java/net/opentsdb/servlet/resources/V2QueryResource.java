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
package net.opentsdb.servlet.resources;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.AsyncContext;
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

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;

import io.opentracing.Span;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import io.opentracing.propagation.TextMapExtractAdapter;
import jersey.repackaged.com.google.common.collect.ImmutableMap;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TsdbPlugin;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.exceptions.RemoteQueryExecutionException;
import net.opentsdb.query.TSQuery;
import net.opentsdb.query.context.DefaultQueryContext;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.HttpQueryV2Executor;
import net.opentsdb.query.execution.QueryExecution;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.execution.serdes.JsonV2QuerySerdes;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.stats.TsdbTracer;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.JSON;

@Path("api/query")
public class V2QueryResource {
  private static final Logger LOG = LoggerFactory.getLogger(
      V2QueryResource.class);
  private static final Logger QUERY_LOG = LoggerFactory.getLogger("QueryLog");
  
  public static final String QUERY_KEY = "TSDQUERY";
  
  public static final String V2_QUERY_KEY = "V2TSDQUERY";
  
  public static final String CONTEXT_KEY = "CONTEXT";
  
  public static final String TRACE_KEY = "TRACE";
  
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response post(
    final @Context ServletConfig servlet_config, 
    final @Context HttpServletRequest request) throws Exception {
    
    final QueryContext context = (QueryContext) request.getAttribute(CONTEXT_KEY);
    final TimeSeriesQuery query = (TimeSeriesQuery) request.getAttribute(QUERY_KEY);
    final TsdbTrace trace;
    if (context != null && context.getTracer() != null) {
      trace = (TsdbTrace) request.getAttribute(TRACE_KEY);
    } else {
      trace = null;
    }
    
    if (request.getAttribute(
        OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE) != null) {
      final Exception e = (Exception) request.getAttribute(
              OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE);
      LOG.info("Completing query=" 
        + JSON.serializeToString(ImmutableMap.<String, Object>builder()
        // TODO - possible upstream headers
        .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
        .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
        .put("traceId", trace != null ? trace.getTraceId() : null)
        .put("status", Response.Status.OK)
        .put("query", request.getAttribute(V2_QUERY_KEY))
        .build()));
      
      QUERY_LOG.info("Completing query=" 
         + JSON.serializeToString(ImmutableMap.<String, Object>builder()
        
        // TODO - possible upstream headers
        .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
        .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
        .put("traceId", trace != null ? trace.getTraceId() : null)
        .put("status", Response.Status.OK)
        .put("trace", trace.serializeToString())
        .put("query", request.getAttribute(V2_QUERY_KEY))
        .build()));
      
      if (trace != null && trace.getFirstSpan() != null) {
        trace.getFirstSpan()
          .setTag("status", "Error")
          .setTag("finalThread", Thread.currentThread().getName())
          .setTag("error", e.getMessage())
          .log("exception", e)
          .finish();
      }
      throw e;
    } else if (request.getAttribute(
        OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE) != null) {
      return handeResponse(servlet_config, request);
    } else {
      return handleQuery(servlet_config, request);
    }
  }
  
  @SuppressWarnings({ "unchecked" })
  @VisibleForTesting
  Response handleQuery(
      final @Context ServletConfig servlet_config, 
      final @Context HttpServletRequest request) throws Exception {
    
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
    
    // initiate the tracer
    final TsdbTrace trace;
    final Span span;
    final TsdbPlugin tracer = tsdb.getRegistry().getDefaultPlugin(TsdbTracer.class);
    if (tracer != null) {
      trace = ((TsdbTracer) tracer).getTracer(true);
      span = trace.tracer().buildSpan(this.getClass().getSimpleName())
          .withTag("endpoint", "/api/query")
          // TODO - more useful info
          .start();
      trace.setFirstSpan(span);
      request.setAttribute(TRACE_KEY, trace);
    } else {
      trace = null;
      span = null;
    }
    
    // parse the query
    final TSQuery ts_query;
    try {
      ts_query = JSON.parseToObject(request.getInputStream(),TSQuery.class);
      // throws an exception if invalid.
      ts_query.validateAndSetQuery();
      request.setAttribute(V2_QUERY_KEY, ts_query);
    } catch (Exception e) {
      throw new QueryExecutionException("Invalid query", 400, e);
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
    
    // start the Async context and pass it around.
    final AsyncContext async = request.startAsync();
    async.setTimeout((Integer) servlet_config.getServletContext()
        .getAttribute(OpenTSDBApplication.ASYNC_TIMEOUT_ATTRIBUTE));
    
    final TimeSeriesQuery query = TSQuery.convertQuery(ts_query);
    if (span != null) {
      span.setTag("queryId", 
          Bytes.byteArrayToString(query.buildHashCode().asBytes()));
    }
    query.validate();
    request.setAttribute(QUERY_KEY, query);
    LOG.info("Executing new query=" + JSON.serializeToString(
        ImmutableMap.<String, Object>builder()
        // TODO - possible upstream headers
        .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
        .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
        .put("traceId", trace != null ? trace.getTraceId() : null)
        .put("query", ts_query)
        .build()));
    
    // setup the context and copy headers for downstream use.
    final QueryContext context = new DefaultQueryContext(tsdb, 
        tsdb.getRegistry().getDefaultExecutionGraph(), 
        trace == null ? null : trace.tracer());
    context.addSessionObject(HttpQueryV2Executor.SESSION_HEADERS_KEY, 
        headers_copy);
    request.setAttribute(CONTEXT_KEY, context);
    
    obj = context.executionGraph().sinkExecutor();
    if (obj == null) {
      throw new WebApplicationException("Execution graph returned a null "
          + "sink executor: " + tsdb.getRegistry().getDefaultExecutionGraph());
    }
    final QueryExecutor<IteratorGroups> executor =
        (QueryExecutor<IteratorGroups>) obj;
    final QueryExecution<IteratorGroups> execution = 
        executor.executeQuery(context, query, span);
    
    /** Class called when the query has completed without an exception. Stashes
     * the results in the request attributes and calls the async dispatch. */
    class SuccessCB implements Callback<Object, IteratorGroups> {
      @Override
      public Object call(final IteratorGroups groups) throws Exception {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Successful response for query=" 
              + JSON.serializeToString(
                  ImmutableMap.<String, Object>builder()
                  // TODO - possible upstream headers
                  .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
                  .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
                  .put("traceId", trace != null ? trace.getTraceId() : null)
                  .put("query", ts_query)
                  .build()));
        }
        request.setAttribute(OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE, groups);
        try {
          async.dispatch();
        } catch (Exception e) {
          LOG.error("Unexpected exception dispatching async request for "
              + "query: " + ts_query, e);
        }
        return null;
      }
    }
    
    /** Exception handler, unwraps and stashes in the request attributes for 
     * serialization. */
    class ErrorCB implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception ex) throws Exception {
        LOG.error("Exception for query: " 
            + Bytes.byteArrayToString(query.buildHashCode().asBytes()), ex);
        request.setAttribute(OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE, ex);
        async.dispatch();
        return null;
      }
    }
    
    execution.deferred()
      .addCallback(new SuccessCB())
      .addErrback(new ErrorCB());
    return null;
  }

  Response handeResponse(
      final @Context ServletConfig servlet_config, 
      final @Context HttpServletRequest request) {
    final IteratorGroups groups = (IteratorGroups) request.getAttribute(
        OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE);
    final QueryContext context = (QueryContext) request.getAttribute(CONTEXT_KEY);
    final TimeSeriesQuery query = (TimeSeriesQuery) request.getAttribute(QUERY_KEY);
    final Span serdes_span;
    final TsdbTrace trace;
    if (context.getTracer() != null) {
      trace = (TsdbTrace) request.getAttribute(TRACE_KEY);
      serdes_span = context.getTracer().buildSpan("serialization")
          .asChildOf(trace.getFirstSpan())
          .start();
    } else {
      serdes_span = null;
      trace = null;
    }

    /** The stream to write to. */
    final StreamingOutput stream = new StreamingOutput() {

      @Override
      public void write(OutputStream output)
          throws IOException, WebApplicationException {
        final JsonGenerator json = JSON.getFactory().createGenerator(output);
        json.writeStartArray();
        
        final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(json);
        serdes.serialize(query, output, groups);
        
        json.writeObjectFieldStart("stats");
        if (trace != null) {
          trace.serializeJSON("trace", json);
        }
        json.writeEndObject();
        
        json.writeEndArray();
        json.close();
        // TODO - trace, other bits.
        if (serdes_span != null) {
          serdes_span.finish();
        }
      }
    };
    
    LOG.info("Completing query=" 
      + JSON.serializeToString(ImmutableMap.<String, Object>builder()
      // TODO - possible upstream headers
      .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
      .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
      .put("traceId", trace != null ? trace.getTraceId() : null)
      .put("status", Response.Status.OK)
      .put("query", request.getAttribute(V2_QUERY_KEY))
      .build()));
    
    QUERY_LOG.info("Completing query=" 
      + JSON.serializeToString(ImmutableMap.<String, Object>builder()
      // TODO - possible upstream headers
      .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
      .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
      .put("traceId", trace != null ? trace.getTraceId() : null)
      .put("status", Response.Status.OK)
      .put("trace", trace.serializeToString())
      .put("query", request.getAttribute(V2_QUERY_KEY))
      .build()));
    
    if (trace != null && trace.getFirstSpan() != null) {
      trace.getFirstSpan()
        .setTag("status", "OK")
        .setTag("finalThread", Thread.currentThread().getName())
        .finish();
    }
    
    return Response.ok().entity(stream)
        .type(MediaType.APPLICATION_JSON)
        .build();
    // all done!
  }
}
