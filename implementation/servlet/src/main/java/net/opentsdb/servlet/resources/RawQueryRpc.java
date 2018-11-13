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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

import net.opentsdb.auth.AuthState;
import net.opentsdb.auth.Authentication;
import net.opentsdb.auth.AuthState.AuthStatus;
import net.opentsdb.core.TSDB;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.query.execution.serdes.JsonV2QuerySerdesOptions;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.servlet.exceptions.GenericExceptionMapper;
import net.opentsdb.servlet.filter.AuthFilter;
import net.opentsdb.servlet.sinks.ServletSinkConfig;
import net.opentsdb.servlet.sinks.ServletSinkFactory;
import net.opentsdb.stats.DefaultQueryStats;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.Trace;
import net.opentsdb.stats.Tracer;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.YAML;

@Path("api/query/graph")
public class RawQueryRpc {
  private static final Logger LOG = LoggerFactory.getLogger(RawQueryRpc.class);
  
  /** Request key used for the V3 TSDB query. */
  public static final String QUERY_KEY = "TSDQUERY";
  
  /** Request key for the query context. */
  public static final String CONTEXT_KEY = "CONTEXT";
  
  /** Request key for the tracer. */
  public static final String TRACE_KEY = "TRACE";
  
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
      tsdb.getStatsCollector().incrementCounter("query.new", "endpoint", "3x");
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
    
    final AsyncContext async = request.startAsync();
    async.setTimeout((Integer) servlet_config.getServletContext()
        .getAttribute(OpenTSDBApplication.ASYNC_TIMEOUT_ATTRIBUTE));
    
    final SemanticQuery query = query_builder.build();
    LOG.info("Executing new query=" + JSON.serializeToString(
        ImmutableMap.<String, Object>builder()
        // TODO - possible upstream headers
        .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
        //.put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
        .put("traceId", trace != null ? trace.traceId() : "")
        .put("query", JSON.serializeToString(query))
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
            //.setResponse(response)
            .setRequest(request)
            .setAsync(async)
            .build())
        .build();
    
    class AsyncTimeout implements AsyncListener {

      @Override
      public void onComplete(final AsyncEvent event) throws IOException {
        // no-op
      }

      @Override
      public void onTimeout(final AsyncEvent event) throws IOException {
        LOG.error("The query has timed out");
        try {
          context.close();
        } catch (Exception e) {
          LOG.error("Failed to close the query: ", e);
        }
        GenericExceptionMapper.serialize(
            new QueryExecutionException("The query has exceeded "
            + "the timeout limit.", 504), event.getSuppliedResponse());
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
    
    try {
      context.initialize(query_span).join();
      context.fetchNext(query_span);
    } catch (Throwable t) {
      LOG.error("Unexpected exception triggering query.", t);
//      if (execute_span != null) {
//        execute_span.setErrorTags(e)
//                    .finish();
//      }
      //GenericExceptionMapper.serialize(t, response);

      async.complete();
      if (query_span != null) {
        query_span.setErrorTags(t)
                   .finish();
      }
      throw t;
    }
    
    return null;
  }
  
}
