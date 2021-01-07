// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
import java.io.InputStreamReader;
import java.time.temporal.ChronoUnit;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import net.opentsdb.auth.AuthState;
import net.opentsdb.auth.Authentication;
import net.opentsdb.auth.AuthState.AuthStatus;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.prometheus.PromQLParser;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.query.execution.prometheus.PrometheusQuerySerdesFactory;
import net.opentsdb.query.execution.prometheus.PrometheusQuerySerdesOptions;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.servlet.filter.AuthFilter;
import net.opentsdb.servlet.sinks.ServletSinkConfig;
import net.opentsdb.servlet.sinks.ServletSinkFactory;
import net.opentsdb.stats.DefaultQueryStats;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.Trace;
import net.opentsdb.stats.Tracer;
import net.opentsdb.stats.StatsCollector.StatsTimer;
import net.opentsdb.threadpools.TSDTask;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * A resource to handle PromQL queries.
 * 
 * TODO - make sure we can pick different serializers.
 * 
 * TODO - support instants, have to parse out "time"
 *
 * @since 3.0
 */
@Path("query/promql")
public class PromQLResource extends BaseTSDBPlugin implements ServletResource {
  private static final Logger LOG = LoggerFactory.getLogger(PromQLResource.class);
  
  public static final String TYPE = PromQLResource.class.getSimpleName().toString();

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response get(final @Context ServletConfig servlet_config, 
                      final @Context HttpServletRequest request) throws Exception {
    final Object stream = request.getAttribute("DATA");
    if (stream != null) {
      return Response.ok()
          .entity(((ByteArrayOutputStream) stream).toByteArray())
          .header("Content-Type", "application/json")
          .build();
    }
    
    return handleQuery(servlet_config, request);
  }
  
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response post(final @Context ServletConfig servlet_config, 
                       final @Context HttpServletRequest request) throws Exception {
    final Object stream = request.getAttribute("DATA");
    if (stream != null) {
      return Response.ok()
          .entity(((ByteArrayOutputStream) stream).toByteArray())
          .header("Content-Type", "application/json")
          .build();
    }
    
    return handleQuery(servlet_config, request);
  }
  
  private Response handleQuery(final ServletConfig servlet_config, 
                               final HttpServletRequest request)
      throws IOException, InterruptedException, ExecutionException {
    
    final String promql;
    String temp = request.getHeader("query"); 
    if (!Strings.isNullOrEmpty(temp)) {
      promql = temp;
    } else {
      final StringBuilder buffer = new StringBuilder();
      final char[] char_buf = new char[1024];
      try (InputStreamReader reader = new InputStreamReader(request.getInputStream())) {
        int read = 0;
        while (read >= 0) {
          read = reader.read(char_buf);
          if (read < 0) {
            break;
          }
          if (read == char_buf.length) {
            buffer.append(char_buf);
          } else {
            for (int i = 0; i < read; i++) {
              buffer.append(char_buf[i]);
            }
          }
        }
      }
      promql = buffer.toString();
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
        throw new QueryExecutionException("Authentication failed.", 403);
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
    
    // extract query params
    final String start = request.getHeader("start");
    final String end = request.getHeader("end");
    final String step = request.getHeader("step");
    final String timeout = request.getHeader("timeout");

    final SemanticQuery query = new PromQLParser(promql,
                                                 start,
                                                 end,
                                                 step)
                 .parse()
                 .build();
    LOG.info("Executing new PromQL query=" + JSON.serializeToString(
        ImmutableMap.<String, Object>builder()
        .put("headers", log_headers == null ? "null" : log_headers)
        .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
        .put("traceId", trace != null ? trace.traceId() : "")
        .put("user", auth_state != null ? auth_state.getUser() : "Unkown")
        .put("remote", request.getRemoteAddr())
        .put("promQL", promql)
        .put("query", query)
        .build()));
    
    SerdesOptions serdes = query.getSerdesConfigs().isEmpty() ? null :
      query.getSerdesConfigs().get(0);
    if (serdes == null) {
      serdes = PrometheusQuerySerdesOptions.newBuilder()
          .setId(PrometheusQuerySerdesFactory.TYPE)
          .build();
    }
    
    long query_timeout = (long) servlet_config.getServletContext()
        .getAttribute(OpenTSDBApplication.ASYNC_TIMEOUT_ATTRIBUTE);
    final AsyncContext async = request.startAsync();
    if (!Strings.isNullOrEmpty(timeout)) {
      try {
        final long parsed = DateTime.parseDuration(timeout);
        if (parsed > query_timeout) {
          LOG.debug("Skipping given timeout of " + parsed + " as it's greater "
              + "than the configured limit of " + query_timeout);
        } else {
          query_timeout = parsed;
        }
      } catch (Exception e) {
        LOG.error("Failed to parse timeout [" + timeout + "] so using the "
            + "default of " + query_timeout, e);
      }
    }
    async.setTimeout(query_timeout);
    
    SemanticQueryContext context = (SemanticQueryContext) SemanticQueryContext.newBuilder()
        .setTSDB(tsdb)
        .setQuery(query)
        .setStats(DefaultQueryStats.newBuilder()
            .setTrace(trace)
            .setQuerySpan(query_span)
            .build())
        .setAuthState(auth_state)
        .setHeaders(log_headers)
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
    
    tsdb.getQueryThreadPool().submit(runTsdQuery, context, TSDTask.QUERY);
    return null;
  }
  
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
      AsyncRunner.asyncRun(null, null, query_span, async, context, PromQLResource.class);
    }
  }
  
  @Override
  public String type() {
    return TYPE;
  }

}
