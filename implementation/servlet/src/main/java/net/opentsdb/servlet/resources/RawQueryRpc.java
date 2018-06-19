package net.opentsdb.servlet.resources;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

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
import javax.ws.rs.core.StreamingOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.stumbleupon.async.Callback;

import jersey.repackaged.com.google.common.collect.ImmutableMap;
import net.opentsdb.auth.AuthState;
import net.opentsdb.auth.Authentication;
import net.opentsdb.auth.AuthState.AuthStatus;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.query.TSQuery;
import net.opentsdb.query.execution.serdes.JsonV2QuerySerdes;
import net.opentsdb.query.execution.serdes.JsonV2QuerySerdesOptions;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.serdes.SerdesFactory;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.servlet.filter.AuthFilter;
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
  public Response post(final @Context ServletConfig servlet_config, 
                   final @Context HttpServletRequest request) throws Exception {
    if (request.getAttribute(OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE) != null) {
      return handleException(request);
    } else if (request.getAttribute(
        OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE) != null) {
      return handeResponse(servlet_config, request);
    } else {
      return handleQuery(servlet_config, request);
    }
  }
  
  @VisibleForTesting
  Response handleQuery(final ServletConfig servlet_config, 
                       final HttpServletRequest request) throws Exception {
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
          .withTag("endpoint", "/api/query/graph")
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
      parse_span.setTag("Status", "OK")
                .setTag("finalThread", Thread.currentThread().getName())
                .finish();
    }
    
    final AsyncContext async = request.startAsync();
    async.setTimeout((Integer) servlet_config.getServletContext()
        .getAttribute(OpenTSDBApplication.ASYNC_TIMEOUT_ATTRIBUTE));

    class LocalSink implements QuerySink {
      
      @Override
      public void onComplete() {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Yay, all done!");
        }
      }

      @Override
      public void onNext(QueryResult next) {
        if (LOG.isDebugEnabled()) {
//          LOG.debug("Successful response for query=" 
//              + JSON.serializeToString(
//                  ImmutableMap.<String, Object>builder()
//                  // TODO - possible upstream headers
//                  .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
//                  .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
//                  .put("traceId", trace != null ? trace.traceId() : "")
//                  .put("query", ts_query)
//                  .build()));
        }
        request.setAttribute(OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE, next);
        try {
          async.dispatch();
        } catch (Exception e) {
          LOG.error("Unexpected exception dispatching async request for "
              + "query: " + query_builder.build(), e);
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
    
    query_builder.addSink(new LocalSink());
    final SemanticQuery query = query_builder.build();
    request.setAttribute(QUERY_KEY, query);
//    LOG.info("Executing new query=" + JSON.serializeToString(
//        ImmutableMap.<String, Object>builder()
//        // TODO - possible upstream headers
//        .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
//        //.put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
//        .put("traceId", trace != null ? trace.traceId() : "")
//        .put("query", query)
//        .build()));
    Span setup_span = null;
    if (query_span != null) {
      setup_span = trace.newSpanWithThread("setupContext")
          .withTag("startThread", Thread.currentThread().getName())
          .asChildOf(query_span)
          .start();
    }
    
    SemanticQueryContext context = (SemanticQueryContext) SemanticQueryContext.newBuilder()
        .setTSDB(tsdb)
        .setQuery(query)
        .build();
    
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
    
    request.setAttribute(CONTEXT_KEY, context);
    
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
    
    return null;
  }
  
  Response handeResponse(final ServletConfig servlet_config, 
                         final HttpServletRequest request) {
    final QueryResult result = (QueryResult) request.getAttribute(
        OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE);
    final SemanticQuery query = (SemanticQuery) request.getAttribute(QUERY_KEY);
    final QueryContext context = (QueryContext) request.getAttribute(CONTEXT_KEY);
    
    final SerdesFactory factory = result.source().pipelineContext().tsdb()
        .getRegistry().getDefaultPlugin(SerdesFactory.class);
    if (factory == null) {
      throw new IllegalStateException("NO Default serdes!");
    }
    TimeSeriesSerdes serdes = factory.newInstance();
    //TimeSeriesSerdes serdes = new JsonV2QuerySerdes();

    // TODO - proper sources
    final Collection<TimeSeriesDataSource> sources = 
        result.source().pipelineContext().downstreamSources(result.source());
    final QuerySourceConfig source_config = (QuerySourceConfig) sources.iterator().next().config();
    
    //final SerdesOptions options = query.getSerdesOptions().get(0);
    final SerdesOptions options = JsonV2QuerySerdesOptions.newBuilder()
//        .setMsResolution(ts_query.getMsResolution())
//        .setShowQuery(ts_query.getShowQuery())
//        .setShowStats(ts_query.getShowStats())
//        .setShowSummary(ts_query.getShowSummary())
        .setStart(source_config.startTime())
        .setEnd(source_config.endTime())
        .setId("serdes")
        .build();
    
    final StreamingOutput stream = new StreamingOutput() {
      @Override
      public void write(final OutputStream output)
          throws IOException, WebApplicationException {
        Span serdes_span = null;
//        if (response_span != null) {
//          serdes_span = context.stats().trace().newSpanWithThread("serdes")
//              .withTag("startThread", Thread.currentThread().getName())
//              .asChildOf(response_span)
//              .start();
//        }

        final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes();
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

        // TODO - trace, other bits.
        if (serdes_span != null) {
          serdes_span.setTag("finalThread", Thread.currentThread().getName())
                     .setTag("status", "OK")
                     .finish();
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
        
        tsdb.getStatsCollector().incrementCounter("query.success", "endpoint", "2x");
//        query_success.incrementAndGet();
//        LOG.info("Completing query=" 
//            + JSON.serializeToString(ImmutableMap.<String, Object>builder()
//            // TODO - possible upstream headers
//            .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
//            .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
//            //.put("traceId", trace != null ? trace.getTraceId() : null)
//            .put("status", Response.Status.OK)
//            .put("query", request.getAttribute(V2_QUERY_KEY))
//            .build()));
//          
//        QUERY_LOG.info("Completing query=" 
//            + JSON.serializeToString(ImmutableMap.<String, Object>builder()
//            // TODO - possible upstream headers
//            .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
//            .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
//            //.put("traceId", trace != null ? trace.getTraceId() : null)
//            .put("status", Response.Status.OK)
//            //.put("trace", trace.serializeToString())
//            .put("query", request.getAttribute(V2_QUERY_KEY))
//            .build()));
//         
//          
//          if (response_span != null) {
//            response_span.setTag("finalThread", Thread.currentThread().getName())
//                         .setTag("status", "OK")
//                         .finish();
//          }
//          if (context.stats().trace() != null && 
//              context.stats().trace().firstSpan() != null) {
//            context.stats().trace().firstSpan()
//              .setTag("status", "OK")
//              .setTag("finalThread", Thread.currentThread().getName())
//              .finish();
//          }
      }
    };
    return Response.ok().entity(stream)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }
  
  Response handleException(final HttpServletRequest request) throws Exception {
    final QueryContext context = (QueryContext) request.getAttribute(CONTEXT_KEY);
    final SemanticQuery query = (SemanticQuery) request.getAttribute(QUERY_KEY);
    final Trace trace;
    if (context != null && context.stats().trace() != null) {
      trace = context.stats().trace();
    } else {
      trace = null;
    }
    
    final Exception e = (Exception) request.getAttribute(
            OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE);
//    LOG.info("Completing query=" 
//      + JSON.serializeToString(ImmutableMap.<String, Object>builder()
//      // TODO - possible upstream headers
//      .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
//      .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
//      .put("traceId", trace != null ? trace.traceId() : "")
//      .put("status", Response.Status.OK)
//      .put("query", request.getAttribute(V2_QUERY_KEY))
//      .build()));
//    
//    QUERY_LOG.info("Completing query=" 
//       + JSON.serializeToString(ImmutableMap.<String, Object>builder()
//      
//      // TODO - possible upstream headers
//      .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
//      .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
//      .put("traceId", trace != null ? trace.traceId() : "")
//      .put("status", Response.Status.OK)
//      //.put("trace", trace.serializeToString())
//      .put("query", request.getAttribute(V2_QUERY_KEY))
//      .build()));
//    
//    if (trace != null && trace.firstSpan() != null) {
//      trace.firstSpan()
//        .setTag("status", "Error")
//        .setTag("finalThread", Thread.currentThread().getName())
//        .setTag("error", e.getMessage() == null ? "null" : e.getMessage())
//        .log("exception", e)
//        .finish();
//    }
//    query_exceptions.incrementAndGet();
    throw e;
  }
}
