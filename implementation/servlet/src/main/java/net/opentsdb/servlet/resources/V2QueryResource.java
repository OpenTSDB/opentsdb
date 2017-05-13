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
import java.lang.reflect.Constructor;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.swing.plaf.synth.SynthScrollBarUI;
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
import com.stumbleupon.async.Callback;

import io.opentracing.Span;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TsdbPlugin;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
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
import net.opentsdb.utils.JSON;


@Path("query/v2")
public class V2QueryResource {
  private static final Logger LOG = LoggerFactory.getLogger(
      V2QueryResource.class);
  
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response post(
    final @Context ServletConfig servlet_config, 
    final @Context HttpServletRequest request) throws Exception {
    if (request.getAttribute(
        OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE) != null) {
      throw (Exception) request.getAttribute(
              OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE);
    } else if (request.getAttribute(
        OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE) != null) {
      
      final IteratorGroups groups = (IteratorGroups) request.getAttribute(
          OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE);
      final QueryContext context = (QueryContext) request.getAttribute("MYCONTEXT");
      final Span serdes_span;
      final TsdbTrace trace;
      if (context.getTracer() != null) {
        trace = (TsdbTrace) request.getAttribute("TRACE");
        serdes_span = context.getTracer().buildSpan("serialization")
            .asChildOf(trace.getFirstSpan())
            .start();
      } else {
        serdes_span = null;
        trace = null;
      }

      StreamingOutput stream = new StreamingOutput() {

        @Override
        public void write(OutputStream output)
            throws IOException, WebApplicationException {
          final JsonGenerator json = JSON.getFactory().createGenerator(output);
          final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(json);
          serdes.serialize(output, groups);
          
          // TODO - trace, other bits.
          if (serdes_span != null) {
            serdes_span.finish();
          }
        }
      };
      
      if (trace != null) {
        trace.getFirstSpan().finish();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Query completed!");
      }
      return Response.ok().entity(stream)
          .type( MediaType.APPLICATION_JSON )
          .build();
      // all done!
    } else {
      final TSDB tsdb = (TSDB) servlet_config.getServletContext()
          .getAttribute(OpenTSDBApplication.TSD_ATTRIBUTE);
      if (tsdb == null) {
        throw new IllegalStateException("The TSDB instance was null.");
      }
      final TsdbTrace trace;
      final Span span;
      final TsdbPlugin tracer = tsdb.getRegistry().getDefaultPlugin(TsdbTracer.class);
      if (tracer != null) {
        trace = ((TsdbTracer) tracer).getTracer(true);
        span = trace.tracer().buildSpan(this.getClass().getSimpleName())
            .withTag("query", "Hello!")
            .start();
        trace.setFirstSpan(span);
        request.setAttribute("TRACE", trace);
      } else {
        trace = null;
        span = null;
      }
      final TSQuery ts_query = JSON.parseToObject(request.getInputStream(), TSQuery.class);
      ts_query.validateAndSetQuery();
      
      // copy the required headers.
      // TODO - break this out into a helper function.
      final Enumeration<String> headers = request.getHeaderNames();
      final Map<String, String> headersCopy = new HashMap<String, String>();
      while (headers.hasMoreElements()) {
        final String header = headers.nextElement();
        if (header.toUpperCase().startsWith("X") || header.equals("Cookie")) {
          headersCopy.put(header, request.getHeader(header));
        }
      }
      
      // start the Async context and pass it around.
      final AsyncContext async = request.startAsync();
      async.setTimeout((Integer) servlet_config.getServletContext()
          .getAttribute(OpenTSDBApplication.ASYNC_TIMEOUT_ATTRIBUTE));
      
      final TimeSeriesQuery query = TSQuery.convertQuery(ts_query);
      query.groupId(new SimpleStringGroupId(""));
      query.validate();
      
      final QueryContext context = new DefaultQueryContext(tsdb, 
          tsdb.getRegistry().getExecutionGraph(null), 
          trace == null ? null : trace.tracer());
      context.addSessionObject(HttpQueryV2Executor.SESSION_HEADERS_KEY, headersCopy);
      request.setAttribute("MYCONTEXT", context);
      
      final QueryExecutor<IteratorGroups> executor =
          (QueryExecutor<IteratorGroups>) context.executionGraph().sinkExecutor();
      
      final QueryExecution<IteratorGroups> execution = 
          executor.executeQuery(context, query, span);
      
      class SuccessCB implements Callback<Object, IteratorGroups> {

        @Override
        public Object call(final IteratorGroups groups) throws Exception {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Query responded. Setting async to serialize.");
          }
          request.setAttribute(OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE, groups);
          async.dispatch();
          return null;
        }
        
      }
      
      class ErrorCB implements Callback<Object, Exception> {
        @Override
        public Object call(final Exception ex) throws Exception {
          if (ex instanceof RemoteQueryExecutionException) {
            try {
            RemoteQueryExecutionException e = (RemoteQueryExecutionException) ex;
            if (e.getExceptions().size() > 0) {
              for (Exception exception : e.getExceptions()) {
                if (exception != null) {
                  exception.printStackTrace();
                }
              }
            }
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
          ex.printStackTrace();
          request.setAttribute(OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE, ex);
          async.dispatch();
          return null;
        }
      }
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Started query");
      }
      execution.deferred()
        .addCallback(new SuccessCB())
        .addErrback(new ErrorCB());
    }
    return null;
  }
  
}
