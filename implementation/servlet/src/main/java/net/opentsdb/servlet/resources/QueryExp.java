package net.opentsdb.servlet.resources;

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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.opentracing.Span;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.DataShardsGroups;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.utils.JSON;

@Path("query/exp")
public class QueryExp {

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response post(
    final @Context ServletConfig servlet_config, 
    final @Context HttpServletRequest request) throws Exception {
    
    if (request.getAttribute(
        OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE) != null) {
      final QueryExecutor<DataShardsGroups> executor = 
          (QueryExecutor<DataShardsGroups>) request.getAttribute("MYEXECUTOR");
      // TODO - attach callback to log faults.
      executor.close();
      throw (Exception) request.getAttribute(
              OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE);
    } else if (request.getAttribute(
        OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE) != null) {
      // TODO
    } else {
      // initiate the query
      final TSDB tsdb = (TSDB) servlet_config.getServletContext()
          .getAttribute(OpenTSDBApplication.TSD_ATTRIBUTE);
      if (tsdb == null) {
        throw new IllegalStateException("The TSDB instance was null.");
      }
      final TsdbTrace trace;
      final Span span;
      if (tsdb.getRegistry().tracer() != null) {
        trace = tsdb.getRegistry().tracer().getTracer(true);
        span = trace.tracer().buildSpan(this.getClass().getSimpleName())
            .withTag("query", "Hello!")
            .start();
        trace.setFirstSpan(span);
      } else {
        trace = null;
        span = null;
      }
      
      final TimeSeriesQuery query = JSON.parseToObject(request.getInputStream(), 
          TimeSeriesQuery.class);
      query.validate();
      
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
    }
    
    return null;
  }
}
