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
package net.opentsdb.servlet.exceptions;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;

import com.google.common.collect.Maps;

import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import jersey.repackaged.com.google.common.collect.Lists;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.utils.JSON;

/**
 * Simple class to convert a {@link QueryExecutionException} exception into a 
 * nicely formatted JSON object.
 * 
 * @since 3.0
 */
public class QueryExecutionExceptionMapper implements 
  ExceptionMapper<QueryExecutionException> {

  /** Whether or not to show the stack traces. */
  private final boolean show_trace;
  
  /** Max depth to recursively print nested exceptions. */
  private final int max_depth;
  
  /**
   * Default ctor.
   * @param show_trace Whether or not to show the stack traces.
   * @param max_depth How deep to recurse.
   */
  public QueryExecutionExceptionMapper(final boolean show_trace, 
                                       final int max_depth) {
    this.show_trace = show_trace;
    this.max_depth = max_depth;
  }
  
  @Override
  public Response toResponse(final QueryExecutionException e) {
    final Map<String, Object> outer_map = Maps.newHashMapWithExpectedSize(1);

    outer_map.put("error", recursiveExceptions(0, e));
    final Status status = Status.fromStatusCode(e.getStatusCode());
    return Response.status(status != null ? status : Status.INTERNAL_SERVER_ERROR)
        .entity(JSON.serializeToString(outer_map))
        .type(MediaType.APPLICATION_JSON)
        .build(); 
  }
  
  /**
   * 
   * @param parent_map The parent map to write sub exceptions to.
   * @param e The new exception.
   */
  private Map<String, Object> recursiveExceptions(final int depth,
      final QueryExecutionException e) {
    final Map<String, Object> response = Maps.newTreeMap();
    response.put("code", e.getStatusCode());
    response.put("message", e.getMessage());
    if (show_trace) {
      final ThrowableProxy tp = new ThrowableProxy(e);
      tp.calculatePackagingData();
      final String formatted_trace = ThrowableProxyUtil.asString(tp);
      response.put("trace", formatted_trace);  
    }
    if (!e.getExceptions().isEmpty()) {
      final List<Map<String, Object>> sub_exceptions = 
          Lists.newArrayListWithExpectedSize(e.getExceptions().size());
      response.put("subExceptions", sub_exceptions);
      for (final Exception ex : e.getExceptions()) {
        if (ex instanceof QueryExecutionException && depth < max_depth) {
          sub_exceptions.add(recursiveExceptions(max_depth + 1, 
              (QueryExecutionException) ex));
        } else {
          final Map<String, Object> sub_exception = Maps.newTreeMap();
          sub_exception.put("code", ex instanceof QueryExecutionException ? 
              ((QueryExecutionException) ex).getStatusCode() : 0);
          sub_exception.put("message", ex.getMessage());
          if (show_trace) {
            final ThrowableProxy tp = new ThrowableProxy(ex);
            tp.calculatePackagingData();
            final String formatted_trace = ThrowableProxyUtil.asString(tp);
            sub_exception.put("trace", formatted_trace);  
          }
          sub_exceptions.add(sub_exception);
        }
      }
    } else if (e.getCause() != null) {
      if (e.getCause() instanceof QueryExecutionException) {
        response.put("cause", 
            recursiveExceptions(depth + 1, (QueryExecutionException) e.getCause()));
      } else {
        final Map<String, Object> cause = Maps.newTreeMap();
        cause.put("code", e.getCause() instanceof QueryExecutionException ? 
            ((QueryExecutionException) e.getCause()).getStatusCode() : 0);
        cause.put("message", e.getCause().getMessage());
        if (show_trace) {
          final ThrowableProxy tp = new ThrowableProxy(e.getCause());
          tp.calculatePackagingData();
          final String formatted_trace = ThrowableProxyUtil.asString(tp);
          cause.put("trace", formatted_trace);  
        }
        response.put("cause", cause);
      }
    }
    return response;
  }
}
