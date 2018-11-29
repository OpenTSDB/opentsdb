// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.servlet.exceptions;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

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
  private static final Logger LOG = LoggerFactory.getLogger(
      QueryExecutionExceptionMapper.class);
  
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
    LOG.error("Query exception", e);
    final Map<String, Object> outer_map = Maps.newHashMapWithExpectedSize(1);
    outer_map.put("error", recursiveExceptions(0, e, show_trace, max_depth));
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
  private static Map<String, Object> recursiveExceptions(
      final int depth,
      final QueryExecutionException e,
      final boolean show_trace,
      final int max_depth) {
    final Map<String, Object> response = Maps.newTreeMap();
    response.put("code", e.getStatusCode());
    response.put("message", e.getMessage());
    if (show_trace && e.getCause() != null) {
      response.put("trace", 
          Throwables.getStackTraceAsString(e.getCause()));  
    }
    if (!e.getThrowables().isEmpty()) {
      final List<Map<String, Object>> sub_exceptions = 
          Lists.newArrayListWithExpectedSize(e.getThrowables().size());
      response.put("subExceptions", sub_exceptions);
      for (final Throwable t : e.getThrowables()) {
        if (t instanceof QueryExecutionException && depth < max_depth) {
          sub_exceptions.add(recursiveExceptions(
              max_depth + 1, 
              (QueryExecutionException) t,
              show_trace,
              max_depth));
        } else {
          final Map<String, Object> sub_exception = Maps.newTreeMap();
          sub_exception.put("code", t instanceof QueryExecutionException ?
              ((QueryExecutionException) t).getStatusCode() : 0);
          sub_exception.put("message", t.getMessage());
          if (show_trace && e.getCause() != null) {
            sub_exception.put("trace", 
                Throwables.getStackTraceAsString(e.getCause()));  
          }
          sub_exceptions.add(sub_exception);
        }
      }
    } else if (e.getCause() != null) {
      if (e.getCause() instanceof QueryExecutionException) {
        response.put("cause", 
            recursiveExceptions(
                depth + 1, 
                (QueryExecutionException) e.getCause(),
                show_trace,
                max_depth));
      } else {
        final Map<String, Object> cause = Maps.newTreeMap();
        cause.put("code", e.getCause() instanceof QueryExecutionException ? 
            ((QueryExecutionException) e.getCause()).getStatusCode() : 0);
        cause.put("message", e.getCause().getMessage());
        if (show_trace) {
          cause.put("trace", 
              Throwables.getStackTraceAsString(e.getCause()));  
        }
        response.put("cause", cause);
      }
    }
    return response;
  }

  public static void serialize(final QueryExecutionException e,
                               final ServletResponse response) {
    LOG.error("Query exception", e);
    final Map<String, Object> outer_map = Maps.newHashMapWithExpectedSize(1);
    outer_map.put("error", recursiveExceptions(0, e, true, 4));
    final Status status = Status.fromStatusCode(e.getStatusCode());
    if (response instanceof HttpServletResponse) {
      ((HttpServletResponse) response).setStatus(
          status != null ? status.getStatusCode() : 
        Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    response.setContentType("application/json");
    try {
      response.getOutputStream().write(JSON.serializeToBytes(outer_map));
    } catch (IOException e1) {
      LOG.error("WTF? Failed to serialize error?", e1);
    }
  }
}
