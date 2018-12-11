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
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.utils.Exceptions;
import net.opentsdb.utils.JSON;

/**
 * Handles formatting an unexpected exception by wrapping it in a JSON
 * map and prettifying the stack trace.
 * 
 * @since 3.0
 */
public class GenericExceptionMapper implements ExceptionMapper<Throwable> {
  private static final Logger LOG = LoggerFactory.getLogger(
      GenericExceptionMapper.class);
  
  @Override
  public Response toResponse(Throwable t) {
    if (t instanceof DeferredGroupException) {
      t = Exceptions.getCause((DeferredGroupException) t);
    }
    
    if (t instanceof NotFoundException) {
      return Response.status(Response.Status.NOT_FOUND)
          .build(); 
    }
    if (t instanceof IllegalArgumentException ) {
      final Map<String, Object> response = new HashMap<String, Object>(3);
      response.put("code", Response.Status.BAD_REQUEST);
      response.put("message", t.getMessage());
      response.put("trace", Throwables.getStackTraceAsString(t));
      
      final Map<String, Object> outerMap = new HashMap<String, Object>(1);
      outerMap.put("error", response);
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(JSON.serializeToString(outerMap))
          .type(MediaType.APPLICATION_JSON)
          .build(); 
    }
    
    LOG.error("Unexpected exception", t);
    
    // TODO - it looks like Jersey doesn't like long error messages. So we'll
    // truncate this guy to around 4096 bytes-ish and that should help us out.
    int len = 12
        + 10 
        + Response.Status.INTERNAL_SERVER_ERROR.toString().length() 
        + 12
        + t.getMessage().length();
        
    final Map<String, Object> response = new HashMap<String, Object>(3);
    response.put("code", Response.Status.INTERNAL_SERVER_ERROR);
    response.put("message", t.getMessage());
    String trace = Throwables.getStackTraceAsString(t);
    if (trace.length() > 4096 - len) {
      int idx = trace.lastIndexOf('\n', 4096 - len);
      if (idx < 0) {
        idx = trace.lastIndexOf('\t', 4096 - len);
      }
      if (idx < 0) {
        idx = 4096 - len;
      }
      trace = trace.substring(0, idx) + "...";
    }
    response.put("trace", trace);
    final Map<String, Object> outerMap = new HashMap<String, Object>(1);
    outerMap.put("error", response);
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(JSON.serializeToString(outerMap))
        .type(MediaType.APPLICATION_JSON)
        .build(); 
  }

  public static void serialize(Throwable t, 
                               final ServletResponse response) {
    if (t instanceof DeferredGroupException) {
      t = Exceptions.getCause((DeferredGroupException) t);
    }
    
    if (t instanceof NotFoundException) {
      if (response instanceof HttpServletResponse) {
        ((HttpServletResponse) response).setStatus(
            Status.NOT_FOUND.getStatusCode());
      }
      return;
    }
    
    LOG.error("Unexpected exception", t);
    
    int status = Status.INTERNAL_SERVER_ERROR.getStatusCode();
    if (t instanceof QueryExecutionException) {
      status = ((QueryExecutionException) t).getStatusCode();
    } else if (t instanceof IllegalArgumentException) {
      status = Status.BAD_REQUEST.getStatusCode();
    }
    
    final Map<String, Object> map = new HashMap<String, Object>(3);
    map.put("code", status);
    map.put("message", t.getMessage());
    map.put("trace", 
        Throwables.getStackTraceAsString(t));
    
    final Map<String, Object> outerMap = new HashMap<String, Object>(1);
    outerMap.put("error", map);
    if (response instanceof HttpServletResponse) {
      ((HttpServletResponse) response).setStatus(status);
    }
    response.setContentType("application/json");
    try {
      response.getOutputStream().write(JSON.serializeToBytes(outerMap));
    } catch (IOException e) {
      LOG.error("WTF? Failed to serialize error?", e);
    }
  }
}
