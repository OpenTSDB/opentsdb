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

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
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
  public Response toResponse(final Throwable t) {
    if (!(t instanceof NotFoundException)) {
      LOG.error("Unexpected exception", t);
    }
    
    final ThrowableProxy tp = new ThrowableProxy(t);
    tp.calculatePackagingData();
    final String formattedTrace = ThrowableProxyUtil.asString(tp);
    
    final Map<String, Object> response = new HashMap<String, Object>(3);
    response.put("code", Response.Status.INTERNAL_SERVER_ERROR);
    response.put("message", t.getMessage());
    response.put("trace", formattedTrace);
    
    final Map<String, Object> outerMap = new HashMap<String, Object>(1);
    outerMap.put("error", response);
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(JSON.serializeToString(outerMap))
        .type(MediaType.APPLICATION_JSON)
        .build(); 
  }

}
