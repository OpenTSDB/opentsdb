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

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

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
  
  @Override
  public Response toResponse(final Throwable t) {
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
