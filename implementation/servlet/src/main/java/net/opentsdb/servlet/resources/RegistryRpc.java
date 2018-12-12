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
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.utils.JSON;

@Path("registry")
public class RegistryRpc {
  
  @Path("shared")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getShared(final @Context ServletConfig servlet_config, 
                            final @Context HttpServletRequest request) throws Exception {
    final TSDB tsdb = getTSDB(servlet_config);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JsonGenerator json = JSON.getFactory().createGenerator(baos);
    json.writeStartObject();
    
    for (final Entry<String, Object> entry :
        tsdb.getRegistry().sharedObjects().entrySet()) {
      json.writeStringField(entry.getKey(), entry.getValue().getClass().toString());
    }
    json.writeEndObject();
    json.close();
    return Response.ok(baos.toByteArray())
        .build();
  }
  
  @Path("datatypes")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTypes(final @Context ServletConfig servlet_config, 
                           final @Context HttpServletRequest request) throws Exception {
    final TSDB tsdb = getTSDB(servlet_config);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JsonGenerator json = JSON.getFactory().createGenerator(baos);
    json.writeStartObject();
    
    json.writeObjectFieldStart("defaultTypeNames");
    for (final Entry<TypeToken<? extends TimeSeriesDataType>, String> entry :
        tsdb.getRegistry().defaultTypeNameMap().entrySet()) {
      json.writeStringField(entry.getKey().toString(), entry.getValue());
    }
    json.writeEndObject();
    
    json.writeObjectFieldStart("namesToTypes");
    for (final Entry<String, TypeToken<? extends TimeSeriesDataType>> entry :
        tsdb.getRegistry().typeMap().entrySet()) {
      json.writeStringField(entry.getKey(), entry.getValue().toString());
    }
    json.writeEndObject();

    json.writeEndObject();
    json.close();
    return Response.ok(baos.toByteArray())
        .build();
  }
  
  @Path("plugins")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPlugins(final @Context ServletConfig servlet_config, 
                             final @Context HttpServletRequest request) throws Exception {
    final TSDB tsdb = getTSDB(servlet_config);
    final Map<Class<?>, Map<String, TSDBPlugin>> plugins = 
        tsdb.getRegistry().plugins();
    String filter = request.getParameter("filter");
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JsonGenerator json = JSON.getFactory().createGenerator(baos);
    json.writeStartObject();
    
    if (!Strings.isNullOrEmpty(filter)) {
      filter = filter.toLowerCase().trim();
      if (!filter.startsWith("^")) {
        filter = ".*" + filter;
      }
      if (!filter.endsWith("$")) {
        filter += ".*";
      }
      final Pattern pattern = Pattern.compile(filter);
      
      for (final Entry<Class<?>, Map<String, TSDBPlugin>> entry : 
        plugins.entrySet()) {
        if (!pattern.matcher(entry.getKey().getCanonicalName()
            .toString().toLowerCase()).matches()) {
          continue;
        }
        
        json.writeObjectFieldStart(entry.getKey().getCanonicalName());
        for (final Entry<String, TSDBPlugin> plugin : entry.getValue().entrySet()) {
          json.writeObjectFieldStart(plugin.getKey() == null ? 
              "Default" : plugin.getKey());
          json.writeStringField("id", plugin.getValue().id());
          json.writeStringField("type", plugin.getValue().type());
          json.writeStringField("version", plugin.getValue().version());
          json.writeStringField("info", "");
          json.writeStringField("class", 
              plugin.getValue().getClass().getCanonicalName());
          json.writeEndObject();
        }
        json.writeEndObject();
      }
    } else {
      for (final Entry<Class<?>, Map<String, TSDBPlugin>> entry : 
          plugins.entrySet()) {
        json.writeObjectFieldStart(entry.getKey().getCanonicalName());
        for (final Entry<String, TSDBPlugin> plugin : entry.getValue().entrySet()) {
          json.writeObjectFieldStart(plugin.getKey() == null ? 
              "Default" : plugin.getKey());
          json.writeStringField("id", plugin.getValue().id());
          json.writeStringField("type", plugin.getValue().type());
          json.writeStringField("version", plugin.getValue().version());
          json.writeStringField("info", "");
          json.writeStringField("class", 
              plugin.getValue().getClass().getCanonicalName());
          json.writeEndObject();
        }
        json.writeEndObject();
      }
    }

    json.writeEndObject();
    json.close();
    return Response.ok(baos.toByteArray())
        .build();
  }
  
  private TSDB getTSDB(final @Context ServletConfig servlet_config) {
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
    return (TSDB) obj;
  }
}
