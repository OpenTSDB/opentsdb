// This file is part of OpenTSDB.
// Copyright (C) 2013-2021  The OpenTSDB Authors.
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

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import net.opentsdb.auth.AuthState;
import net.opentsdb.auth.AuthState.AuthStatus;
import net.opentsdb.auth.Authentication;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.servlet.filter.AuthFilter;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.SchemaFactory;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import javax.ws.rs.core.Response.Status;

/**
 * Handles the suggest endpoint that returns X number of metrics, tagks or
 * tagvs that start with the given string. It's used for auto-complete entries
 * and does not support wildcards.
 */
@Path("suggest")
public final class SuggestRpc {
  private static final Logger LOG = LoggerFactory.getLogger(SuggestRpc.class);

  // TODO - not pretty but this really is a 1x schema related RPC.
  private final Schema schema;

  public SuggestRpc(final TSDB tsdb) {
    TimeSeriesDataSourceFactory factory = tsdb.getRegistry().getDefaultPlugin(
            TimeSeriesDataSourceFactory.class);
    if (factory == null) {
      LOG.warn("No default TimeSeriesDataSourceFactory found. Queries will just " +
              "return 503s");
      schema = null;
      return;
    }

    if (!(factory instanceof SchemaFactory)) {
      LOG.warn("The default factory is not an instance of 'SchemaFactory. ("
              + factory.getClass() + "). Queries will just " + "return 503s");
      schema = null;
      return;
    }

    schema = ((SchemaFactory) factory).schema();
  }

  /**
   * Handles an HTTP based suggest query from 2x and earlier.
   * <b>Note:</b> This method must remain backwards compatible with the 1.x 
   * API call
   * @throws Exception if something went pear shaped.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response get(final @Context ServletConfig servlet_config,
                      final @Context HttpServletRequest request) throws Exception {
    if (schema == null) {
      return Response.status(Status.SERVICE_UNAVAILABLE)
              .entity("A data source for the suggest API has not been configured.")
              .build();
    }

    final String type;
    final String q;
    final String max;
    String[] temp = request.getParameterValues("type");
    if (temp == null || temp.length < 1 || Strings.isNullOrEmpty(temp[0])) {
      throw new QueryExecutionException(
              "Type `type` field must be present in the query string.", 400);
    }
    type = temp[0];

    temp = request.getParameterValues("q");
    if (temp != null && temp.length > 0) {
      q = temp[0];
    } else {
      q = null;
    }

    temp = request.getParameterValues("max");
    if (temp != null && temp.length > 0) {
      max = temp[0];
    } else {
      max = null;
    }

    return process(servlet_config, request, type, q, max);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response post(final @Context ServletConfig servlet_config,
                       final @Context HttpServletRequest request) throws Exception {
    final String type;
    final String q;
    final String max;

    final ObjectMapper mapper = JSON.getMapper();
    JsonNode root = mapper.readTree(request.getInputStream());
    JsonNode temp = root.get("type");
    if (temp == null || temp.isNull()) {
      throw new QueryExecutionException(
              "Type `type` field must be present in the payload.", 400);
    }
    type = temp.asText();

    temp = root.get("q");
    if (temp != null && !temp.isNull()) {
      q = temp.asText();
    } else {
      q = null;
    }

    temp = root.get("max");
    if (temp != null && !temp.isNull()) {
      max = temp.asText();
    } else {
      max = null;
    }
    return process(servlet_config, request, type, q, max);
  }

  private Response process(final @Context ServletConfig servlet_config,
                           final @Context HttpServletRequest request,
                           final String type,
                           final String q,
                           final String max) throws Exception {
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
      tsdb.getStatsCollector().incrementCounter("query.new", "endpoint", "suggest");
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

    final int max_results;
    if (max != null && !max.isEmpty()) {
      try {
        max_results = Integer.parseInt(max);
      } catch (NumberFormatException nfe) {
        throw new QueryExecutionException("Unable to parse 'max' as a number", 400);
      }
    } else {
      max_results = 0;
    }

    // TODO - async
    final UniqueIdType uidType;
    if ("metrics".equals(type)) {
      uidType = UniqueIdType.METRIC;
    } else if ("tagk".equals(type)) {
      uidType = UniqueIdType.TAGK;
    } else if ("tagv".equals(type)) {
      uidType = UniqueIdType.TAGV;
    } else {
      throw new QueryExecutionException("Invalid 'type' parameter", 400);
    }

    final List<String> suggestions = schema.suggest(uidType, q, max_results)
            .join(60_000);
    return Response.ok(JSON.serializeToString(suggestions)).build();
  }
}
