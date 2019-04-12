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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import net.opentsdb.auth.AuthState;
import net.opentsdb.auth.AuthState.AuthStatus;
import net.opentsdb.auth.Authentication;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.meta.*;
import net.opentsdb.meta.MetaDataStorageResult.MetaResult;
import net.opentsdb.meta.BatchMetaQuery.QueryType;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.servlet.exceptions.GenericExceptionMapper;
import net.opentsdb.servlet.filter.AuthFilter;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.Trace;
import net.opentsdb.stats.Tracer;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Pair;

import net.opentsdb.utils.UniqueKeyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

@Path("search/timeseries")
public class MetaRpc {
    private static final Logger LOG = LoggerFactory.getLogger(MetaRpc.class);
    private final String NAME = "name";
    private final String COUNT = "count";
    
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public void post(final @Context ServletConfig servlet_config,
                     final @Context HttpServletRequest request,
                     final @Context HttpServletResponse response) throws Exception {
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
            tsdb.getStatsCollector().incrementCounter("query.new", "endpoint", "meta");
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
                    .withTag("endpoint", "/api/search")
                    .withTag("user", auth_state != null ? auth_state.getUser() : "Unkown")
                    // TODO - more useful info
                    .start();
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
        final BatchMetaQuery query;
        if (content_type != null && content_type.toLowerCase().contains("yaml")) {
           throw new Exception("yaml query not supported. please use json");
        } else {
            ObjectMapper mapper = JSON.getMapper();
            JsonNode node = mapper.readTree(request.getInputStream());
            query = DefaultBatchMetaQuery.parse(tsdb, mapper, node).build();
        }

        ObjectMapper map = new ObjectMapper();
        LOG.info("Meta query == " + map.writeValueAsString
                (query.metaQueries()));
        // TODO validate
        if (parse_span != null) {
            parse_span.setSuccessTags()
                    .finish();
        }

        // TODO - actual async
        final AsyncContext async = request.startAsync();
        async.setTimeout((Integer) servlet_config.getServletContext()
                .getAttribute(OpenTSDBApplication.ASYNC_TIMEOUT_ATTRIBUTE));

        LOG.info("Executing new query=" + JSON.serializeToString(
                ImmutableMap.<String, Object>builder()
                        // TODO - possible upstream headers
                        .put("queryId", Bytes.byteArrayToString(String.valueOf(query.hashCode()).getBytes()))
                        .put("traceId", trace != null ? trace.traceId() : "")
                        .put("query", JSON.serializeToString(query))
                        .build()));
        Span setup_span = null;
        if (query_span != null) {
            setup_span = trace.newSpanWithThread("setupContext")
                    .withTag("startThread", Thread.currentThread().getName())
                    .asChildOf(query_span)
                    .start();
        }
        
        try {
          final Map<String, MetaDataStorageResult> metaDataStorageResults =
                  tsdb.getRegistry().getDefaultPlugin(
                  MetaDataStorageSchema.class).runQuery(query, query_span)
                  .join();

          response.setContentType(MediaType.APPLICATION_JSON);
          ByteArrayOutputStream stream = new ByteArrayOutputStream();

          JsonFactory factory = new JsonFactory();

          JsonGenerator json = factory.createGenerator(stream);

          json.writeStartObject();

          json.writeArrayFieldStart("results");
          for (Map.Entry<String, MetaDataStorageResult> metadata_result :
                  metaDataStorageResults.entrySet() ) {
            MetaDataStorageResult metadata_storage_result = metadata_result
                    .getValue();
            if (metadata_storage_result.result() == MetaResult.EXCEPTION) {
              throw metadata_storage_result.exception();
            }


            json.writeStartObject();
            json.writeNumberField("totalHits", metadata_storage_result.totalHits());
            json.writeStringField("id", metadata_storage_result.id());

            json.writeFieldName("namespaces");
            json.writeStartArray();
            for (final String namespace : metadata_storage_result.namespaces()) {
              json.writeString(namespace);
            }
            json.writeEndArray();

            json.writeFieldName("timeseries");
            json.writeStartArray();
            for (TimeSeriesId timeSeriesId : metadata_storage_result.timeSeries()) {
              json.writeStartObject();
              BaseTimeSeriesStringId id = (BaseTimeSeriesStringId) timeSeriesId;
              json.writeStringField("metric", id.metric());
              json.writeObjectFieldStart("tags");

              for (final Map.Entry<String, String> entry : id.tags().entrySet()) {
                json.writeStringField(entry.getKey(), entry.getValue());
              }
              json.writeEndObject();
              json.writeEndObject();
            }
            json.writeEndArray();

            json.writeFieldName("metrics");
            json.writeStartArray();
            if (metadata_storage_result.metrics() != null) {
              for (final Pair<String, Long> metric : metadata_storage_result.metrics()) {
                json.writeStartObject();
                json.writeStringField(NAME, metric.getKey());
                json.writeNumberField(COUNT, metric.getValue());
                json.writeEndObject();
              }
            }
            json.writeEndArray();

            json.writeObjectFieldStart("tagKeysAndValues");
            if (metadata_storage_result.tags() != null) {
              for (final Map.Entry<UniqueKeyPair<String, Long>, List<UniqueKeyPair<String, Long>>>

                      tags :
                      metadata_storage_result.tags().entrySet()) {
                json.writeObjectFieldStart(tags.getKey().getKey());
                json.writeNumberField("hits", tags.getKey().getValue());
                json.writeArrayFieldStart("values");
                if (tags.getValue() != null) {
                  for (final Pair<String, Long> tagv : tags.getValue()) {
                    json.writeStartObject();
                    json.writeStringField(NAME, tagv.getKey());
                    json.writeNumberField(COUNT, tagv.getValue());
                    json.writeEndObject();
                  }
                }
                json.writeEndArray();
                json.writeEndObject();
              }
            }
            json.writeEndObject();

            json.writeFieldName("tagKeys");
            json.writeStartArray();
            if (query.type() == QueryType.TAG_KEYS &&
                    metadata_storage_result.tagKeysOrValues() != null) {
              for (final Pair<String, Long> key_or_value : metadata_storage_result.tagKeysOrValues()) {
                json.writeStartObject();
                json.writeStringField(NAME, key_or_value.getKey());
                json.writeNumberField(COUNT, key_or_value.getValue());
                json.writeEndObject();
              }
            }
            json.writeEndArray();

            json.writeFieldName("tagValues");
            json.writeStartArray();
            if (query.type() == QueryType.TAG_VALUES &&
                    metadata_storage_result.tagKeysOrValues() != null) {
              for (final Pair<String, Long> key_or_value : metadata_storage_result.tagKeysOrValues()) {
                json.writeStartObject();
                json.writeStringField(NAME, key_or_value.getKey());
                json.writeNumberField(COUNT, key_or_value.getValue());
                json.writeEndObject();
              }
            }
            json.writeEndArray();
            json.writeEndObject();

          }
          json.writeEndArray();
          json.writeEndObject();
          json.close();
          final byte[] data = stream.toByteArray();
          response.getOutputStream().write(data);
          response.getOutputStream().close();
          stream.close();

        } catch (Throwable t) {
            LOG.error("Unexpected exception triggering query.", t);
            GenericExceptionMapper.serialize(t, response);
            async.complete();
            if (query_span != null) {
                query_span.setErrorTags(t)
                        .finish();
            }
        }

    }

}
