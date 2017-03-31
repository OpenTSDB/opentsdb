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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
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

import io.netty.util.Timer;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.DataShard;
import net.opentsdb.data.DataShards;
import net.opentsdb.data.DataShardsGroup;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.RemoteQueryExecutionException;
import net.opentsdb.query.TSQuery;
import net.opentsdb.query.context.HttpContextFactory;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.context.RemoteContext;
import net.opentsdb.query.execution.MultiClusterQueryExecutor;
import net.opentsdb.query.execution.QueryExecution;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
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
      final @Context HttpServletRequest request) {
    try {
      if (request.getAttribute(
          OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE) != null) {
        throw new WebApplicationException(
            (Exception) request.getAttribute(
                OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE), 
            Response.Status.BAD_REQUEST);

      } else if (request.getAttribute(
          OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE) != null) {
        
        final DataShardsGroup groups = (DataShardsGroup) request.getAttribute(
            OpenTSDBApplication.QUERY_RESULT_ATTRIBUTE);
        
        StreamingOutput stream = new StreamingOutput() {

          @Override
          public void write(OutputStream output)
              throws IOException, WebApplicationException {
            JsonGenerator json = JSON.getFactory().createGenerator(output);
            json.writeStartArray();
            
            for (final DataShards shards : groups.data()) {
              for (final DataShard shard : shards.data()) {
                json.writeStartObject();
                
                json.writeStringField("metric", new String(shard.id().metrics().get(0)));
                json.writeObjectFieldStart("tags");
                for (final Entry<byte[], byte[]> entry : shard.id().tags().entrySet()) {
                  json.writeStringField(new String(entry.getKey()), new String(entry.getValue()));
                }
                json.writeEndObject();
                json.writeObjectFieldStart("dps");
                
                @SuppressWarnings("unchecked")
                TimeSeriesIterator<NumericType> it = shard.iterator();
                while (it.status() == IteratorStatus.HAS_DATA) {
                  TimeSeriesValue<NumericType> v = it.next();
                  if (v.value().isInteger()) {
                    json.writeNumberField(Long.toString(v.timestamp().msEpoch()), 
                        v.value().longValue());
                  } else {
                    json.writeNumberField(Long.toString(v.timestamp().msEpoch()), 
                        v.value().doubleValue());
                  }
                }
                
                json.writeEndObject();
                json.writeEndObject();
                json.flush();
              }
            }
            
            json.writeEndArray();
            json.flush();
            json.close();
          }
          
        };
        return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
        // all done!
      } else {
        final TSQuery ts_query = JSON.parseToObject(request.getInputStream(), TSQuery.class);
        ts_query.validateAndSetQuery();
        
        final TSDB tsdb = (TSDB) servlet_config.getServletContext()
            .getAttribute(OpenTSDBApplication.TSD_ATTRIBUTE);
        if (tsdb == null) {
          throw new IllegalStateException("The TSDB instance was null.");
        }
        
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
        
        final MyContext context = new MyContext(
            (HttpContextFactory) servlet_config.getServletContext()
            .getAttribute(OpenTSDBApplication.HTTP_CONTEXT_FACTORY),
            headersCopy, tsdb.getTimer());
        
        final QueryExecutor<DataShardsGroup> executor = 
            new MultiClusterQueryExecutor<DataShardsGroup>(context, 
                DataShardsGroup.class, 5000L);
        
        final QueryExecution<DataShardsGroup> execution = 
            executor.executeQuery(query);
        
        class SuccessCB implements Callback<Object, DataShardsGroup> {

          @Override
          public Object call(final DataShardsGroup groups) throws Exception {
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
        
        execution.deferred()
          .addCallback(new SuccessCB())
          .addErrback(new ErrorCB());
      }
    } catch (Exception e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
    return null;
  }
  
  class MyContext extends QueryContext {
    final HttpContextFactory ctx;
    final Map<String, String> headers;
    final Timer timer;
    MyContext(final HttpContextFactory ctx, final Map<String, String> headers,
        final Timer timer) {
      this.ctx = ctx;
      this.headers = headers;
      this.timer = timer;
    }
    @Override
    public RemoteContext getRemoteContext() {
      return ctx.getContext(this, headers);
    }

    @Override
    public Timer getTimer() {
      return timer;
    }
    
  }
}
