// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.servlet.sinks;

import java.io.ByteArrayOutputStream;

import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.QuerySinkCallback;
import net.opentsdb.query.serdes.SerdesCallback;
import net.opentsdb.query.serdes.SerdesFactory;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.servlet.exceptions.GenericExceptionMapper;
import net.opentsdb.servlet.exceptions.QueryExecutionExceptionMapper;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.JSON;

/**
 * A simple sink that will serialize the results.
 * 
 * @since 3.0
 */
public class ServletSink implements QuerySink, SerdesCallback {
  private static final Logger LOG = LoggerFactory.getLogger(
      ServletSink.class);
  private static final Logger QUERY_LOG = LoggerFactory.getLogger("QueryLog");
  
  /** The context we're operating under. */
  private final QueryContext context;
  
  /** The sink config. */
  private final ServletSinkConfig config;
  
  /** The serializer. */
  private final TimeSeriesSerdes serdes;
  
  /** TEMP - This sucks but we need to figure out proper async writes. */
  private final ByteArrayOutputStream stream;
  
  /** The query sink callback. Only one allowed. */
  private QuerySinkCallback callback;
  
  /**
   * Default ctor.
   * @param context The non-null context.
   * @param config The non-null config.
   */
  public ServletSink(final QueryContext context, 
                     final ServletSinkConfig config) {
    this.context = context;
    this.config = config;
    
    final SerdesFactory factory = context.tsdb().getRegistry()
        .getPlugin(SerdesFactory.class, config.serdesOptions().getType());
    if (factory == null) {
      throw new IllegalArgumentException("Unable to find a serdes "
          + "factory for the type: " + config.serdesOptions().getType());
    }
    
    // TODO - noooo!!!!
    stream = new ByteArrayOutputStream();
    serdes = factory.newInstance(
        context, 
        config.serdesOptions(), 
        stream);
    if (serdes == null) {
      throw new IllegalArgumentException("Factory returned a null "
          + "instance for the type: " + config.serdesOptions().getType());
    }
  }
  
  @Override
  public void onComplete() {
    if (context.query().isTraceEnabled()) {
      context.logTrace("Query serialization complete.");
    }
    if (context.query().getMode() == QueryMode.VALIDATE) {
      // no-op
      return;
    }
    try {
      serdes.serializeComplete(null);
      config.request().setAttribute("DATA", stream);
//      try {
//        // TODO - oh this is sooooo ugly.... *sniff*
//        config.response().setContentType("application/json");
//        final byte[] data = stream.toByteArray();
//        stream.close();
//        config.response().setContentLength(data.length);
//        config.response().setStatus(200);
//        config.response().getOutputStream().write(data);
//        config.response().getOutputStream().close();
//      } catch (IOException e1) {
//        onError(e1);
//        return;
//      }
      //config.async().complete();
      config.async().dispatch();
      logComplete();
    } catch (Exception e) {
      LOG.error("Unexpected exception dispatching async request for "
          + "query: " + JSON.serializeToString(context.query()), e);
      GenericExceptionMapper.serialize(e, config.async().getResponse());
      config.async().complete();
      logComplete();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Yay, all done!");
    }
  }

  @Override
  public void onNext(final QueryResult next) {
    if (next.exception() != null) {
      onError(next.exception());
      return;
    }
    
    if (next.error() != null) {
      onError(new QueryExecutionException(next.error(), 0));
      return;
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Successful response for query=" 
          + JSON.serializeToString(
              ImmutableMap.<String, Object>builder()
              // TODO - possible upstream headers
              .put("queryId", Bytes.byteArrayToString(context.query().buildHashCode().asBytes()))
              .put("node", next.source().config().getId() + ":" + next.dataSource())
              .build()));
    }
    if (context.query().isTraceEnabled()) {
      context.logTrace(next.source(), "Received response: " 
          + next.source().config().getId() + ":" + next.dataSource());
    }
    
    final Span serdes_span = context.stats().querySpan() != null ?
        context.stats().querySpan().newChild("onNext_" 
            + next.source().config().getId() + ":" + next.dataSource())
        .start()
        : null;
    
    class FinalCB implements Callback<Void, Object> {
      @Override
      public Void call(final Object ignored) throws Exception {
        if (ignored != null && ignored instanceof Throwable) {
          LOG.error("Failed to serialize result: " 
              + next.source().config().getId() + ":" 
              + next.dataSource(), (Throwable) ignored);
        }
        try {
          next.close();
        } catch (Throwable t) {
          LOG.warn("Failed to close result: " 
              + next.source().config().getId() + ":" + next.dataSource(), t);
        }
        if (context.query().isTraceEnabled()) {
          context.logTrace(next.source(), "Finished serializing response: " 
              + next.source().config().getId() + ":" + next.dataSource());
        }
        if (serdes_span != null) {
          serdes_span.setSuccessTags().finish();
        }
        return null;
      }
    }
    
    try {
      serdes.serialize(next, serdes_span)
        .addBoth(new FinalCB());
    } catch (Exception e) {
      onError(e);
      return;
    }
  }

  @Override
  public void onNext(final PartialTimeSeries next, 
                     final QuerySinkCallback callback) {
    if (this.callback == null) {
      synchronized (this) {
        if (this.callback == null) {
          this.callback = callback;
        }
      }
    }
    
    if (this.callback != callback) {
      // TODO WTF?
    }
    serdes.serialize(next, this, null /** TODO */);
  }
  
  @Override
  public void onError(final Throwable t) {
    LOG.error("Exception for query: " 
        + JSON.serializeToString(context.query()), t);
    context.logError("Error sent to the query sink: " + t.getMessage());
    try {
      if (t instanceof QueryExecutionException) {
        QueryExecutionExceptionMapper.serialize(
            (QueryExecutionException) t, 
            config.async().getResponse());
      } else {
        GenericExceptionMapper.serialize(t, config.async().getResponse());
      }
      config.async().complete();
      logComplete(t);
    } catch (Throwable t1) {
      LOG.error("WFT? response may have been serialized?", t1);
    }
  }
  
  void logComplete() {
    logComplete(null);
  }
  
  void logComplete(final Throwable t) {
    if (config.statsTimer() != null) {
      config.statsTimer().stop("user", context.authState() != null ? 
          context.authState().getUser() : "Unkown", "endpoint", 
          config.request().getRequestURI() /* TODO - trim */);
    }
    
    LOG.info("Completing query=" 
      + JSON.serializeToString(ImmutableMap.<String, Object>builder()
      // TODO - possible upstream headers
      .put("queryId", Bytes.byteArrayToString(context.query().buildHashCode().asBytes()))
      //.put("queryHash", Bytes.byteArrayToString(context.query().buildTimelessHashCode().asBytes()))
      .put("traceId", context.stats().trace() != null ? 
          context.stats().trace().traceId() : "")
      .put("status", Response.Status.OK)
      //.put("query", JSON.serializeToString(context.query()))
      .put("error", t == null ? "null" : t.toString())
      .build()));
    
    QUERY_LOG.info("Completing query=" 
       + JSON.serializeToString(ImmutableMap.<String, Object>builder()
      // TODO - possible upstream headers
      .put("queryId", Bytes.byteArrayToString(context.query().buildHashCode().asBytes()))
      //.put("queryHash", Bytes.byteArrayToString(context.query().buildTimelessHashCode().asBytes()))
      .put("traceId", context.stats().trace() != null ? 
          context.stats().trace().traceId() : "")
      .put("status", Response.Status.OK)
      //.put("trace", trace.serializeToString())
      .put("query", JSON.serializeToString(context.query()))
      .put("error", t == null ? "null" : t.toString())
      .build()));
    
    if (context.stats().querySpan() != null) {
      if (t != null) {
        context.stats().querySpan()
          .setErrorTags(t)
          .setTag("query", JSON.serializeToString(context.query()))
          .finish();
      } else {
        context.stats().querySpan()
        .setSuccessTags()
        .setTag("query", JSON.serializeToString(context.query()))
        .finish();        
      }
    }
  }

  @Override
  public void onComplete(final PartialTimeSeries pts) {
    callback.onComplete(pts);
  }
  
  @Override
  public void onError(final PartialTimeSeries pts, final Throwable t) {
    callback.onError(pts, t);
  }
}
