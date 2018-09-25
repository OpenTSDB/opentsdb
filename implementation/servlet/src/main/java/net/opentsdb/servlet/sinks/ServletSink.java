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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.stumbleupon.async.Callback;

import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.serdes.SerdesFactory;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.JSON;

/**
 * A simple sink that will serialize the results.
 * 
 * @since 3.0
 */
public class ServletSink implements QuerySink {
  private static final Logger LOG = LoggerFactory.getLogger(
      ServletSink.class);
  
  /** The context we're operating under. */
  private final QueryContext context;
  
  /** The sink config. */
  private final ServletSinkConfig config;
  
  /** The serializer. */
  private final TimeSeriesSerdes serdes;
  
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
    try {
      serdes = factory.newInstance(context, config.serdesOptions(), 
          config.response().getOutputStream());
    } catch (IOException e) {
      throw new RuntimeException("Failed to open HTTP output stream", e);
    }
    if (serdes == null) {
      throw new IllegalArgumentException("Factory returned a null "
          + "instance for the type: " + config.serdesOptions().getType());
    }
  }
  
  @Override
  public void onComplete() {
    try {
      serdes.serializeComplete(null);
      try {
        config.response().flushBuffer();
        config.response().getOutputStream().close();
      } catch (IOException e1) {
        onError(e1);
        return;
      }
      config.async().complete();
      
//      tsdb.getStatsCollector().incrementCounter("query.success", "endpoint", "2x");
//      //query_success.incrementAndGet();
//      LOG.info("Completing query=" 
//          + JSON.serializeToString(ImmutableMap.<String, Object>builder()
//          // TODO - possible upstream headers
//          .put("queryId", Bytes.byteArrayToString(context.query().buildHashCode().asBytes()))
//          //.put("queryHash", Bytes.byteArrayToString(context.query().buildTimelessHashCode().asBytes()))
//          //.put("traceId", trace != null ? trace.getTraceId() : null)
//          .put("status", Response.Status.OK)
//          .put("query", JSON.serializeToString(context.query()))));
//        
////      QUERY_LOG.info("Completing query=" 
////          + JSON.serializeToString(ImmutableMap.<String, Object>builder()
////          // TODO - possible upstream headers
////          .put("queryId", Bytes.byteArrayToString(query.buildHashCode().asBytes()))
////          .put("queryHash", Bytes.byteArrayToString(query.buildTimelessHashCode().asBytes()))
////          //.put("traceId", trace != null ? trace.getTraceId() : null)
////          .put("status", Response.Status.OK)
////          //.put("trace", trace.serializeToString())
////          .put("query", request.getAttribute(V2_QUERY_KEY))
////          .build()));
//       
//        
////        if (response_span != null) {
////          response_span.setTag("finalThread", Thread.currentThread().getName())
////                       .setTag("status", "OK")
////                       .finish();
////        }
//        if (context.stats().trace() != null && 
//            context.stats().trace().firstSpan() != null) {
//          context.stats().trace().firstSpan()
//            .setTag("status", "OK")
//            .setTag("finalThread", Thread.currentThread().getName())
//            .finish();
//        }
      
    } catch (Exception e) {
      LOG.error("Unexpected exception dispatching async request for "
          + "query: " + JSON.serializeToString(context.query()), e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Yay, all done!");
    }
  }

  @Override
  public void onNext(final QueryResult next) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Successful response for query=" 
          + JSON.serializeToString(
              ImmutableMap.<String, Object>builder()
              // TODO - possible upstream headers
              .put("queryId", Bytes.byteArrayToString(context.query().buildHashCode().asBytes()))
              //.put("queryHash", Bytes.byteArrayToString(context.query().buildTimelessHashCode().asBytes()))
              //.put("traceId", trace != null ? trace.traceId() : "")
              .put("query", JSON.serializeToString(context.query()))
              .build()));
    }
    
    class FinalCB implements Callback<Void, Object> {
      @Override
      public Void call(final Object ignored) throws Exception {
        next.close();
        return null;
      }
    }
    
    try {
      serdes.serialize(next, null /* TODO */)
        .addBoth(new FinalCB());
    } catch (Exception e) {
      onError(e);
      return;
    }
  }

  @Override
  public void onError(final Throwable t) {
    LOG.error("Exception for query: " 
        //+ Bytes.byteArrayToString(query.buildHashCode().asBytes()), t);
        ,t);
    //request.setAttribute(OpenTSDBApplication.QUERY_EXCEPTION_ATTRIBUTE, t);
    try {
      config.async().dispatch();
    } catch (Exception e) {
      LOG.error("WFT? Dispatch may have already been called", e);
    }
  }
}
