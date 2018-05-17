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
package net.opentsdb.stats;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.ConfigurationCallback;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.BraveTrace.BraveTraceBuilder;
import zipkin.Span;
import zipkin.reporter.AsyncReporter;
import zipkin.reporter.Reporter;
import zipkin.reporter.okhttp3.OkHttpSender;

/**
 * An implementation of the OpenTracing and TsdbTracer using Brave. For now it
 * will post to a Zipkin server via OkHTTP. Still a work in progress.
 * 
 * TODO - allow for various senders.
 * 
 * @since 3.0
 */
public class BraveTracer implements Tracer {
  private static final Logger LOG = LoggerFactory.getLogger(BraveTracer.class);
  
  /** Configuration keys. */
  public static final String SERVICE_NAME_KEY = "tsdb.tracer.service_name";
  public static final String ENDPOINT_KEY = "tracer.brave.zipkin.endpoint";
  
  /** The default service name to set for traces. */
  private String service_name;
  
  /** The endpoint to send traces to. */
  private volatile String zipkin_endpoint;
  
  /** The sender to use when publishing to a Zipkin server. */
  private volatile OkHttpSender zipkin_sender;
  
  /** The reporter the sender is attached to. */
  private volatile AsyncReporter<zipkin.Span> zipkin_reporter;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    if (tsdb == null) {
      throw new IllegalArgumentException("TSDB cannot be null.");
    }
    tsdb.getConfig().register(SERVICE_NAME_KEY, null, false, 
        "A name for the service logged in traces coming from this"
        + " application.");
    tsdb.getConfig().register(ENDPOINT_KEY, null, true, 
        "The host and endpoint to send traces to. E.g. "
        + "'http://127.0.0.1:9411/api/v1/spans'");
    
    service_name = tsdb.getConfig().getString(SERVICE_NAME_KEY);
    if (Strings.isNullOrEmpty(service_name)) {
      throw new IllegalArgumentException("Cannot instantiate tracer plugin "
          + "without a valid '" + SERVICE_NAME_KEY + "'");
    }
    
    tsdb.getConfig().bind(ENDPOINT_KEY, new EndpointCallback());
    return Deferred.fromResult(null);
  }
  
  @Override
  public Trace newTrace(final boolean report, final boolean debug) {
    return newTrace(report, debug, this.service_name);
  }
  
  @Override
  public Trace newTrace(final boolean report, 
                        final boolean debug, 
                        final String service_name) {
    if (Strings.isNullOrEmpty(service_name)) {
      throw new IllegalArgumentException("Service name cannot be null or empty.");
    }
    final BraveTraceBuilder builder = BraveTrace.newBuilder()
        .setIs128(true)
        .setIsDebug(debug)
        .setId(service_name);
    if (report) {
      final SpanCatcher span_catcher = new SpanCatcher(report);
      builder.setSpanCatcher(span_catcher);
    }
    return builder.build();
  }
  
  @Override
  public String id() {
    return "Brave Tracer";
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public Deferred<Object> shutdown() {
    if (zipkin_reporter != null) {
      try {
        zipkin_reporter.flush();
        zipkin_reporter.close();
      } catch (Exception e) {
        LOG.error("Exception caught closing zipkin_reporter: " 
            + zipkin_reporter, e);
      }
    }
    if (zipkin_sender != null) {
      try {
        zipkin_sender.close();
      } catch (Exception e) {
        LOG.error("Exception caught closing zipkin_sender: " 
            + zipkin_sender, e);
      }
    }
    return Deferred.fromResult(null);
  }
  
  /**
   * A means of capturing the spans so that we can serialize them later on
   * as part of the response. It can still forward to a reporter if configured.
   */
  class SpanCatcher implements Reporter<Span> {
    /** A set used to store spans as they come in. Should be thread safe. */
    final Set<Span> spans = Sets.newConcurrentHashSet();
    
    /** Whether or not we're to forward these spans. */
    private final boolean forward;
    
    /**
     * Default ctor.
     * @param forward Whether or not we're to forward these spans.
     */
    SpanCatcher(final boolean forward) {
      this.forward = forward;
    }
    
    public void report(final Span span) {
      spans.add(span);
      
      // catch the volatile state.
      final AsyncReporter<zipkin.Span> zipkin_reporter = 
          BraveTracer.this.zipkin_reporter;
      if (forward && zipkin_reporter != null) {
        zipkin_reporter.report(span);
      }
    }
  }

  @VisibleForTesting
  String serviceName() {
    return service_name;
  }

  @VisibleForTesting
  String endpoint() {
    return zipkin_endpoint;
  }
  
  @VisibleForTesting
  OkHttpSender sender() {
    return zipkin_sender;
  }
  
  @VisibleForTesting
  AsyncReporter<zipkin.Span> reporter() {
    return zipkin_reporter;
  }
  
  /** A callback used to instantiate or update the sender and reporter
   * whenever the configuration changes. */
  private class EndpointCallback implements ConfigurationCallback<String> {
    @Override
    public void update(final String key, final String value) {
      if (zipkin_endpoint == null && value == null) {
        // no change.
        return;
      }
      if (zipkin_endpoint != null && value != null && 
          zipkin_endpoint.equals(value)) {
        // no change
        return;
      }
      
      // otherwise, there was a change in the endpoint.
      OkHttpSender extant_sender = null;
      AsyncReporter<zipkin.Span> extant_reporter = null;
      
      try {
        synchronized (BraveTracer.this) {
          extant_sender = zipkin_sender;
          extant_reporter = zipkin_reporter;
          
          if (Strings.isNullOrEmpty(value)) {
            zipkin_sender = null;
            zipkin_reporter = null;
            LOG.info("New Zipkin endpoint is null. Stopping the sender.");
          } else {
            zipkin_sender = OkHttpSender.create(value);
            zipkin_reporter = AsyncReporter
                .builder(zipkin_sender)
                .build();
            LOG.info("Created a new Zipkin reporter with endpoint [" 
                + value + "]");
          }
        }
      } catch (Exception e) {
        LOG.error("Failed to initialize the sender", e);
        zipkin_sender = null;
        zipkin_reporter = null;
      }
      
      // now we need to release the old resources
      if (extant_reporter != null) {
        try {
          extant_reporter.close();
        } catch (Exception e) {
          LOG.warn("Failed to close the reporter", e);
        }
      }
      
      if (extant_sender != null) {
        try {
          extant_sender.close();
        } catch (Exception e) {
          LOG.warn("Failed to close the sender", e);
        }
      }
    }
    
    @Override
    public String toString() {
      return "Brave Tracer Endpoint Callback";
    }
  }
}
