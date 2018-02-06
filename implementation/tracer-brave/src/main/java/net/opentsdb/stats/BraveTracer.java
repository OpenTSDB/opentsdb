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

import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.stats.BraveTrace.BraveTraceBuilder;
import zipkin.BinaryAnnotation;
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
  
  /** The default service name to set for traces. */
  private String service_name;
  
  /** The sender to use when publishing to a Zipkin server. */
  private OkHttpSender zipkin_sender;
  
  /** The reporter the sender is attached to. */
  private AsyncReporter<zipkin.Span> zipkin_reporter;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    if (tsdb == null) {
      throw new IllegalArgumentException("TSDB cannot be null.");
    }
    
    service_name = tsdb.getConfig().getString("tsdb.tracer.service_name");
    if (Strings.isNullOrEmpty(service_name)) {
      throw new IllegalArgumentException("Cannot instantiate tracer plugin "
          + "without a valid 'tsdb.tracer.service_name'");
    }
    
    final String zipkin_endpoint = 
        tsdb.getConfig().getString("tracer.brave.zipkin.endpoint");
    if (!Strings.isNullOrEmpty(zipkin_endpoint)) {
      zipkin_sender = OkHttpSender.create(zipkin_endpoint);
      zipkin_reporter = AsyncReporter
          .builder(zipkin_sender)
          .build();
      LOG.info("Setup OkHTTPSender for reporting to Zipkin: " + zipkin_endpoint);
    }
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
      if (forward && zipkin_reporter != null) {
        zipkin_reporter.report(span);
      }
    }
  }

  @VisibleForTesting
  String serviceName() {
    return service_name;
  }
}
