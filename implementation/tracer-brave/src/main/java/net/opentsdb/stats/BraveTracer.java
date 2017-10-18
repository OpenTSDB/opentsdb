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

import io.opentracing.Tracer;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.TSDB;
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
public class BraveTracer extends TsdbTracer {
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
    this.tsdb = tsdb;
    
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
  public TsdbTrace getTracer(final boolean report) {
    return getTracer(report, null);
  }
  
  @Override
  public TsdbTrace getTracer(final boolean report, final String service_name) {
    final brave.Tracer.Builder builder = brave.Tracer.newBuilder();
    builder.traceId128Bit(true);
    if (Strings.isNullOrEmpty(service_name)) {
      builder.localServiceName(this.service_name);
    } else {
      builder.localServiceName(service_name);
    }
    final SpanCatcher span_catcher = new SpanCatcher(report);
    builder.reporter(span_catcher);
    return new Trace(brave.opentracing.BraveTracer.wrap(builder.build()),
        span_catcher);
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
   * Implementation of the TsdbTrace that holds the span catcher so we can
   * serialize the spans for this trace.
   */
  class Trace extends TsdbTrace {
    
    /** The span catcher associated with this trace. */
    private final SpanCatcher catcher;
    
    /**
     * Default ctor.
     * @param tracer A non-null tracer;
     * @param catcher A non-null span catcher.
     */
    Trace(final Tracer tracer, final SpanCatcher catcher) {
      super(tracer);
      this.catcher = catcher;
    }

    @Override
    public void serializeJSON(final String name, final JsonGenerator json) {
      Span last_span = null;
      try {
        json.writeArrayFieldStart(name);
        for (final Span span : catcher.spans) {
          last_span = span;
          json.writeStartObject();
          json.writeStringField("traceId", Long.toHexString(span.traceId));
          json.writeStringField("id", Long.toHexString(span.id));
          json.writeStringField("name", span.name);
          if (span.parentId == null) {
            json.writeNullField("parentId");
          } else {
            json.writeStringField("parentId", Long.toHexString(span.parentId));
          }
          // span timestamps could potentially be null.
          if (span.timestamp != null) {
            json.writeNumberField("timestamp", span.timestamp);
            json.writeNumberField("duration", span.duration);
          }
          // TODO - binary annotations, etc.
          if (span.binaryAnnotations != null) {
            json.writeObjectFieldStart("tags");
            for (final BinaryAnnotation tag : span.binaryAnnotations) {
              switch (tag.type) {
              case STRING:
                json.writeStringField(tag.key, new String(tag.value));
                break;
              default:
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Skipping span data type: " + tag.type);
                }
              }
            }
            json.writeEndObject();
          }
          json.writeEndObject();
        }
        json.writeEndArray();
      } catch (NullPointerException e) {
        LOG.error("WTF? NPE?: " + last_span);
      } catch (IOException e) {
        throw new RuntimeException("Unexpected exception", e);
      }
    }

    @Override
    public String serializeToString() {
      final StringBuilder buf = new StringBuilder()
          .append("[");
      int i = 0;
      for (final Span span : catcher.spans) {
        if (i++ > 0) {
          buf.append(",");
        }
        buf.append(span.toString());
      }
      buf.append("]");
      return buf.toString();
    }
    
    @Override
    protected String extractTraceId(final io.opentracing.Span span) {
      if (span == null) {
        throw new IllegalArgumentException("Span cannot be null.");
      }
      if (!(span instanceof brave.opentracing.BraveSpan)) {
        throw new IllegalArgumentException("Span was not a Brave span. Make "
            + "sure you're using the proper tracing plugins");
      }
      if (span.context() == null) {
        throw new IllegalStateException("WTF? Span context was null.");
      }
      if (!(span.context() instanceof brave.opentracing.BraveSpanContext)) {
        throw new IllegalArgumentException("Span context was not a Brave span "
            + "context. Make sure you're using the proper tracing plugins");
      }
      return ((brave.opentracing.BraveSpanContext) span.context())
          .unwrap().traceIdString();
    }
  }
  
  /**
   * A means of capturing the spans so that we can serialize them later on
   * as part of the response. It can still forward to a reporter if configured.
   */
  private class SpanCatcher implements Reporter<Span> {
    /** A set used to store spans as they come in. Should be thread safe. */
    private final Set<Span> spans = Sets.newConcurrentHashSet();
    
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
