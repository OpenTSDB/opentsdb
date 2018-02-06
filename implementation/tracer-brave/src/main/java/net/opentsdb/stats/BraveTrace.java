// This file is part of OpenTSDB.
// Copyright (C) 2017 The OpenTSDB Authors.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Strings;

import net.opentsdb.stats.BraveSpan.BraveSpanBuilder;
import net.opentsdb.stats.BraveTracer.SpanCatcher;
import zipkin.BinaryAnnotation;

/**
 * An implementation of a trace using Brave.
 * 
 * @since 3.0
 */
public class BraveTrace implements net.opentsdb.stats.Trace {
  private static final Logger LOG = LoggerFactory.getLogger(BraveTrace.class);
  
  /** The tracer this trace came from. */
  private final io.opentracing.Tracer tracer;
  
  /** Whether or not we should trace debug information. */
  private final boolean is_debug;
  
  /** The span catcher for reporting. */
  private final SpanCatcher span_catcher;
  
  /** The first span of the trace. */
  private Span first_span;
  
  /** Whether or not the first span was set. Can't check for null so... */
  private volatile boolean first_span_set = false;
  
  /**
   * Protected ctor for the builder. 
   * @param builder A non-null builder to pull settings from.
   */
  protected BraveTrace(BraveTraceBuilder builder) {
    final brave.Tracer.Builder tracer_builder = brave.Tracer.newBuilder()
        .traceId128Bit(builder.is128)
        .localServiceName(builder.id);
    if (builder.span_catcher != null) {
      tracer_builder.reporter(builder.span_catcher);
      span_catcher = builder.span_catcher;
    } else {
      span_catcher = null;
    }
    tracer = brave.opentracing.BraveTracer.wrap(tracer_builder.build());
    is_debug = builder.is_debug;
  }
  
  @Override
  public BraveSpanBuilder newSpan(final String id) {
    final BraveSpanBuilder builder = 
        (BraveSpanBuilder) BraveSpan.newBuilder(this, id);
    // double-check lock to avoid contention after the first span is set
    if (!first_span_set) {
      synchronized (this) {
        if (!first_span_set) {
          builder.isFirst(true);
          first_span_set = true;
        }
      }
    }
    return builder;
  }

  @Override
  public BraveSpanBuilder newSpan(final String id, final String... tags) {
    if (tags == null) {
      throw new IllegalArgumentException("Tags cannot be null.");
    }
    if (tags.length % 2 != 0) {
      throw new IllegalArgumentException("Must have an even number of tags.");
    }
    final BraveSpanBuilder builder = 
        (BraveSpanBuilder) BraveSpan.newBuilder(this, id);
    // double-check lock to avoid contention after the first span is set
    if (!first_span_set) {
      synchronized (this) {
        if (!first_span_set) {
          builder.isFirst(true);
          first_span_set = true;
        }
      }
    }
    for (int i = 0; i < tags.length; i += 2) {
      if (Strings.isNullOrEmpty(tags[i])) {
        throw new IllegalArgumentException("Cannot have a null or empty tag key.");
      }
      if (Strings.isNullOrEmpty(tags[i + 1])) {
        throw new IllegalArgumentException("Cannot have a null or empty tag value.");
      }
      builder.withTag(tags[i], tags[i + 1]);
    }
    return builder;
  }
  
  @Override
  public BraveSpanBuilder newSpanWithThread(final String id) {
    final BraveSpanBuilder builder = 
        (BraveSpanBuilder) BraveSpan.newBuilder(this, id);
    // double-check lock to avoid contention after the first span is set
    if (!first_span_set) {
      synchronized (this) {
        if (!first_span_set) {
          builder.isFirst(true);
          first_span_set = true;
        }
      }
    }
    builder.withTag("startThread", Thread.currentThread().getName());
    return builder;
  }

  @Override
  public BraveSpanBuilder newSpanWithThread(final String id, final String... tags) {
    if (tags == null) {
      throw new IllegalArgumentException("Tags cannot be null.");
    }
    if (tags.length % 2 != 0) {
      throw new IllegalArgumentException("Must have an even number of tags.");
    }
    final BraveSpanBuilder builder = 
        (BraveSpanBuilder) BraveSpan.newBuilder(this, id);
    // double-check lock to avoid contention after the first span is set
    if (!first_span_set) {
      synchronized (this) {
        if (!first_span_set) {
          builder.isFirst(true);
          first_span_set = true;
        }
      }
    }
    for (int i = 0; i < tags.length; i += 2) {
      if (Strings.isNullOrEmpty(tags[i])) {
        throw new IllegalArgumentException("Cannot have a null or empty tag key.");
      }
      if (Strings.isNullOrEmpty(tags[i + 1])) {
        throw new IllegalArgumentException("Cannot have a null or empty tag value.");
      }
      builder.withTag(tags[i], tags[i + 1]);
    }
    builder.withTag("startThread", Thread.currentThread().getName());
    return builder;
  }
  
  @Override
  public boolean isDebug() {
    return is_debug;
  }
  
  @Override
  public String traceId() {
    if (first_span == null) {
      throw new IllegalArgumentException("No spans have been recorded.");
    }
    if (!(first_span.implementationSpan() instanceof brave.opentracing.BraveSpan)) {
      throw new IllegalArgumentException("Span was not a Brave span. Make "
          + "sure you're using the proper tracing plugins");
    }
    final brave.opentracing.BraveSpan span = 
        (brave.opentracing.BraveSpan) first_span.implementationSpan();
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
  
  @Override
  public Span firstSpan() {
    return first_span;
  }
  
  public static BraveTraceBuilder newBuilder() {
    return new BraveTraceBuilder();
  }

  /**
   * Builder for the trace.
   */
  static class BraveTraceBuilder {
    private boolean is128;
    private String id;
    private SpanCatcher span_catcher;
    private boolean is_debug;
    
    public BraveTraceBuilder setIs128(final boolean is128) {
      this.is128 = is128;
      return this;
    }
    
    public BraveTraceBuilder setId(final String id) {
      if (Strings.isNullOrEmpty(id)) {
        throw new IllegalArgumentException("ID cannot be null or empty.");
      }
      this.id = id;
      return this;
    }
    
    public BraveTraceBuilder setSpanCatcher(final SpanCatcher span_catcher) {
      this.span_catcher = span_catcher;
      return this;
    }
    
    public BraveTraceBuilder setIsDebug(final boolean is_debug) {
      this.is_debug = is_debug;
      return this;
    }
    
    public Trace build() {
      if (Strings.isNullOrEmpty(id)) {
        throw new IllegalArgumentException("ID cannot be null or empty.");
      }
      return new BraveTrace(this);   
    }
  }
  
  /**
   * TODO - find a better home for this.
   * @param name
   * @param json
   */
  public void serializeJSON(final String name, final JsonGenerator json) {
    zipkin.Span last_span = null;
    try {
      json.writeArrayFieldStart(name);
      for (final zipkin.Span span : span_catcher.spans) {
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
  
  /**
   * TODO - find a better home for this.
   * @return
   */
  public String serializeToString() {
    final StringBuilder buf = new StringBuilder()
        .append("[");
    int i = 0;
    for (final zipkin.Span span : span_catcher.spans) {
      if (i++ > 0) {
        buf.append(",");
      }
      buf.append(span.toString());
    }
    buf.append("]");
    return buf.toString();
  }

  
  void setFirstSpan(final Span span) {
    first_span = span;
  }
  
  io.opentracing.Tracer trace() {
    return tracer;
  }
}
