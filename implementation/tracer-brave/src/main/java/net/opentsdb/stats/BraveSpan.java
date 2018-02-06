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

import com.google.common.base.Strings;

/**
 * An implementation of the {@link Span} class using Brave and OpenTracing.
 * 
 * @since 3.0
 */
public class BraveSpan implements net.opentsdb.stats.Span {
  
  /** A link to the constructed real span. */
  private final io.opentracing.Span span;
  
  /** A link to the trace this span belongs to (so we can spawn children) */
  private final BraveTrace trace;
  
  /**
   * Protected ctor for building.
   * @param builder The non-null builder.
   */
  protected BraveSpan(final BraveSpanBuilder builder) {
    this.span = builder.builder.start();
    trace = builder.trace;
    
    if (builder.is_first) {
      trace.setFirstSpan(this);
    }
  }
  
  @Override
  public void finish() {
    span.finish();
  }

  @Override
  public void finish(final long duration) {
    span.finish(duration);
  }

  @Override
  public Span setSuccessTags() {
    setTag("status", "OK");
    setTag("finalThread", Thread.currentThread().getName());
    return this;
  }
  
  @Override
  public Span setErrorTags() {
    setTag("status", "Error");
    setTag("finalThread", Thread.currentThread().getName());
    return this;
  }
  
  @Override
  public Span setTag(final String key, final String value) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Empty or null keys are not allowed.");
    }
    span.setTag(key, value);
    return this;
  }

  @Override
  public Span setTag(final String key, final Number value) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Empty or null keys are not allowed.");
    }
    if (value == null) {
      throw new IllegalArgumentException("Numeric values cannot be null.");
    }
    span.setTag(key, value);
    return this;
  }
  
  @SuppressWarnings("deprecation")
  @Override
  public Span log(final String key, final Throwable t) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Empty or null keys are not allowed.");
    }
    if (t == null) {
      throw new IllegalArgumentException("Null exceptions are not allowed.");
    }
    span.log(key, t);
    return this;
  }

  @Override
  public Object implementationSpan() {
    return span;
  }
  
  @Override
  public BraveSpanBuilder newChild(final String id) {
    return new BraveSpanBuilder(trace)
        .buildSpan(id)
        .asChildOf(this);
  }
  
  /**
   * Helper to return a new span builder.
   * @param trace A non-null trace.
   * @param id A non-null and non-empty span Id.
   * @return The builder.
   */
  static BraveSpanBuilder newBuilder(final BraveTrace trace, final String id) {
    return new BraveSpanBuilder(trace)
        .buildSpan(id);
  }
  
  /**
   * An implementation of the {@link net.opentsdb.stats.Span.SpanBuilder} for
   * constructing a Brave span.
   * 
   * @since 3.0
   */
  public static class BraveSpanBuilder implements net.opentsdb.stats.Span.SpanBuilder {
    private final BraveTrace trace;
    private io.opentracing.Tracer.SpanBuilder builder;
    private boolean is_first;
    
    /**
     * Package private Ctor used by the trace.
     * @param trace A non-null trace.
     */
    BraveSpanBuilder(final BraveTrace trace) {
      if (trace == null) {
        throw new IllegalArgumentException("Trace cannot be null.");
      }
      this.trace = trace;
    }
    
    @Override
    public BraveSpanBuilder asChildOf(final Span parent) {
      if (parent == null) {
        return this;
      }
      builder.asChildOf((io.opentracing.Span) parent.implementationSpan());
      return this;
    }

    @Override
    public BraveSpanBuilder withTag(final String key, final String value) {
      if (Strings.isNullOrEmpty(key)) {
        throw new IllegalArgumentException("Empty or null keys are not allowed.");
      }
      builder.withTag(key, value);
      return this;
    }

    @Override
    public BraveSpanBuilder withTag(final String key, final Number value) {
      if (Strings.isNullOrEmpty(key)) {
        throw new IllegalArgumentException("Empty or null keys are not allowed.");
      }
      if (value == null) {
        throw new IllegalArgumentException("Numeric values cannot be null.");
      }
      builder.withTag(key, value);
      return this;
    }

    @Override
    public BraveSpanBuilder buildSpan(final String id) {
      if (Strings.isNullOrEmpty(id)) {
        throw new IllegalArgumentException("Span ID may not be null or empty.");
      }
      builder = trace.trace().buildSpan(id);
      return this;
    }

    @Override
    public Span start() {
      return new BraveSpan(this);
    }

    /**
     * Package private helper to mark the builder as the first span so that it
     * calls back the set method on the trace implementation.
     * @param is_first Whether or not it's the first span in the trace.
     * @return The builder.
     */
    BraveSpanBuilder isFirst(final boolean is_first) {
      this.is_first = is_first;
      return this;
    }
  }

}
