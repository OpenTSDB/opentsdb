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

import java.util.Collections;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import io.opentracing.Span;
import io.opentracing.Tracer;

/**
 * A unique trace object for a given operation in OpenTSDB. IT contains a 
 * reference to the tracer used to generate spans as well as methods to
 * serialize the final results of the trace.
 * <p>
 * Callers receiving a new trace should store the first span via 
 * {@link #setFirstSpan(Span)}.
 * 
 * @since 3.0
 */
public abstract class TsdbTrace {
  
  /** A non-null tracer to generate spans from. */
  private final Tracer tracer;
  
  /** The first span in the (local) trace. */
  private Span first_span;
  
  /** The trace ID, set when the first span is recorded. */
  private String trace_id;
  
  /**
   * Default ctor that stores the tracer.
   * @param tracer A non-null tracer.
   * @throws IllegalArgumentException if the tracer was null.
   */
  public TsdbTrace(final Tracer tracer) {
    if (tracer == null) {
      throw new IllegalArgumentException("Tracer cannot be null.");
    }
    this.tracer = tracer;
  }
  
  /** @return The tracer that will generate spans. */
  public Tracer tracer() {
    return tracer;
  }
  
  /** @param span The first span of the trace. */
  public void setFirstSpan(final Span span) {
    this.first_span = span;
    trace_id = extractTraceId(span);
  }
  
  /** @return The first span of the trace if set. */
  public Span getFirstSpan() {
    return first_span;
  }
  
  /**
   * @return The trace ID after {@link #setFirstSpan(Span)} has been called. Null
   * otherwise.
   */
  public String getTraceId() {
    return trace_id;
  }
  
  /**
   * Serialize the trace as JSON using the given JsonGenerator. The name is 
   * applied to the array or object.
   * @param name A non-null name to apply to the array or object.
   * @param json A non-null generator to write to.
   */
  public abstract void serializeJSON(final String name, final JsonGenerator json);
  
  /** @return The trace as a string. */
  public abstract String serializeToString();
  
  /**
   * Casts the span back to the proper implementation so the trace ID can be
   * extracted.
   * @param span A non-null span.
   * @return A non-null trace ID in hex.
   * @throws IllegalArgumentException if the span was null or the span/context
   * were of the wrong implementation.
   * @throws IllegalStateException if the context was null. Shouldn't happen.
   */
  protected abstract String extractTraceId(final Span span);
  
  /**
   * Sets the default successful span tags including "status=OK" and 
   * "finalThread=&lt;local_thread_name&gt;". 
   * @return A non-null immutable map of tags.
   */
  public static Map<String, String> successfulTags() {
    return successfulTags((String[]) null);
  }
  
  /**
   * Sets the default successful span tags including "status=OK" and 
   * "finalThread=&lt;local_thread_name&gt;" along with the given tags.
   * Note that if the tag count is uneven, the last tag key will be dropped.
   * Null keys or values are also skipped.
   * @param tags An optional list of tag key and tag value pairs in K, V, K, V
   * order.
   * @return A non-null immutable map of tags.
   */
  public static Map<String, String> successfulTags(final String... tags) {
    final Builder<String, String> builder = 
        new ImmutableMap.Builder<String, String>()
        .put("status", "OK")
        .put("finalThread", Thread.currentThread().getName());
    if (tags != null) {
      return addTags(builder, tags);
    }
    return builder.build();
  }
  
  /**
   * Sets the default canceled span tags including "status=Canceled", 
   * "error=&lt;exception.message()&gt;" and 
   * "finalThread=&lt;local_thread_name&gt;".
   * @param e An optional exception to pull a message from.
   * @return A non-null immutable map of tags.
   */
  public static Map<String, String> canceledTags(final Exception e) {
    return canceledTags(e, (String[]) null);
  }
  
  /**
   * Sets the default canceled span tags including "status=Canceled", 
   * "error=&lt;exception.message()&gt;" and 
   * "finalThread=&lt;local_thread_name&gt;" along with the given tags.
   * @param e An optional exception to pull a message from.
   * @param tags An optional list of tag key and tag value pairs in K, V, K, V
   * order.
   * @return A non-null immutable map of tags.
   */
  public static Map<String, String> canceledTags(final Exception e, 
                                                 final String... tags) {
    final Builder<String, String> builder = 
        new ImmutableMap.Builder<String, String>()
        .put("status", "Canceled")
        .put("error", e != null ? e.getMessage() : "Canceled")
        .put("finalThread", Thread.currentThread().getName());
    if (tags != null) {
      return addTags(builder, tags);
    }
    return builder.build();
  }
  
  /**
   * Sets the default exception span tags including "status=Error", 
   * "error=&lt;exception.message()&gt;" and 
   * "finalThread=&lt;local_thread_name&gt;".
   * @param e An optional exception to pull a message from.
   * @return A non-null immutable map of tags.
   */
  public static Map<String, String> exceptionTags(final Exception e) {
    return exceptionTags(e, (String[]) null);
  }
  
  /**
   * Sets the default exception span tags including "status=Error", 
   * "error=&lt;exception.message()&gt;" and 
   * "finalThread=&lt;local_thread_name&gt;" along with the given tags.
   * @param e An optional exception to pull a message from.
   * @param tags An optional list of tag key and tag value pairs in K, V, K, V
   * order.
   * @return A non-null immutable map of tags.
   */
  public static Map<String, String> exceptionTags(final Exception e,
                                                  final String... tags) {
    final Builder<String, String> builder = 
        new ImmutableMap.Builder<String, String>()
        .put("status", "Error")
        .put("error", e != null ? e.getMessage() : "Unknown")
        .put("finalThread", Thread.currentThread().getName());
    if (tags != null) {
      return addTags(builder, tags);
    }
    return builder.build();
  }
  
  /**
   * Sets given exception as the "exception" annotation for the span.
   * @param e A non-null exception.
   * @return A non-null map of annotations.
   */
  public static Map<String, Object> exceptionAnnotation(final Exception e) {
    return exceptionAnnotation(e, (Object[]) null);
  }
  
  /**
   * Sets given exception as the "exception" annotation for the span.
   * @param e A non-null exception.
   * @param notes An optional set of notes to add in the pattern String, Object, 
   * String, Object... Any nulls or non-String keys are skipped.
   * @return A non-null map of annotations.
   */
  public static Map<String, Object> exceptionAnnotation(final Exception e,
                                                        final Object... notes) {
    final Builder<String, Object> builder = 
        new ImmutableMap.Builder<String, Object>()
          .put("exception", e == null ? "null" : e);
    if (notes != null) {
      return annotations(builder, notes);
    }
    return builder.build();
  }
  
  /**
   * Returns a map of annotation.
   * @param notes An optional set of notes to add in the pattern String, Object, 
   * String, Object... Any null keys or non-String keys are skipped.
   * @return A non-null map of annotations. May be empty if the notes were null.
   */
  public static Map<String, Object> annotations(final Object... notes) {
    if (notes == null) {
      return Collections.<String, Object>emptyMap();
    }
    return annotations(new ImmutableMap.Builder<String, Object>(), notes);
  }
  
  /**
   * Returns a map of annotation.
   * @param builder A non-null builder.
   * @param notes An optional set of notes to add in the pattern String, Object, 
   * String, Object... Any null keys or non-String keys are skipped.
   * @return A non-null map of annotations. May be empty if the notes were null.
   */
  private static Map<String, Object> annotations(
      final Builder<String, Object> builder, final Object... notes) {
    for (int i = 0; i < notes.length; i++) {

      if (i + 1 >= notes.length) {
        break; // one short!
      }
      if (notes[i] == null || !(notes[i] instanceof String) || 
          notes[i + 1] == null) {
        i++;
        continue;
      }
      builder.put((String) notes[i++], notes[i]);
    }
    return builder.build();
  }
  
  /**
   * Creates a map of tags for tracing.
   * @param tags An optional list of tag key and tag value pairs in K, V, K, V
   * order.
   * @return A non-null immutable map of tags.
   */
  public static Map<String, String> addTags(final String... tags) {
    return addTags(new ImmutableMap.Builder<String, String>(), tags);
  }
  
  /**
   * Adds the given tags to the builder.
   * @param builder A non-null builder to populate.
   * @param tags
   * @return A non-null list of tag key and tag value pairs in K, V, K, V
   * order.
   */
  private static Map<String, String> addTags(final Builder<String, String> builder, 
                                             final String... tags) {
    for (int i = 0; i < tags.length; i++) {
      if (i + 1 >= tags.length) {
        break; // one short!
      }
      // skip nulls.
      if (tags[i] == null || tags[i + 1] == null) {
        i++;
        continue;
      }
      builder.put(tags[i++], tags[i]);
    }
    return builder.build();
  }
}
