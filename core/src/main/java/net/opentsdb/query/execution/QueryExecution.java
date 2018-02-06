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
package net.opentsdb.query.execution;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.pojo.TimeSeriesQuery;

/**
 * A state container for asynchronous queries. All {@link QueryExecutor}s should
 * return an instance of this execution when responding to a query. The query
 * client can then wait on the {@link #deferred()} for results or if it needs
 * to shutdown, it can cancel the request.
 *
 * @param <T> The type of data that's returned by the query.
 * 
 * @since 3.0
 */
public abstract class QueryExecution<T> {
  /** The query that is associated with this request. */
  protected final TimeSeriesQuery query;
  
  /** The deferred that will be called with a result (good data or an exception) */
  protected final Deferred<T> deferred;
  
  /** A thread safe boolean to use when calling or canceling. */
  protected final AtomicBoolean completed;
  
  /** An optional tracer span to log results to on completion. */
  protected Span tracer_span;
  
  /**
   * Default ctor.
   * @param query A non-null query.
   */
  public QueryExecution(final TimeSeriesQuery query) {
    this.query = query;
    deferred = new Deferred<T>();
    completed = new AtomicBoolean();
  }
  
  /** @return The deferred that will be called with a result. */
  public Deferred<T> deferred() {
    return deferred;
  }
  
  /**
   * Passes the result to the deferred and triggers it's callback chain. If the
   * {@link #tracer_span} has been set, it's marked as finished.
   * @param result A non-null result of type T or an exception.
   * @throws IllegalStateException if the deferred was already called.
   */
  protected void callback(final Object result) {
    if (completed.compareAndSet(false, true)) {
      if (tracer_span != null) {
        tracer_span.finish();
      }
      deferred.callback(result);
    } else {
      throw new IllegalStateException("Callback was already executed: " + this);
    }
  }

  /**
   * Passes the result to the deferred and triggers it's callback chain. Also 
   * sets the tracer span and adds the given stats as tags.
   * @param result A non-null result of the type T or an exception.
   * @param trace_tags An optional set of stats to tag the tracer span with.
   * @throws IllegalStateException if the deferred was already called.
   */
  protected void callback(final Object result, 
                          final Map<String, String> trace_tags) {
    callback(result, trace_tags, null);
  }
  
  /**
   * Passes the result to the deferred and triggers it's callback chain. Also 
   * sets the tracer span and adds the given stats as tags.
   * @param result A non-null result of the type T or an exception.
   * @param trace_tags An optional set of stats to tag the tracer span with.
   * @param trace_log An optional set of log events (exceptions, etc) to tag
   * the tracer span with.
   * @throws IllegalStateException if the deferred was already called.
   */
  protected void callback(final Object result, 
                       final Map<String, String> trace_tags,
                       final Map<String, Object> trace_log) {
    if (completed.compareAndSet(false, true)) {
      if (tracer_span != null) {
        if (trace_tags != null) {
          for (final Entry<String, String> stat : trace_tags.entrySet()) {
            tracer_span.setTag(stat.getKey(), stat.getValue());
          }
        }
        if (trace_log != null) {
          tracer_span.log(trace_log);
        }
        tracer_span.finish();
      }
      deferred.callback(result);
    } else {
      throw new IllegalStateException("Callback was already executed: " + this);
    }
  }
  
  /**
   * Creates and starts a new tracer span with the given ID.
   * If the tracer is set to null, this will be a no-op (but arguments are
   * still validated).
   * @param context A non-null context to fetch the {@link Tracer} from.
   * @param id A non-null and non-empty ID describing the operation.
   * @throws IllegalArgumentException if the context was null or ID was null.
   * @throws IllegalStateException if the span was already set.
   */
  protected void setSpan(final QueryContext context, final String id) {
    setSpan(context, id, null, null);
  }
  
  /**
   * Creates and starts a new tracer span with the given ID as a child of the
   * given parent.
   * If the tracer is set to null, this will be a no-op (but arguments are
   * still validated).
   * @param context A non-null context to fetch the {@link Tracer} from.
   * @param id A non-null and non-empty ID describing the operation.
   * @param parent A non-null parent span.
   * @throws IllegalArgumentException if the context, ID or parent span were
   * null.
   * @throws IllegalStateException if the span was already set.
   */
  protected void setSpan(final QueryContext context, 
                         final String id,
                         final Span parent) {
    if (parent == null) {
      throw new IllegalArgumentException("Parent span cannot be null!");
    }
    setSpan(context, id, parent, null);
  }

  /**
   * Creates and starts a new tracer span with the given ID.
   * If the tracer is set to null, this will be a no-op (but arguments are
   * still validated).
   * @param context A non-null context to fetch the {@link Tracer} from.
   * @param id A non-null and non-empty ID describing the operation.
   * @param tracer_tags An optional set of tracer tags describing the operation.
   * @throws IllegalArgumentException if the context was null or ID was null.
   * @throws IllegalStateException if the span was already set.
   */
  protected void setSpan(final QueryContext context, 
                       final String id,
                       final Map<String, String> tracer_tags) {
    setSpan(context, id, null, tracer_tags);
  }
  
  /**
   * Creates and starts a new tracer span with the given ID, optionally with
   * a parent span.
   * @param context A non-null context to fetch the {@link Tracer} from.
   * @param id A non-null and non-empty ID describing the operation.
   * @param parent An optional parent span.
   * @param tracer_tags An optional set of tracer tags describing the operation.
   * @throws IllegalArgumentException if the context was null or ID was null.
   * @throws IllegalStateException if the span was already set.
   */
  protected void setSpan(final QueryContext context, 
                         final String id,
                         final Span parent, 
                         final Map<String, String> tracer_tags) {
    if (context == null) {
      throw new IllegalArgumentException("Context cannot be null.");
    }
    if (Strings.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("Id cannot be null.");
    }
    if (context.getTracer() == null) {
      return;
    }
    if (tracer_span != null) {
      throw new IllegalStateException("Tracer span was already set.");
    }
    
    final SpanBuilder builder = context.getTracer().buildSpan(id);
    if (parent != null) {
      builder.asChildOf(parent);
    }
    if (tracer_tags != null) {
      for (final Entry<String, String> entry : tracer_tags.entrySet()) {
        builder.withTag(entry.getKey(), entry.getValue());
      }
    }
    tracer_span = builder.start();
  }
  
  /** @return The query associated with this execution. */
  public TimeSeriesQuery query() {
    return query;
  }
  
  /** @return Whether or not the query has completed and the deferred has a 
   * result. */
  public boolean completed() {
    return completed.get();
  }
  
  /**
   * Cancels the query if it's outstanding. 
   * <b>WARNING:</b> Implementations must make sure that the {@link #deferred()}
   * is called somehow when a cancellation occurs.
   * <b>Note:</b> Race conditions are possible if the implementation calls into
   * {@link #callback(Object)}. Check the {@link #completed} state.
   */
  public abstract void cancel();

  /** @return The tracer span if set, null if not. */
  public Span tracerSpan() {
    return tracer_span;
  }
}
