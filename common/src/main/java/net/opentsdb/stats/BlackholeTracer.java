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
package net.opentsdb.stats;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;

/**
 * When a tracing plugin is not loaded, this class should be passed
 * around so that we avoid a ton of {@code if (span != null)...} 
 * checks.
 * 
 * @since 3.0
 */
public class BlackholeTracer implements Tracer, Trace, Span, Span.SpanBuilder {

  @Override
  public void finish() { }

  @Override
  public void finish(final long duration) {  }

  @Override
  public Span setSuccessTags() {
    return this;
  }

  @Override
  public Span setErrorTags() {
    return this;
  }

  @Override
  public Span setErrorTags(final Throwable t) {
    return this;
  }

  @Override
  public Span setTag(final String key, final String value) {
    return this;
  }

  @Override
  public Span setTag(final String key, final Number value) {
    return this;
  }

  @Override
  public Span log(final String key, final Throwable t) {
    return this;
  }

  @Override
  public Object implementationSpan() {
    return this;
  }

  @Override
  public SpanBuilder newChild(final String id) {
    return this;
  }

  @Override
  public boolean isDebug() {
    return false;
  }

  @Override
  public SpanBuilder asChildOf(final Span parent) {
    return this;
  }

  @Override
  public SpanBuilder withTag(final String key, final String value) {
    return this;
  }

  @Override
  public SpanBuilder withTag(final String key, final Number value) {
    return this;
  }

  @Override
  public SpanBuilder buildSpan(final String id) {
    return this;
  }

  @Override
  public Span start() {
    return this;
  }

  @Override
  public SpanBuilder newSpan(final String id) {
    return this;
  }

  @Override
  public SpanBuilder newSpan(final String id, final String... tags) {
    return this;
  }

  @Override
  public SpanBuilder newSpanWithThread(final String id) {
    return this;
  }

  @Override
  public SpanBuilder newSpanWithThread(final String id, 
                                       final String... tags) {
    return this;
  }

  @Override
  public String traceId() {
    return "Null";
  }

  @Override
  public Span firstSpan() {
    return this;
  }

  @Override
  public String id() {
    return "BlackholeTracer";
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public Trace newTrace(final boolean report, final boolean debug) {
    return this;
  }

  @Override
  public Trace newTrace(final boolean report, 
                        final boolean debug, 
                        final String service_name) {
    return this;
  }

}
