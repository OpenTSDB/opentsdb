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

/**
 * A tracing span wrapping one of various tracing implementations. Right now it
 * mimics the OpenTracing API: http://opentracing.io/.
 * 
 * @since 3.0
 */
public interface Span {
  
  /**
   * Called when the span measurement is finished and records the duration
   * of the span from the time {@link SpanBuilder#start()} was called. No more 
   * tags can be set after this point.
   */
  public void finish();
  
  /**
   * Called when the span measurement is finished recording the given duration
   * instead of calculating it from the start time. No more tags can be set
   * after this point.
   * @param duration A positive duration in TODO??? time units
   */
  public void finish(final long duration);
  
  /**
   * Sets the tags "status=OK" and "finalThread=&lt;local_thread_name&gt;".
   * @return The span.
   */
  public Span setSuccessTags();
  
  /**
   * Sets the tags "status=Error" and "finalThread=&lt;local_thread_name&gt;".
   * @return The span.
   */
  public Span setErrorTags();
  
  /**
   * Sets a tag on a span. May overwrite.
   * @param key A non-null and non-empty key.
   * @param value A non-null and non-empty value.
   * @return The span.
   */
  public Span setTag(final String key, final String value);
  
  /**
   * Sets a tag on a span. May overwrite.
   * @param key A non-null and non-empty key.
   * @param value A numeric value.
   * @return The span.
   */
  public Span setTag(final String key, final Number value);
  
  /**
   * Logs the given key and exception
   * @param key A non-null and non-empty key.
   * @param t A non-null exception.
   * @return The span.
   */
  public Span log(final String key, final Throwable t);
  
  /**
   * @return The implementation's span object for chaining.
   */
  public Object implementationSpan();
  
  /**
   * Creates a new child span from the current span.
   * @param id A non-null and non-empty span ID.
   * @return A new span builder with this span as the parent.
   * @throws IllegalArgumentException if the ID was null or empty.
   */
  public SpanBuilder newChild(final String id);
  
  /**
   * The builder used to construct and start a span.
   * 
   * @since 3.0
   */
  public interface SpanBuilder {
    /**
     * Adds a parent for this new span.
     * @param parent A non-null parent.
     * @return The span builder.
     */
    public SpanBuilder asChildOf(final Span parent);
    
    /**
     * Sets a tag on the span.
     * @param key A non-null and non-empty key.
     * @param value A non-null and non-empty value.
     * @return The span builder.
     */
    public SpanBuilder withTag(final String key, final String value);
    
    /**
     * Sets a tag on the span. 
     * @param key A non-null and non-empty key.
     * @param value A numeric value.
     * @return The span builder.
     */
    public SpanBuilder withTag(final String key, final Number value);
    
    /**
     * Sets the ID of the span. This should be the first method called when
     * constructing a span.
     * @param id A non-null and non-empty ID for the span.
     * @return The span builder.
     */
    public SpanBuilder buildSpan(final String id);
    
    /**
     * Constructs the span and records the current timestamp for timing purposes.
     * @return
     */
    public Span start();
  }
}
