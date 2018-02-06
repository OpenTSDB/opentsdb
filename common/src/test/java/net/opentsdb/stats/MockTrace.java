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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.stats.Span.SpanBuilder;

/**
 * Class used for unit testing pipelines providing a mock tracer implementation.
 */
public class MockTrace implements Trace {
  public final AtomicLong span_timestamp = new AtomicLong();
  public List<MockSpan> spans = Lists.newArrayList();
  public boolean is_debug;
  public Span first_span;
  
  @Override
  public SpanBuilder newSpan(final String id) {
    Builder builder = (Builder) new Builder().buildSpan(id);
    if (first_span == null) {
      builder.is_first = true;
    }
    return builder;
  }
  
  @Override
  public SpanBuilder newSpan(final String id, final String... tags) {
    if (tags == null) {
      throw new IllegalArgumentException("Tags cannot be null.");
    }
    if (tags.length % 2 != 0) {
      throw new IllegalArgumentException("Must have an even number of tags.");
    }
    Builder builder = (Builder) new Builder().buildSpan(id);
    for (int i = 0; i < tags.length; i += 2) {
      if (Strings.isNullOrEmpty(tags[i])) {
        throw new IllegalArgumentException("Cannot have a null or empty tag key.");
      }
      if (Strings.isNullOrEmpty(tags[i + 1])) {
        throw new IllegalArgumentException("Cannot have a null or empty tag value.");
      }
      builder.withTag(tags[i], tags[i + 1]);
    }
    if (first_span == null) {
      builder.is_first = true;
    }
    return builder;
  }
  
  @Override
  public SpanBuilder newSpanWithThread(final String id) {
    Builder builder = (Builder) new Builder().buildSpan(id);
    builder.withTag("startThread", Thread.currentThread().getName());
    if (first_span == null) {
      builder.is_first = true;
    }
    return builder;
  }
  
  @Override
  public SpanBuilder newSpanWithThread(final String id, final String... tags) {
    if (tags == null) {
      throw new IllegalArgumentException("Tags cannot be null.");
    }
    if (tags.length % 2 != 0) {
      throw new IllegalArgumentException("Must have an even number of tags.");
    }
    Builder builder = (Builder) new Builder().buildSpan(id);
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
    if (first_span == null) {
      builder.is_first = true;
    }
    return builder;
  }

  @Override
  public boolean isDebug() {
    return is_debug;
  }
  
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder()
        .append("isDebug=")
        .append(is_debug)
        .append(", spans=")
        .append(spans);
     return buf.toString();
  }
  
  public class MockSpan implements Span {
    public String id;
    public Span parent;
    public Object mock_span;
    public final long start;
    public long end;
    public Map<String, Object> tags;
    public Map<String, Throwable> exceptions;
    
    protected MockSpan(final Builder builder) {
      if (Strings.isNullOrEmpty(builder.id)) {
        throw new IllegalArgumentException("Span ID cannot be null.");
      }
      start = span_timestamp.getAndIncrement();
      id = builder.id;
      parent = builder.parent;
      tags = builder.tags;
      mock_span = new Object();
    }
    
    @Override
    public void finish() {
      end = span_timestamp.getAndIncrement();
      synchronized(MockTrace.this) {
        spans.add(this);
      }
    }

    @Override
    public void finish(long duration) {
      end = duration;
      synchronized(MockTrace.this) {
        spans.add(this);
      }
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
    public Span setTag(String key, String value) {
      if (tags == null) {
        tags = Maps.newHashMap();
      }
      tags.put(key, value);
      return this;
    }

    @Override
    public Span setTag(String key, Number value) {
      if (tags == null) {
        tags = Maps.newHashMap();
      }
      tags.put(key, value);
      return this;
    }
    
    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
          .append("id=")
          .append(id)
          .append(", parent=[")
          .append(parent == null ? "null" : ((MockSpan) parent).id)
          .append("], start=")
          .append(start)
          .append(", end=")
          .append(end)
          .append(", tags=")
          .append(tags);
      return buf.toString();
    }
    
    @Override
    public Object implementationSpan() {
      return mock_span;
    }

    @Override
    public Span log(String key, Throwable t) {
      if (exceptions == null) {
        exceptions = Maps.newHashMap();
      }
      exceptions.put(key, t);
      return this;
    }

    @Override
    public SpanBuilder newChild(final String id) {
      return new Builder().buildSpan(id)
          .asChildOf(this);
    }
  }
  
  public class Builder implements SpanBuilder {
    private String id;
    private Span parent;
    private Map<String, Object> tags;
    private boolean is_first;
    
    @Override
    public SpanBuilder asChildOf(Span parent) {
      this.parent = parent;
      return this;
    }

    @Override
    public SpanBuilder withTag(String key, String value) {
      if (tags == null) {
        tags = Maps.newHashMap();
      }
      tags.put(key, value);
      return this;
    }

    @Override
    public SpanBuilder withTag(String key, Number value) {
      if (tags == null) {
        tags = Maps.newHashMap();
      }
      tags.put(key, value);
      return this;
    }

    @Override
    public SpanBuilder buildSpan(String id) {
      this.id = id;
      return this;
    }

    @Override
    public Span start() {
      if (is_first) {
        first_span = new MockSpan(this);
        return first_span;
      }
      return new MockSpan(this);
    }
    
  }

  @Override
  public String traceId() {
    return "ab";
  }

  @Override
  public Span firstSpan() {
    return first_span;
  }
}
