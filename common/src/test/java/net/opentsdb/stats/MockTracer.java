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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.stats.Span.SpanBuilder;

/**
 * Class used for unit testing pipelines providing a mock tracer implementation.
 */
public class MockTracer implements Tracer {
  public final AtomicLong span_timestamp = new AtomicLong();
  public List<MockSpan> spans = Lists.newArrayList();
  public boolean is_debug;
  
  @Override
  public SpanBuilder newSpan(String id) {
    return new Builder().buildSpan(id);
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
    public final long start;
    public long end;
    public Map<String, Object> tags;
    
    protected MockSpan(final Builder builder) {
      start = span_timestamp.getAndIncrement();
      id = builder.id;
      parent = builder.parent;
      tags = builder.tags;
    }
    
    @Override
    public void finish() {
      end = span_timestamp.getAndIncrement();
      synchronized(MockTracer.this) {
        spans.add(this);
      }
    }

    @Override
    public void finish(long duration) {
      end = duration;
      synchronized(MockTracer.this) {
        spans.add(this);
      }
    }

    @Override
    public void setTag(String key, String value) {
      if (tags == null) {
        tags = Maps.newHashMap();
      }
      tags.put(key, value);
    }

    @Override
    public void setTag(String key, Number value) {
      if (tags == null) {
        tags = Maps.newHashMap();
      }
      tags.put(key, value);
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
  }
  
  public class Builder implements SpanBuilder {
    private String id;
    private Span parent;
    private Map<String, Object> tags;
    
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
      return new MockSpan(this);
    }
    
  }
}