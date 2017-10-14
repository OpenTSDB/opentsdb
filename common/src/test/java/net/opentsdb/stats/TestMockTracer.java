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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import net.opentsdb.stats.MockTracer.MockSpan;

public class TestMockTracer {

  @Test
  public void ctor() {
    final MockTracer tracer = new MockTracer();
    assertEquals(0, tracer.span_timestamp.get());
    assertTrue(tracer.spans.isEmpty());
  }
  
  @Test
  public void span() {
    final MockTracer tracer = new MockTracer();
    Span span = tracer.newSpan("test_span")
        .withTag("key", "value")
        .start();
    assertEquals("test_span", ((MockSpan) span).id);
    assertNull(((MockSpan) span).parent);
    assertEquals(0, ((MockSpan) span).start);
    assertEquals(0, ((MockSpan) span).end);
    assertEquals(1, ((MockSpan) span).tags.size());
    assertEquals("value", ((MockSpan) span).tags.get("key"));
    assertTrue(tracer.spans.isEmpty());
    
    span.finish();
    assertEquals(1, tracer.spans.size());
    assertSame(span, tracer.spans.get(0));
    assertEquals(1, ((MockSpan) span).end);
  }
  
  @Test
  public void spanWithParent() {
    final MockTracer tracer = new MockTracer();
    Span span = tracer.newSpan("parent")
        .withTag("key", "value")
        .start();
    
    Span child = tracer.newSpan("child")
        .asChildOf(span)
        .withTag("key", "kid")
        .start();
    
    assertSame(span, ((MockSpan) child).parent);
    
    child.finish();
    span.finish();
    
    assertEquals(2, tracer.spans.size());
    assertSame(child, tracer.spans.get(0));
    assertSame(span, tracer.spans.get(1));
    assertEquals(2, ((MockSpan) child).end);
    assertEquals(3, ((MockSpan) span).end);
  }
}
