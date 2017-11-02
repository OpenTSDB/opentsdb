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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.stats.MockTrace.MockSpan;

public class TestMockTrace {

  @Test
  public void ctor() {
    final MockTrace tracer = new MockTrace();
    assertEquals(0, tracer.span_timestamp.get());
    assertTrue(tracer.spans.isEmpty());
  }
  
  @Test
  public void newSpan() {
    final Exception e = new RuntimeException("Boo!");
    final MockTrace tracer = new MockTrace();
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
    
    span.setTag("extra", "tag");
    span.log("Error", e);
    span.finish();
    assertEquals(1, tracer.spans.size());
    assertSame(span, tracer.spans.get(0));
    assertEquals(1, ((MockSpan) span).end);
    assertEquals(2, ((MockSpan) span).tags.size());
    assertEquals(1, ((MockSpan) span).exceptions.size());
    assertSame(span, tracer.firstSpan());
    assertEquals("value", ((MockSpan) span).tags.get("key"));
    assertEquals("tag", ((MockSpan) span).tags.get("extra"));
    
    tracer.newSpan("test_span2")
      .withTag("key", "value")
      .start();
    assertSame(span, tracer.firstSpan());
    
    span = tracer.newSpan("test_span", "key2", "value2")
        .start();
    assertEquals(1, ((MockSpan) span).tags.size());
    assertEquals("value2", ((MockSpan) span).tags.get("key2"));
    
    try {
      tracer.newSpan(null).start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    try {
      tracer.newSpan("").start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    try {
      tracer.newSpan("testspan", null).start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    try {
      tracer.newSpan("testspan", "key").start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    try {
      tracer.newSpan("testspan", null, "value").start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    try {
      tracer.newSpan("testspan", "", "value").start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    try {
      tracer.newSpan("testspan", "key", null).start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    try {
      tracer.newSpan("testspan", "key", "").start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
  }
  
  @Test
  public void newSpanWithThread() {
    final Exception e = new RuntimeException("Boo!");
    final MockTrace tracer = new MockTrace();
    Span span = tracer.newSpanWithThread("test_span")
        .withTag("key", "value")
        .start();
    assertEquals("test_span", ((MockSpan) span).id);
    assertNull(((MockSpan) span).parent);
    assertEquals(0, ((MockSpan) span).start);
    assertEquals(0, ((MockSpan) span).end);
    assertEquals(2, ((MockSpan) span).tags.size());
    assertEquals("value", ((MockSpan) span).tags.get("key"));
    assertNotNull(((MockSpan) span).tags.get("startThread"));
    assertTrue(tracer.spans.isEmpty());
    
    span.setTag("extra", "tag");
    span.log("Error", e);
    span.finish();
    assertEquals(1, tracer.spans.size());
    assertSame(span, tracer.spans.get(0));
    assertEquals(1, ((MockSpan) span).end);
    assertEquals(3, ((MockSpan) span).tags.size());
    assertEquals(1, ((MockSpan) span).exceptions.size());
    assertSame(span, tracer.firstSpan());
    assertEquals("value", ((MockSpan) span).tags.get("key"));
    assertEquals("tag", ((MockSpan) span).tags.get("extra"));
    assertNotNull(((MockSpan) span).tags.get("startThread"));
    
    tracer.newSpanWithThread("test_span2")
      .withTag("key", "value")
      .start();
    assertSame(span, tracer.firstSpan());
    
    span = tracer.newSpanWithThread("test_span", "key2", "value2")
        .start();
    assertEquals(2, ((MockSpan) span).tags.size());
    assertEquals("value2", ((MockSpan) span).tags.get("key2"));
    assertNotNull(((MockSpan) span).tags.get("startThread"));
    
    try {
      tracer.newSpanWithThread(null).start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    try {
      tracer.newSpanWithThread("").start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    try {
      tracer.newSpanWithThread("testspan", null).start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    try {
      tracer.newSpanWithThread("testspan", "key").start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    try {
      tracer.newSpanWithThread("testspan", null, "value").start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    try {
      tracer.newSpanWithThread("testspan", "", "value").start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    try {
      tracer.newSpanWithThread("testspan", "key", null).start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    try {
      tracer.newSpanWithThread("testspan", "key", "").start();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
  }
  
  @Test
  public void spanWithParent() {
    final MockTrace tracer = new MockTrace();
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
  
  @Test
  public void spanChild() {
    final MockTrace tracer = new MockTrace();
    Span span = tracer.newSpan("parent")
        .withTag("key", "value")
        .start();
    
    Span child = span.newChild("child")
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
