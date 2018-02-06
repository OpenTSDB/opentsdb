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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.stats.BraveSpan.BraveSpanBuilder;
import net.opentsdb.stats.Span.SpanBuilder;

public class TestBraveSpan {

  private BraveTrace trace;
  private io.opentracing.Tracer tracer;
  private io.opentracing.Tracer.SpanBuilder ot_builder;
  private io.opentracing.Tracer.SpanBuilder ot_builder_child;
  private io.opentracing.Span mock_span;
  private io.opentracing.Span mock_span_child;
  
  @Before
  public void before() throws Exception {
    trace = mock(BraveTrace.class);
    tracer = mock(io.opentracing.Tracer.class);
    ot_builder = mock(io.opentracing.Tracer.SpanBuilder.class);
    ot_builder_child = mock(io.opentracing.Tracer.SpanBuilder.class);
    mock_span = mock(io.opentracing.Span.class);
    mock_span_child = mock(io.opentracing.Span.class);
    
    when(trace.trace()).thenReturn(tracer);
    when(tracer.buildSpan(anyString()))
      .thenReturn(ot_builder)
      .thenReturn(ot_builder_child);
    when(ot_builder.start()).thenReturn(mock_span);
    when(ot_builder_child.start()).thenReturn(mock_span_child);
  }
  
  @Test
  public void builder() throws Exception {
    final SpanBuilder builder = BraveSpan.newBuilder(trace, "Test");
    verify(tracer, times(1)).buildSpan(anyString());
    
    builder.asChildOf(null);
    verify(ot_builder, never()).asChildOf((io.opentracing.Span) null);
    
    final Span mock_span = mock(Span.class);
    final io.opentracing.Span mock_io_span = mock(io.opentracing.Span.class);
    when(mock_span.implementationSpan()).thenReturn(mock_io_span);
    builder.asChildOf(mock_span);
    verify(ot_builder, times(1)).asChildOf(mock_io_span);
    
    builder.withTag("john", "snow");
    verify(ot_builder, times(1)).withTag("john", "snow");
    
    builder.withTag("houses", 42);
    verify(ot_builder, times(1)).withTag("houses", 42);
    
    try {
      builder.withTag(null, "snow");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      builder.withTag("", "snow");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    builder.withTag("ygritte", (String) null);
    
    try {
      builder.withTag(null, 42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      builder.withTag("", 42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      builder.withTag("ygritte", (Number) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      BraveSpan.newBuilder(null, "Test");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      BraveSpan.newBuilder(trace, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      BraveSpan.newBuilder(trace, "");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void ctor() throws Exception {
    BraveSpan span = (BraveSpan) BraveSpan.newBuilder(trace, "Test")
        .withTag("danny", "targy")
        .start();
    verify(ot_builder, times(1)).start();
    verify(trace, never()).setFirstSpan(any(Span.class));
    assertSame(mock_span, span.implementationSpan());
    
    span = (BraveSpan) BraveSpan.newBuilder(trace, "Test")
        .withTag("danny", "targy")
        .isFirst(true)
        .start();
    verify(ot_builder, times(1)).start();
    verify(trace, times(1)).setFirstSpan(any(Span.class));
    assertSame(mock_span_child, span.implementationSpan());
  }
  
  @Test
  public void finish() throws Exception {
    BraveSpan span = (BraveSpan) BraveSpan.newBuilder(trace, "Test")
        .withTag("danny", "targy")
        .start();
    verify(ot_builder, times(1)).start();
    verify(trace, never()).setFirstSpan(any(Span.class));
    assertSame(mock_span, span.implementationSpan());
    
    span.finish();
    verify(mock_span, times(1)).finish();
    
    span.finish(42L);
    verify(mock_span, times(1)).finish(42L);
  }
 
  @Test
  public void setSuccessTags() throws Exception {
    BraveSpan span = (BraveSpan) BraveSpan.newBuilder(trace, "Test")
        .start();
    span.setSuccessTags();
    verify(mock_span, times(1)).setTag("status", "OK");
    verify(mock_span, times(1)).setTag(eq("finalThread"), anyString());
  }
  
  @Test
  public void setErrorTags() throws Exception {
    BraveSpan span = (BraveSpan) BraveSpan.newBuilder(trace, "Test")
        .start();
    span.setErrorTags();
    verify(mock_span, times(1)).setTag("status", "Error");
    verify(mock_span, times(1)).setTag(eq("finalThread"), anyString());
  }
  
  @Test
  public void setTagsLogs() throws Exception {
    BraveSpan span = (BraveSpan) BraveSpan.newBuilder(trace, "Test")
        .start();
    verify(ot_builder, times(1)).start();
    verify(trace, never()).setFirstSpan(any(Span.class));
    assertSame(mock_span, span.implementationSpan());
    
    assertSame(span, span.setTag("danny", "targy"));
    verify(mock_span, times(1)).setTag("danny", "targy");
    
    try {
      span.setTag(null, "targy");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      span.setTag("", "targy");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    assertSame(span, span.setTag("eddy", (String) null));
    verify(mock_span, times(1)).setTag("eddy", (String) null);
   
    assertSame(span, span.setTag("houses", 42));
    verify(mock_span, times(1)).setTag("houses", 42);
    
    try {
      span.setTag(null, 42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      span.setTag("", 42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      span.setTag("houses", (Number) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    final Exception ex = new RuntimeException("Boo!");
    assertSame(span, span.log("error", ex));
    verify(mock_span, times(1)).log("error", ex);
    
    try {
      span.log(null, ex);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      span.log("", ex);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      span.log("houses", null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void newChild() throws Exception {
    BraveSpan span = (BraveSpan) BraveSpan.newBuilder(trace, "Test")
        .start();
    
    BraveSpanBuilder builder = span.newChild("child");
    BraveSpan child = (BraveSpan) builder.start();
    assertSame(mock_span_child, child.implementationSpan());
  }
}
