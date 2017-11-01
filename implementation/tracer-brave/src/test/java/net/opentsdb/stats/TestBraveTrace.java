// This file is part of OpenTSDB.
// Copyright (C) 2017 The OpenTSDB Authors.
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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.stats.BraveSpan.BraveSpanBuilder;
import net.opentsdb.stats.BraveTracer.SpanCatcher;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ BraveTrace.class, brave.Tracer.class, 
  brave.opentracing.BraveTracer.class })
public class TestBraveTrace {

  private brave.Tracer brave_tracer;
  private brave.Tracer.Builder tracer_builder;
  private SpanCatcher span_catcher;
  private brave.opentracing.BraveTracer tracer;
  private io.opentracing.Tracer.SpanBuilder ot_builder;
  private io.opentracing.Tracer.SpanBuilder ot_builder_child;
  private io.opentracing.Span mock_span;
  private io.opentracing.Span mock_span_child;
  
  @Before
  public void before() throws Exception {
    brave_tracer = PowerMockito.mock(brave.Tracer.class);
    tracer_builder = PowerMockito.mock(brave.Tracer.Builder.class);
    span_catcher = mock(SpanCatcher.class);
    tracer = mock(brave.opentracing.BraveTracer.class);
    ot_builder = mock(io.opentracing.Tracer.SpanBuilder.class);
    ot_builder_child = mock(io.opentracing.Tracer.SpanBuilder.class);
    mock_span = mock(io.opentracing.Span.class);
    mock_span_child = mock(io.opentracing.Span.class);
    
    PowerMockito.mockStatic(brave.Tracer.class);
    when(brave.Tracer.newBuilder()).thenReturn(tracer_builder);
    when(tracer_builder.build()).thenReturn(brave_tracer);
   
    PowerMockito.mockStatic(brave.opentracing.BraveTracer.class);
    when(brave.opentracing.BraveTracer.wrap(any(brave.Tracer.class)))
      .thenReturn(tracer);
    
    when(tracer_builder.traceId128Bit(anyBoolean()))
      .thenReturn(tracer_builder);
    when(tracer_builder.localServiceName(anyString()))
      .thenReturn(tracer_builder);
    
    when(tracer.buildSpan(anyString()))
      .thenReturn(ot_builder)
      .thenReturn(ot_builder_child);
    when(ot_builder.start()).thenReturn(mock_span);
    when(ot_builder_child.start()).thenReturn(mock_span_child);
  }
  
  @Test
  public void builder() throws Exception {
    BraveTrace.newBuilder()
      .setId("MyTrace")
      .setIs128(true)
      .setIsDebug(true)
      .setSpanCatcher(span_catcher);
    
    BraveTrace.newBuilder()
      .setId("MyTrace")
      .setIs128(false)
      .setIsDebug(false)
      .setSpanCatcher(null);
    
    try {
      BraveTrace.newBuilder()
        .setId(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      BraveTrace.newBuilder()
        .setId("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void ctor() throws Exception {
    BraveTrace trace = (BraveTrace) BraveTrace.newBuilder()
      .setId("MyTrace")
      .setIs128(true)
      .setIsDebug(true)
      .setSpanCatcher(span_catcher)
      .build();
    
    verify(tracer_builder, times(1)).traceId128Bit(true);
    verify(tracer_builder, times(1)).localServiceName("MyTrace");
    verify(tracer_builder, times(1)).reporter(span_catcher);
    assertTrue(trace.isDebug());
    
    try {
      BraveTrace.newBuilder()
        //.setId("MyTrace")
        .setIs128(true)
        .setIsDebug(true)
        .setSpanCatcher(span_catcher)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      BraveTrace.newBuilder()
        .setId("")
        .setIs128(true)
        .setIsDebug(true)
        .setSpanCatcher(span_catcher)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void newSpan() throws Exception {
    BraveTrace trace = (BraveTrace) BraveTrace.newBuilder()
        .setId("MyTrace")
        .setIs128(true)
        .setIsDebug(true)
        .setSpanCatcher(span_catcher)
        .build();
    
    BraveSpanBuilder span_builder1 = trace.newSpan("Foo");
    assertNull(trace.firstSpan());
    
    BraveSpanBuilder span_builder2 = trace.newSpan("Foo");
    assertNull(trace.firstSpan());
    
    Span span1 = span_builder1.start();
    assertSame(span1, trace.firstSpan());
    
    span_builder2.start();
    assertSame(span1, trace.firstSpan());
  }
}
