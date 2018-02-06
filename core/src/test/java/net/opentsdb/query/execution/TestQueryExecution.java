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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.stumbleupon.async.TimeoutException;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.pojo.TimeSeriesQuery;

public class TestQueryExecution {

  private QueryContext context;
  private TimeSeriesQuery query;
  private Tracer tracer;
  private SpanBuilder span_builder;
  private Span span;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryContext.class);
    query = mock(TimeSeriesQuery.class);
    tracer = mock(Tracer.class);
    span_builder = mock(SpanBuilder.class);
    span = mock(Span.class);
    
    when(context.getTracer()).thenReturn(tracer);
    when(tracer.buildSpan(anyString())).thenReturn(span_builder);
    when(span_builder.start()).thenReturn(span);
  }
  
  @Test
  public void ctor() throws Exception {
    final TestImp exec = new TestImp(query);
    assertNotNull(exec.deferred());
    assertSame(query, exec.query());
    assertFalse(exec.completed());
    assertNull(exec.tracerSpan());
    
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
  }
  
  @Test
  public void callback() throws Exception {
    TestImp exec = new TestImp(query);
    assertFalse(exec.completed());
    
    exec.callback(42L);
    assertEquals(42L, (long) exec.deferred().join());
    assertTrue(exec.completed());
    
    exec = new TestImp(query);
    assertFalse(exec.completed());
    
    exec.callback(new IllegalArgumentException("Boo!"));
    try {
      exec.deferred().join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertTrue(exec.completed());
    
    try {
      exec.callback(42L);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void callbackWithTraceSet() throws Exception {
    TestImp exec = new TestImp(query);
    exec.tracer_span = span;
    assertFalse(exec.completed());
    verify(span, never()).finish();
    
    exec.callback(42L);
    assertEquals(42L, (long) exec.deferred().join());
    assertTrue(exec.completed());
    verify(span, times(1)).finish();
    
    exec = new TestImp(query);
    span = mock(Span.class);
    exec.tracer_span = span;
    assertFalse(exec.completed());
    verify(span, never()).finish();
    
    exec.callback(new IllegalArgumentException("Boo!"));
    try {
      exec.deferred().join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertTrue(exec.completed());
    verify(span, times(1)).finish();
    
    try {
      exec.callback(42L);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    verify(span, times(1)).finish();
  }
  
  @Test
  public void callbackWithTraceSetAndTags() throws Exception {
    final Map<String, String> tags = Maps.newHashMap();
    tags.put("key", "value");
    
    TestImp exec = new TestImp(query);
    exec.tracer_span = span;
    assertFalse(exec.completed());
    verify(span, never()).finish();
    
    exec.callback(42L, tags);
    assertEquals(42L, (long) exec.deferred().join());
    assertTrue(exec.completed());
    verify(span, times(1)).setTag("key", "value");
    verify(span, times(1)).finish();
    
    exec = new TestImp(query);
    span = mock(Span.class);
    exec.tracer_span = span;
    assertFalse(exec.completed());
    verify(span, never()).finish();
    
    exec.callback(new IllegalArgumentException("Boo!"), tags);
    try {
      exec.deferred().join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertTrue(exec.completed());
    verify(span, times(1)).finish();
    verify(span, times(1)).setTag("key", "value");
    
    try {
      exec.callback(42L);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    verify(span, times(1)).finish();
  }
  
  @Test
  public void callbackWithTraceSetAndTagsAndLogs() throws Exception {
    final Map<String, String> tags = Maps.newHashMap();
    tags.put("key", "value");
    final Map<String, Object> logs = Maps.newHashMap();
    logs.put("log", 42L);
    
    TestImp exec = new TestImp(query);
    exec.tracer_span = span;
    assertFalse(exec.completed());
    verify(span, never()).finish();
    
    exec.callback(42L, tags, logs);
    assertEquals(42L, (long) exec.deferred().join());
    assertTrue(exec.completed());
    verify(span, times(1)).setTag("key", "value");
    verify(span, times(1)).log(logs);
    verify(span, times(1)).finish();
    
    exec = new TestImp(query);
    span = mock(Span.class);
    exec.tracer_span = span;
    assertFalse(exec.completed());
    verify(span, never()).finish();
    
    exec.callback(new IllegalArgumentException("Boo!"), tags, logs);
    try {
      exec.deferred().join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertTrue(exec.completed());
    verify(span, times(1)).finish();
    verify(span, times(1)).setTag("key", "value");
    verify(span, times(1)).log(logs);
    
    try {
      exec.callback(42L);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    verify(span, times(1)).finish();
    
    exec = new TestImp(query);
    span = mock(Span.class);
    exec.tracer_span = span;
    assertFalse(exec.completed());
    
    // nulls ok
    exec.callback(42L, null, logs);
    assertEquals(42L, (long) exec.deferred().join());
    assertTrue(exec.completed());
    verify(span, never()).setTag("key", "value");
    verify(span, times(1)).log(logs);
    verify(span, times(1)).finish();
  }
  
  @Test
  public void cancel() throws Exception {
    TestImp exec = new TestImp(query);
    assertFalse(exec.completed());
    
    exec.cancel();
    
    assertEquals(-1L, (long) exec.deferred().join());
    assertTrue(exec.completed());
  }
  
  @Test
  public void setSpan() throws Exception {
    TestImp exec = new TestImp(query);
    assertNull(exec.tracerSpan());
    
    exec.setSpan(context, "Woot!");
    verify(tracer, times(1)).buildSpan("Woot!");
    verify(span_builder, times(1)).start();
    verify(span_builder, never()).withTag(anyString(), anyString());
    verify(span_builder, never()).asChildOf(any(Span.class));
    assertSame(span, exec.tracerSpan());
    
    try {
      exec.setSpan(context, "Woot!");
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    exec = new TestImp(query);
    try {
      exec.setSpan(null, "Woot!");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      exec.setSpan(context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      exec.setSpan(context, "");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(context.getTracer()).thenReturn(null);
    exec.setSpan(context, "Woot!");
    assertNull(exec.tracerSpan());
  }
  
  @Test
  public void setSpanWithParent() throws Exception {
    final Span parent = mock(Span.class);
    TestImp exec = new TestImp(query);
    assertNull(exec.tracerSpan());
    
    exec.setSpan(context, "Woot!", parent);
    verify(tracer, times(1)).buildSpan("Woot!");
    verify(span_builder, times(1)).start();
    verify(span_builder, never()).withTag(anyString(), anyString());
    verify(span_builder, times(1)).asChildOf(parent);
    assertSame(span, exec.tracerSpan());
    
    try {
      exec.setSpan(context, "Woot!", parent);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    exec = new TestImp(query);
    try {
      exec.setSpan(context, "Woot!", (Span) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void setSpanWithParentAndTags() throws Exception {
    final Map<String, String> tags = Maps.newHashMap();
    tags.put("key", "value");
    final Span parent = mock(Span.class);
    TestImp exec = new TestImp(query);
    assertNull(exec.tracerSpan());
    
    exec.setSpan(context, "Woot!", parent, tags);
    verify(tracer, times(1)).buildSpan("Woot!");
    verify(span_builder, times(1)).start();
    verify(span_builder, times(1)).withTag("key", "value");
    verify(span_builder, times(1)).asChildOf(parent);
    assertSame(span, exec.tracerSpan());
    
    try {
      exec.setSpan(context, "Woot!", parent, tags);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    exec = new TestImp(query);
    exec.setSpan(context, "Woot!", null, null);
  }
  
  class TestImp extends QueryExecution<Long> {

    public TestImp(TimeSeriesQuery query) {
      super(query);
    }

    @Override
    public void cancel() {
      callback(-1L);
    }
    
  }
}
