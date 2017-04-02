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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.exceptions.Reporter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import zipkin.Span;
import zipkin.reporter.AsyncReporter;
import zipkin.reporter.okhttp3.OkHttpSender;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ BraveTracer.class, AsyncReporter.class, brave.Tracer.class,
  AsyncReporter.Builder.class, OkHttpSender.class })
public class TestBraveTracer {

  private TSDB tsdb;
  private Config config;
  private OkHttpSender sender;
  private AsyncReporter<zipkin.Span> reporter;
  private AsyncReporter.Builder reporter_builder;
  private brave.Tracer tracer;
  private brave.Tracer.Builder tracer_builder;
  
  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    config = new Config(false);
    sender = mock(OkHttpSender.class);
    reporter = mock(AsyncReporter.class);
    reporter_builder = PowerMockito.mock(AsyncReporter.Builder.class);
    tracer = PowerMockito.mock(brave.Tracer.class);
    tracer_builder = PowerMockito.mock(brave.Tracer.Builder.class);
    
    when(tsdb.getConfig()).thenReturn(config);
    PowerMockito.mockStatic(OkHttpSender.class);
    when(OkHttpSender.create(anyString())).thenReturn(sender);
    PowerMockito.mockStatic(AsyncReporter.class);
    when(AsyncReporter.builder(sender)).thenReturn(reporter_builder);
    when(reporter_builder.build()).thenReturn(reporter);
    PowerMockito.mockStatic(brave.Tracer.class);
    when(brave.Tracer.newBuilder()).thenReturn(tracer_builder);
    when(tracer_builder.build()).thenReturn(tracer);
    
    config.overrideConfig("tsdb.tracer.service_name", "UnitTest");
    config.overrideConfig("tracer.brave.zipkin.endpoint", 
        "http://127.0.0.1:9411/api/v1/spans");
  }
  
  @Test
  public void initializeWithoutReporting() throws Exception {
    config.overrideConfig("tracer.brave.zipkin.endpoint", null);
    
    BraveTracer plugin = new BraveTracer();
    assertNull(plugin.initialize(tsdb).join());
    PowerMockito.verifyStatic(never());
    OkHttpSender.create("http://127.0.0.1:9411/api/v1/spans");
    verify(reporter_builder, never()).build();
    assertEquals("UnitTest", plugin.serviceName());
  }
  
  @Test
  public void initializeWithReporting() throws Exception {
    BraveTracer plugin = new BraveTracer();
    assertNull(plugin.initialize(tsdb).join());
    PowerMockito.verifyStatic(times(1));
    OkHttpSender.create("http://127.0.0.1:9411/api/v1/spans");
    verify(reporter_builder, times(1)).build();
    assertEquals("UnitTest", plugin.serviceName());
  }
  
  @Test
  public void initializeExceptions() throws Exception {
    BraveTracer plugin = new BraveTracer();
    try {
      plugin.initialize(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    config.overrideConfig("tsdb.tracer.service_name", null);
    try {
      plugin.initialize(tsdb);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    config.overrideConfig("tsdb.tracer.service_name", "");
    try {
      plugin.initialize(tsdb);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    config.overrideConfig("tsdb.tracer.service_name", "UnitTest");
    when(OkHttpSender.create(anyString()))
      .thenThrow(new IllegalArgumentException("Boo!"));
    try {
      plugin.initialize(tsdb);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void getTracer() throws Exception {
    final BraveTracer plugin = new BraveTracer();
    plugin.initialize(tsdb).join();
    
    TsdbTrace trace = plugin.getTracer(true, null);
    verify(tracer_builder, times(1)).traceId128Bit(true);
    verify(tracer_builder, times(1)).localServiceName("UnitTest");
    verify(tracer_builder, times(1)).reporter(
        (zipkin.reporter.Reporter<Span>) any(Reporter.class));
    assertNotSame(tracer, trace.tracer());
    assertTrue(trace.tracer() instanceof io.opentracing.Tracer);
    
    trace = plugin.getTracer(true, "");
    verify(tracer_builder, times(2)).localServiceName("UnitTest");
    
    trace = plugin.getTracer(true, "Override");
    verify(tracer_builder, times(1)).localServiceName("Override");
  }

  @Test
  public void shutdown() throws Exception {
    BraveTracer plugin = new BraveTracer();
    plugin.initialize(tsdb).join();
    
    assertNull(plugin.shutdown().join());
    verify(reporter, times(1)).flush();
    verify(reporter, times(1)).close();
    verify(sender, times(1)).close();
    
    // no problems if reporting isn't configured.
    config.overrideConfig("tracer.brave.zipkin.endpoint", null);
    plugin = new BraveTracer();
    plugin.initialize(tsdb).join();
  }
}
