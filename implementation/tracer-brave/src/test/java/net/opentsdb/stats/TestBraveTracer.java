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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.google.common.collect.Maps;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.stats.BraveTrace.BraveTraceBuilder;
import net.opentsdb.stats.BraveTracer.SpanCatcher;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.okhttp3.OkHttpSender;

public class TestBraveTracer {

  private MockedStatic<BraveTrace> mockedBraveTrace;
  private MockedStatic<OkHttpSender> mockedOkHttpSender;
  private MockedStatic<AsyncReporter> mockedAsyncReporter;

  private DefaultTSDB tsdb;
  private Configuration config;
  private OkHttpSender sender;
  private AsyncReporter<zipkin2.Span> reporter;
  private AsyncReporter.Builder reporter_builder;
  private Trace trace;
  private BraveTraceBuilder tracer_builder;
  private Map<String, String> config_map;
  
  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    mockedBraveTrace = Mockito.mockStatic(BraveTrace.class);
    tsdb = mock(DefaultTSDB.class);
    config_map = Maps.newHashMap();
    config = UnitTestConfiguration.getConfiguration(config_map);
    sender = mock(OkHttpSender.class);
    reporter = mock(AsyncReporter.class);
    reporter_builder = mock(AsyncReporter.Builder.class);
    trace = Mockito.mock(Trace.class);
    tracer_builder = Mockito.mock(BraveTraceBuilder.class);

    when(tsdb.getConfig()).thenReturn(config);
    mockedOkHttpSender = Mockito.mockStatic(OkHttpSender.class);
    mockedOkHttpSender.when(() -> OkHttpSender.create(anyString())).thenReturn(sender);
    mockedAsyncReporter = Mockito.mockStatic(AsyncReporter.class);
    mockedAsyncReporter.when(() -> AsyncReporter.builder(sender)).thenReturn(reporter_builder);
    when(reporter_builder.build()).thenReturn(reporter);
    mockedBraveTrace.when(BraveTrace::newBuilder).thenReturn(tracer_builder);
    
    config_map.put(BraveTracer.SERVICE_NAME_KEY, "UnitTest");
    config_map.put(BraveTracer.ENDPOINT_KEY, 
        "http://127.0.0.1:9411/api/v1/spans");
    
    when(tracer_builder.setIs128(anyBoolean())).thenReturn(tracer_builder);
    when(tracer_builder.setIsDebug(anyBoolean())).thenReturn(tracer_builder);
    when(tracer_builder.setId(anyString())).thenReturn(tracer_builder);
    when(tracer_builder.build()).thenReturn(trace);
  }

  @After
  public void tearDownStaticMocks() {
    mockedBraveTrace.closeOnDemand();
    if (mockedOkHttpSender != null) mockedOkHttpSender.close();
    if (mockedAsyncReporter != null) mockedAsyncReporter.close();
  }
  
  @Test
  public void initializeWithoutReporting() throws Exception {
    config_map.put(BraveTracer.ENDPOINT_KEY, (String) null);
    
    BraveTracer plugin = new BraveTracer();
    assertNull(plugin.initialize(tsdb, null).join());
    mockedOkHttpSender.verify(
        () -> OkHttpSender.create("http://127.0.0.1:9411/api/v1/spans"), never());
    verify(reporter_builder, never()).build();
    assertEquals("UnitTest", plugin.serviceName());
  }
  
  @Test
  public void initializeWithReporting() throws Exception {
    BraveTracer plugin = new BraveTracer();
    assertNull(plugin.initialize(tsdb, null).join());
    mockedOkHttpSender.verify(
        () -> OkHttpSender.create("http://127.0.0.1:9411/api/v1/spans"), times(1));
    verify(reporter_builder, times(1)).build();
    assertEquals("UnitTest", plugin.serviceName());
  }
  
  @Test
  public void initializeNullTSD() throws Exception {
    try {
      new BraveTracer().initialize(null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void initializeNoServiceName() throws Exception {
    config_map.put("tsdb.tracer.service_name", null);
    try {
      new BraveTracer().initialize(tsdb, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void initializeEmptyServiceName() throws Exception {
    config_map.put("tsdb.tracer.service_name", "");
    try {
      new BraveTracer().initialize(tsdb, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { } 
  }
  
  @Test
  public void initializationSenderException() throws Exception {
    config_map.put("tsdb.tracer.service_name", "UnitTest");
    mockedOkHttpSender.when(() -> OkHttpSender.create(anyString()))
      .thenThrow(new IllegalArgumentException("Boo!"));
    BraveTracer plugin = new BraveTracer();
    plugin.initialize(tsdb, null);
    assertNull(plugin.sender());
    assertNull(plugin.reporter());
  }
  
  @Test
  public void newTraceReportAndDebug() throws Exception {
    BraveTracer plugin = new BraveTracer();
    plugin.initialize(tsdb, null).join();
    
    Trace new_trace = plugin.newTrace(true, true);
    verify(tracer_builder, times(1)).setIs128(true);
    verify(tracer_builder, times(1)).setIsDebug(true);
    verify(tracer_builder, times(1)).setId("UnitTest");
    verify(tracer_builder, times(1)).setSpanCatcher(any(SpanCatcher.class));
    assertSame(trace, new_trace);
  }
  
  @Test
  public void newTraceReportAndDebugNamed() throws Exception {
    BraveTracer plugin = new BraveTracer();
    plugin.initialize(tsdb, null).join();
    
    Trace new_trace = plugin.newTrace(true, true, "Boo!");
    verify(tracer_builder, times(1)).setIs128(true);
    verify(tracer_builder, times(1)).setIsDebug(true);
    verify(tracer_builder, times(1)).setId("Boo!");
    verify(tracer_builder, times(1)).setSpanCatcher(any(SpanCatcher.class));
    assertSame(trace, new_trace);
  }
  
  @Test
  public void newTraceNoReportAndNoDebug() throws Exception {
    BraveTracer plugin = new BraveTracer();
    plugin.initialize(tsdb, null).join();
    
    Trace new_trace = plugin.newTrace(false, false);
    verify(tracer_builder, times(1)).setIs128(true);
    verify(tracer_builder, times(1)).setIsDebug(false);
    verify(tracer_builder, times(1)).setId("UnitTest");
    verify(tracer_builder, never()).setSpanCatcher(any(SpanCatcher.class));
    assertSame(trace, new_trace);
  }
  
  @Test
  public void newTraceErrors() throws Exception {
    BraveTracer plugin = new BraveTracer();
    plugin.initialize(tsdb, null).join();
    
    try {
      plugin.newTrace(false, false, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      plugin.newTrace(false, false, "");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void shutdown() throws Exception {
    BraveTracer plugin = new BraveTracer();
    plugin.initialize(tsdb, null).join();
    
    assertNull(plugin.shutdown().join());
    verify(reporter, times(1)).flush();
    verify(reporter, times(1)).close();
    verify(sender, times(1)).close();
  }
  
  @Test
  public void shutdownNotStarted() throws Exception {
    // no problems if reporting isn't configured.
    config_map.put("tracer.brave.zipkin.endpoint", null);
    BraveTracer plugin = new BraveTracer();
    plugin.initialize(tsdb, null).join();
  }
}
