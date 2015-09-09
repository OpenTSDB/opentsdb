// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.tsd;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import net.opentsdb.core.BaseTsdbTest.FakeTaskTimer;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.Config;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
             "ch.qos.*", "org.slf4j.*",
             "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSDB.class, Config.class, HttpQuery.class, 
  StorageExceptionHandler.class })
public final class TestPutRpc {
  private static final Map<String, String> TAGS = new HashMap<String, String>(1);
  static {
    TAGS.put("host", "web01");
  }
  private TSDB tsdb = null;
  private AtomicLong requests = new AtomicLong();
  private AtomicLong hbase_errors = new AtomicLong();
  private AtomicLong invalid_values = new AtomicLong();
  private AtomicLong illegal_arguments = new AtomicLong();
  private AtomicLong unknown_metrics = new AtomicLong();
  private AtomicLong writes_blocked = new AtomicLong();
  private StorageExceptionHandler handler;
  private FakeTaskTimer timer;
  
  @Before
  public void before() throws Exception {
    tsdb = NettyMocks.getMockedHTTPTSDB();
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, 42, TAGS))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, -42, TAGS))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, 42.2f, TAGS))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, -42.2f, TAGS))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, 4220.0f, TAGS))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, -4220.0f, TAGS))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, .0042f, TAGS))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, -0.0042f, TAGS))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.system", 1365465600, 24, TAGS))
      .thenReturn(Deferred.fromResult(new Object()));
    // errors
    when(tsdb.addPoint("doesnotexist", 1365465600, 42, TAGS))
      .thenThrow(new NoSuchUniqueName("metric", "doesnotexist"));
    when(tsdb.addPoint("sys.cpu.system", 1365465600, 1, TAGS))
      .thenReturn(Deferred.fromError(new RuntimeException("Wotcher!")));
    when(tsdb.addPoint("sys.cpu.system", 1365465600, 2, TAGS))
      .thenReturn(new Deferred<Object>());
    
    requests = Whitebox.getInternalState(PutDataPointRpc.class, "requests");
    requests.set(0);
    hbase_errors = Whitebox.getInternalState(PutDataPointRpc.class, "hbase_errors");
    hbase_errors.set(0);
    invalid_values = Whitebox.getInternalState(PutDataPointRpc.class, "invalid_values");
    invalid_values.set(0);
    illegal_arguments = Whitebox.getInternalState(PutDataPointRpc.class, "illegal_arguments");
    illegal_arguments.set(0);
    unknown_metrics = Whitebox.getInternalState(PutDataPointRpc.class, "unknown_metrics");
    unknown_metrics.set(0);
    writes_blocked = Whitebox.getInternalState(PutDataPointRpc.class, "writes_blocked");
    writes_blocked.set(0);
    
    timer = new FakeTaskTimer();
    
    handler = mock(StorageExceptionHandler.class);
    when(tsdb.getStorageExceptionHandler()).thenReturn(handler);
    when(tsdb.getTimer()).thenReturn(timer);
  }
  
  @Test
  public void constructor() {
    assertNotNull(new PutDataPointRpc());
  }
  
  // Socket RPC Tests ------------------------------------

  @Test
  public void execute() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc();
    final Channel chan = NettyMocks.fakeChannel();
    assertNotNull(put.execute(tsdb, chan, new String[] { "put", "sys.cpu.nice", 
        "1365465600", "42", "host=web01" }).joinUninterruptibly());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(chan, never()).write(any());
    verify(chan, never()).isConnected();
    verify(tsdb, never()).getStorageExceptionHandler();
  }

  @Test
  public void executeBadValue() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc();
    final Channel chan = NettyMocks.fakeChannel();
    assertNull(put.execute(tsdb, chan, new String[] { "put", "sys.cpu.nice", 
        "1365465600", "notanum", "host=web01" }).joinUninterruptibly());
    assertEquals(1, requests.get());
    assertEquals(1, invalid_values.get());
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void executeMissingMetric() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc();
    final Channel chan = NettyMocks.fakeChannel();
    assertNull(put.execute(tsdb, chan, new String[] { "put", "", 
        "1365465600", "42", "host=web01" }).joinUninterruptibly());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, illegal_arguments.get());
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void executeMissingMetricNotWriteable() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc();
    final Channel chan = NettyMocks.fakeChannel();
    when(chan.isWritable()).thenReturn(false);
    assertNull(put.execute(tsdb, chan, new String[] { "put", "", 
        "1365465600", "42", "host=web01" }).joinUninterruptibly());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, illegal_arguments.get());
    assertEquals(1, writes_blocked.get());
    verify(chan, never()).write(any());
    verify(chan, times(1)).isConnected();
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void executeUnknownMetric() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc();
    final Channel chan = NettyMocks.fakeChannel();
    assertNull(put.execute(tsdb, chan, new String[] { "put", "doesnotexist", 
        "1365465600", "42", "host=web01" }).joinUninterruptibly());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, unknown_metrics.get());
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @SuppressWarnings("unchecked")
  @Test (expected = RuntimeException.class)
  public void executeRuntimeException() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), 
        (HashMap<String, String>)any()))
        .thenThrow(new RuntimeException("Fail!"));
    
    final PutDataPointRpc put = new PutDataPointRpc();
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, new String[] { "put", "doesnotexist", 
        "1365465600", "42", "host=web01" });
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void executeHBaseError() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), 
        (HashMap<String, String>)any(HashMap.class)))
        .thenReturn(Deferred.fromError(new RuntimeException("Wotcher!")));
    
    final PutDataPointRpc put = new PutDataPointRpc();
    final Channel chan = NettyMocks.fakeChannel();
    assertNull(put.execute(tsdb, chan, new String[] { "put", "sys.cpu.nice", 
        "1365465600", "42", "host=web01" }).joinUninterruptibly());

    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, hbase_errors.get());
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    verify(tsdb, times(1)).getStorageExceptionHandler();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void executeHBaseErrorNotWriteable() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), 
        (HashMap<String, String>)any()))
        .thenReturn(Deferred.fromError(new RuntimeException("Wotcher!")));
    
    final PutDataPointRpc put = new PutDataPointRpc();
    final Channel chan = NettyMocks.fakeChannel();
    when(chan.isWritable()).thenReturn(false);
    assertNull(put.execute(tsdb, chan, new String[] { "put", "sys.cpu.nice", 
        "1365465600", "42", "host=web01" }).joinUninterruptibly());

    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, hbase_errors.get());
    assertEquals(1, writes_blocked.get());
    verify(chan, never()).write(any());
    verify(chan, times(1)).isConnected();
    verify(tsdb, times(1)).getStorageExceptionHandler();
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void executeHBaseErrorHandler() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), 
        (HashMap<String, String>)any()))
        .thenReturn(Deferred.fromError(new RuntimeException("Wotcher!")));
    
    final PutDataPointRpc put = new PutDataPointRpc();
    final Channel chan = NettyMocks.fakeChannel();
    assertNull(put.execute(tsdb, chan, new String[] { "put", "sys.cpu.nice", 
        "1365465600", "42", "host=web01" }).joinUninterruptibly());

    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, hbase_errors.get());
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    verify(tsdb, times(1)).getStorageExceptionHandler();
    verify(handler, times(1)).handleError((IncomingDataPoint)any(), 
        (Exception)any());
  }

  @Test (expected = NullPointerException.class)
  public void executeNullTSDB() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc();
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(null, chan, new String[] { "put", "sys.cpu.nice", 
        "1365465600", "42", "host=web01" });
  }
  
  @Test
  public void executeNullChannelOK() throws Exception {
    // we can pass in a null channel but since we only write when an error occurs
    // then we won't fail.
    final PutDataPointRpc put = new PutDataPointRpc();
    assertNotNull(put.execute(tsdb, null, new String[] { "put", "sys.cpu.nice", 
        "1365465600", "42", "host=web01" }).joinUninterruptibly());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test (expected = NullPointerException.class)
  public void executeNullChannelError() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, null, new String[] { "put", "sys.cpu.nice", 
        "1365465600", "notanumber", "host=web01" });
  }
  
  @Test (expected = NullPointerException.class)
  public void executeNullArray() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc();
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, null);
  }
  
  @Test (expected = ArrayIndexOutOfBoundsException.class)
  public void executeEmptyArray() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc();
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, new String[0]);
  }
  
  @Test
  public void executeShortArray() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc();
    final Channel chan = NettyMocks.fakeChannel();
    assertNull(put.execute(tsdb, chan, new String[] { "put", "sys.cpu.nice", 
        "1365465600", "42" }).joinUninterruptibly());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, illegal_arguments.get());
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  // HTTP RPC Tests --------------------------------------
  
  @Test
  public void putSingle() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }

  @Test
  public void putDouble() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putSingleSummary() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?summary", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":1"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putSingleDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":1"));
    assertTrue(response.contains("\"errors\":[]"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putSingleSummaryAndDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?summary&details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":1"));
    assertTrue(response.contains("\"errors\":[]"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putDoubleSummary() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?summary", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":2"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putNegativeInt() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putFloat() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42.2,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putNegativeFloat() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-42.2,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putSEBig() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":4.22e3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putSECaseBig() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":4.22E3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putNegativeSEBig() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-4.22e3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putNegativeSECaseBig() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-4.22E3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putSETiny() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":4.2e-3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putSECaseTiny() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":4.2E-3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putNegativeSETiny() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-4.2e-3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void putNegativeSECaseTiny() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-4.2E-3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void badMethod() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/put");
    PutDataPointRpc put = new PutDataPointRpc();
    BadRequestException ex = null;
    try {
      put.execute(tsdb, query);
    } catch (BadRequestException e) {
      ex = e;
    }
    assertNotNull(ex);
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }

  @Test
  public void badJSON() throws Exception {
    // missing a quotation mark
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp:1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    BadRequestException ex = null;
    try {
      put.execute(tsdb, query);
    } catch (BadRequestException e) {
      ex = e;
    }
    assertNotNull(ex);
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void notJSON() throws Exception {
    // missing a quotation mark
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", "Hello World");
    PutDataPointRpc put = new PutDataPointRpc();
    BadRequestException ex = null;
    try {
      put.execute(tsdb, query);
    } catch (BadRequestException e) {
      ex = e;
    }
    assertNotNull(ex);
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void noContent() throws Exception {
    // missing a quotation mark
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", "");
    PutDataPointRpc put = new PutDataPointRpc();
    BadRequestException ex = null;
    try {
      put.execute(tsdb, query);
    } catch (BadRequestException e) {
      ex = e;
    }
    assertNotNull(ex);
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void hbaseError() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), 
        (HashMap<String, String>)any()))
        .thenReturn(Deferred.fromError(new RuntimeException("Wotcher!")));
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    BadRequestException ex = null;
    try {
      put.execute(tsdb, query);
    } catch (BadRequestException e) {
      ex = e;
    }
    assertNull(ex);
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, hbase_errors.get());
    verify(tsdb, times(1)).getStorageExceptionHandler();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void hbaseErrorHandler() throws Exception {
    final StorageExceptionHandler handler = mock(StorageExceptionHandler.class);
    when(tsdb.getStorageExceptionHandler()).thenReturn(handler);
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), 
        (HashMap<String, String>)any()))
        .thenReturn(Deferred.fromError(new RuntimeException("Wotcher!")));
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    BadRequestException ex = null;
    try {
      put.execute(tsdb, query);
    } catch (BadRequestException e) {
      ex = e;
    }
    assertNull(ex);
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, hbase_errors.get());
    verify(tsdb, times(1)).getStorageExceptionHandler();
    verify(handler, times(1)).handleError((IncomingDataPoint)any(), 
        (Exception)any());
  }
  
  @Test
  public void noSuchUniqueName() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"doesnotexist\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unknown metric\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    assertEquals(1, unknown_metrics.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }

  @Test
  public void missingMetric() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Metric name was empty\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
 
  @Test
  public void nullMetric() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":null,\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Metric name was empty\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }

  @Test
  public void missingTimestamp() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Invalid timestamp\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void nullTimestamp() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":null,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Invalid timestamp\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }

  @Test
  public void invalidTimestamp() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":-1,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Invalid timestamp\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }

  @Test
  public void missingValue() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"tags\":"
        + "{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Empty value\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(1, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }

  @Test
  public void nullValue() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":null,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Empty value\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(1, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void emptyValue() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":\"\",\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Empty value\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(1, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void badValue() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":\"notanumber\",\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unable to parse value to a number\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(1, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }

  @Test
  public void ValueNaN() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":NaN,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unable to parse value to a number\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(1, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void ValueNaNCase() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":Nan,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    BadRequestException ex = null;
    try {
      put.execute(tsdb, query);
    } catch (BadRequestException e) {
      ex = e;
    }
    assertNotNull(ex);
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void ValueINF() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":+INF,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unable to parse value to a number\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(1, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void ValueNINF() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-INF,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unable to parse value to a number\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(1, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void ValueINFUnsigned() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":INF,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    BadRequestException ex = null;
    try {
      put.execute(tsdb, query);
    } catch (BadRequestException e) {
      ex = e;
    }
    assertNotNull(ex);
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void ValueINFCase() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":+inf,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    BadRequestException ex = null;
    try {
      put.execute(tsdb, query);
    } catch (BadRequestException e) {
      ex = e;
    }
    assertNotNull(ex);
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void ValueInfiniy() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":+Infinity,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unable to parse value to a number\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(1, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void ValueNInfiniy() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-Infinity,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unable to parse value to a number\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(1, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void ValueInfinityUnsigned() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":Infinity,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unable to parse value to a number\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(1, invalid_values.get());
    assertEquals(0, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void missingTags() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\":42"
        + "}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Missing tags\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }

  @Test
  public void nullTags() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":null}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Missing tags\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }
  
  @Test
  public void emptyTags() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Missing tags\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    assertEquals(1, illegal_arguments.get());
    verify(tsdb, never()).getStorageExceptionHandler();
  }

  @Test
  public void syncOKNoDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync=true", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
    verify(tsdb, never()).getTimer();
  }
  
  @Test
  public void syncOKSummary() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync=true&summary", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":2"));
    assertFalse(response.contains("\"errors\":[]"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
    verify(tsdb, never()).getTimer();
  }
  
  @Test
  public void syncOKSummaryDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/put?sync=true&summary&details", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":2"));
    assertTrue(response.contains("\"errors\":[]"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
    verify(tsdb, never()).getTimer();
  }
  
  @Test
  public void syncOKDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync=true&details", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":2"));
    assertTrue(response.contains("\"errors\":[]"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
    verify(tsdb, never()).getTimer();
  }

  @Test
  public void syncOneFailed() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"code\":400"));
    assertTrue(response.contains("\"message\":"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, times(1)).getStorageExceptionHandler();
    verify(tsdb, never()).getTimer();
  }
  
  @Test
  public void syncOneFailedSummary() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync&summary", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":1"));
    assertFalse(response.contains("\"errors\":[]"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, times(1)).getStorageExceptionHandler();
    verify(tsdb, never()).getTimer();
  }
  
  @Test
  public void syncOneFailedDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync&details", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":1"));
    assertTrue(response.contains("\"errors\":[{"));
    assertTrue(response.contains("Wotcher!"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, times(1)).getStorageExceptionHandler();
    verify(tsdb, never()).getTimer();
  }
  
  @Test
  public void syncTwoFailedDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync&details", 
        "[{\"metric\":\"doesnotexist\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    System.out.println(response);
    assertTrue(response.contains("\"success\":0"));
    assertTrue(response.contains("\"failed\":2"));
    assertTrue(response.contains("\"errors\":[{"));
    assertTrue(response.contains("Wotcher!"));
    assertTrue(response.contains("Unknown metric"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, times(1)).getStorageExceptionHandler();
    verify(tsdb, never()).getTimer();
  }

  @Test
  public void syncOKTimeoutNoDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync&sync_timeout=30000", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
    verify(tsdb, times(1)).getTimer();
    verify(timer.timeout, times(1)).cancel();
  }
  
  @Test
  public void syncOKTimeoutSummary() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/put?sync&sync_timeout=30000&summary", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":2"));
    assertTrue(response.contains("\"timeouts\":0"));
    assertFalse(response.contains("\"errors\":[]"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, never()).getStorageExceptionHandler();
    verify(tsdb, times(1)).getTimer();
    verify(timer.timeout, times(1)).cancel();
  }

  @Test
  public void syncTimeoutOneFailed() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync&sync_timeout=30000", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"code\":400"));
    assertTrue(response.contains("\"message\":"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, times(1)).getStorageExceptionHandler();
    verify(tsdb, times(1)).getTimer();
    verify(timer.timeout, times(1)).cancel();
  }

  @Test
  public void syncTimeoutOneFailedSummary() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync&summary&sync_timeout=30000", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":1"));
    assertTrue(response.contains("\"timeouts\":0"));
    assertFalse(response.contains("\"errors\":[]"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, times(1)).getStorageExceptionHandler();
    verify(tsdb, times(1)).getTimer();
    verify(timer.timeout, times(1)).cancel();
  }

  @Test
  public void syncTimeoutTwoFailedDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/put?sync&details&sync_timeout=30000", 
        "[{\"metric\":\"doesnotexist\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    System.out.println(response);
    assertTrue(response.contains("\"success\":0"));
    assertTrue(response.contains("\"failed\":2"));
    assertTrue(response.contains("\"timeouts\":0"));
    assertTrue(response.contains("\"errors\":[{"));
    assertTrue(response.contains("Wotcher!"));
    assertTrue(response.contains("Unknown metric"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, times(1)).getStorageExceptionHandler();
    verify(tsdb, times(1)).getTimer();
    verify(timer.timeout, times(1)).cancel();
  }

  @Test
  public void syncTimeoutOneFailedTimedoutSummary() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/put?sync&summary&sync_timeout=30000", 
        "[{\"metric\":\"sys.cpu.system\",\"timestamp\":1365465600,\"value\""
        + ":2,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    verify(tsdb, times(1)).getTimer();
    verify(timer.timeout, never()).cancel();
    
    timer.continuePausedTask();
    
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertTrue(response.contains("\"timeouts\":1"));
    assertFalse(response.contains("\"errors\":[]"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, times(1)).getStorageExceptionHandler();
    verify(timer.timeout, never()).cancel();
  }
  
  @Test
  public void syncTimeoutOneFailedTimedoutDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/put?sync&details&sync_timeout=30000", 
        "[{\"metric\":\"sys.cpu.system\",\"timestamp\":1365465600,\"value\""
        + ":2,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    verify(tsdb, times(1)).getTimer();
    verify(timer.timeout, never()).cancel();
    
    timer.continuePausedTask();
    
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    System.out.println(response);
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    assertTrue(response.contains("\"timeouts\":1"));
    assertTrue(response.contains("\"errors\":[{"));
    assertTrue(response.contains("Write timedout"));
    assertTrue(response.contains("Wotcher!"));
    assertEquals(1, requests.get());
    assertEquals(0, invalid_values.get());
    verify(tsdb, times(1)).getStorageExceptionHandler();
    verify(timer.timeout, never()).cancel();
  }
}
