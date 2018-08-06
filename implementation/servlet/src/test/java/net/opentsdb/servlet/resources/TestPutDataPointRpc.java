// This file is part of OpenTSDB.
// Copyright (C) 2013-2016  The OpenTSDB Authors.
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.Charset;
import java.util.HashMap;

import org.hbase.async.HBaseException;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.PutRequest;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
             "ch.qos.*", "org.slf4j.*",
             "com.sum.*", "org.xml.*"})
public final class TestPutRpc extends BaseTestPutRpc {
  
  @Test
  public void constructor() {
    assertNotNull(new PutDataPointRpc(tsdb.getConfig()));
  }
  
  // Socket RPC Tests ------------------------------------

  @Test
  public void execute() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, new String[] { "put", METRIC_STRING, 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
      .joinUninterruptibly();
    validateCounters(1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    verify(chan, never()).write(any());
    verify(chan, never()).isConnected();
    validateSEH(false);
  }

  @Test
  public void executeBadValue() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, new String[] { "put", METRIC_STRING, 
        "1365465600", "notanum", TAGK_STRING + "=" + TAGV_STRING })
      .joinUninterruptibly();
    validateCounters(1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
  @Test
  public void executeMissingMetric() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, new String[] { "put", "", 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
      .joinUninterruptibly();
    validateCounters(1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
  @Test
  public void executeMissingMetricNotWriteable() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    when(chan.isWritable()).thenReturn(false);
    put.execute(tsdb, chan, new String[] { "put", "", 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
      .joinUninterruptibly();
    validateCounters(1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0);
    verify(chan, never()).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
  @Test
  public void executeUnknownMetric() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, new String[] { "put", NSUN_METRIC, 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
      .joinUninterruptibly();
    validateCounters(1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
  @SuppressWarnings("unchecked")
  @Test (expected = RuntimeException.class)
  public void executeRuntimeException() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), 
        (HashMap<String, String>)any()))
        .thenThrow(new RuntimeException("Fail!"));
    
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, new String[] { "put", METRIC_STRING, 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING });
    validateCounters(1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0);
    validateSEH(true);
  }
  
  @Test
  public void executeHBaseError() throws Exception {
    when(client.put(any(PutRequest.class)))
        .thenReturn(Deferred.fromError(mock(HBaseException.class)));
    
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, new String[] { "put", METRIC_STRING, 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
      .joinUninterruptibly();
    validateCounters(1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(true);
  }
  
  @Test
  public void executeHBaseErrorNotWriteable() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromError(mock(HBaseException.class)));
    
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    when(chan.isWritable()).thenReturn(false);
    put.execute(tsdb, chan, new String[] { "put", METRIC_STRING, 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
      .joinUninterruptibly();
    validateCounters(1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0);
    verify(chan, never()).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(true);
  }
  
  @Test
  public void executeHBaseErrorHandler() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromError(mock(HBaseException.class)));
    setStorageExceptionHandler();
    
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, new String[] { "put", METRIC_STRING, 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
      .joinUninterruptibly();
    validateCounters(1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(true);
  }

  @Test
  public void executePleaseThrottle() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromError(mock(PleaseThrottleException.class)));
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, new String[] { "put", METRIC_STRING, 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
      .joinUninterruptibly();
    validateCounters(1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(true);
  }
  
  @Test
  public void executePleaseThrottleNotWriteable() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromError(mock(PleaseThrottleException.class)));
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    when(chan.isWritable()).thenReturn(false);
    put.execute(tsdb, chan, new String[] { "put", METRIC_STRING, 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
      .joinUninterruptibly();
    validateCounters(1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0);
    verify(chan, never()).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(true);
  }
  
  @Test
  public void executePleaseThrottleHandler() throws Exception {
    setStorageExceptionHandler();
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromError(mock(PleaseThrottleException.class)));
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, new String[] { "put", METRIC_STRING, 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
      .joinUninterruptibly();
    validateCounters(1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(true);
  }
  
  @Test (expected = NullPointerException.class)
  public void executeNullTSDB() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(null, chan, new String[] { "put", METRIC_STRING, 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING });
  }
  
  @Test
  public void executeNullChannelOK() throws Exception {
    // we can pass in a null channel but since we only write when an error occurs
    // then we won't fail.
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, null, new String[] { "put", METRIC_STRING, 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
      .joinUninterruptibly();
    assertEquals(1, telnet_requests.get());
    assertEquals(0, invalid_values.get());
    validateCounters(1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test (expected = NullPointerException.class)
  public void executeNullChannelError() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, null, new String[] { "put", METRIC_STRING, 
        "1365465600", "notanumber", TAGK_STRING + "=" + TAGV_STRING });
  }
  
  @Test (expected = NullPointerException.class)
  public void executeNullArray() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, null);
  }
  
  @Test (expected = ArrayIndexOutOfBoundsException.class)
  public void executeEmptyArray() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, new String[0]);
  }
  
  @Test
  public void executeShortArray() throws Exception {
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    put.execute(tsdb, chan, new String[] { "put", METRIC_STRING, 
        "1365465600", "42" }).joinUninterruptibly();
    validateCounters(1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
  // HTTP RPC Tests --------------------------------------
  
  @Test
  public void putSingle() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void putTwo() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING 
        + "\"}},{\"metric\":\"" + METRIC_B_STRING + "\","
        + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
        + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putSingleSummary() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?summary", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":1"));
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putSingleDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":1"));
    assertTrue(response.contains("\"errors\":[]"));
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putSingleSummaryAndDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?summary&details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":1"));
    assertTrue(response.contains("\"errors\":[]"));
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putTwoSummary() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?summary", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING 
            + "\"}},{\"metric\":\"" + METRIC_B_STRING + "\","
            + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
            + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":2"));
    validateCounters(0, 1, 2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putNegativeInt() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":-42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putFloat() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":42.2,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putDoublePrecisionFloatingPoint() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        + ":123456.123456789,\"tags\":{\"" + TAGK_STRING + "\":\"" 
        + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void putNegativeDoublePrecisionFloatingPoint() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        + ":-123456.123456789,\"tags\":{\"" + TAGK_STRING + "\":\"" 
        + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putNegativeFloat() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":-42.2,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putSEBig() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":42.22e3,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putSECaseBig() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":42.22E3,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putNegativeSEBig() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":-42.22e3,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putNegativeSECaseBig() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":-42.22E3,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putSETiny() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":42.22e-3,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putSECaseTiny() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":42.22E-3,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putNegativeSETiny() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":-4.2e-3,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void putNegativeSECaseTiny() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":-4.2E-3,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void badMethod() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/put");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    try {
      put.execute(tsdb, query);
      fail("Expected BadRequestException");
    } catch (BadRequestException e) { }
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void badJSON() throws Exception {
    // missing a quotation mark
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp:1365465600,\"value\""
        +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    try {
      put.execute(tsdb, query);
      fail("Expected BadRequestException");
    } catch (BadRequestException e) { }
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void notJSON() throws Exception {
    // missing a quotation mark
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", "Hello World");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    try {
      put.execute(tsdb, query);
      fail("Expected BadRequestException");
    } catch (BadRequestException e) { }
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void noContent() throws Exception {
    // missing a quotation mark
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", "");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    try {
      put.execute(tsdb, query);
      fail("Expected BadRequestException");
    } catch (BadRequestException e) { }
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void inFlightExceeded() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromError(mock(PleaseThrottleException.class)));
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0);
    validateSEH(true);
  }

  @Test
  public void inFlightExceededHandler() throws Exception {
    setStorageExceptionHandler();
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromError(mock(PleaseThrottleException.class)));
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0);
    validateSEH(true);
  }
  
  @Test
  public void hbaseError() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromError(mock(HBaseException.class)));
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(true);
  }

  @Test
  public void hbaseErrorHandler() throws Exception {
    setStorageExceptionHandler();
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromError(mock(HBaseException.class)));
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    validateCounters(0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(true);
  }
  
  @Test
  public void noSuchUniqueName() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + NSUN_METRIC + "\",\"timestamp\":1365465600,\"value\""
            +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unknown metric\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void missingMetric() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"timestamp\":1365465600,\"value\""
            +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Metric name was empty\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
 
  @Test
  public void nullMetric() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":null,\"timestamp\":1365465600,\"value\""
            +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Metric name was empty\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void missingTimestamp() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"value\""
            +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Invalid timestamp\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void nullTimestamp() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":null,\"value\""
            +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Invalid timestamp\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void invalidTimestamp() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":-1,\"value\""
            +":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Invalid timestamp\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void missingValue() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"tags\":"
        + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Empty value\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void nullValue() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":null,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Empty value\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void emptyValue() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":\"\",\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Empty value\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void badValue() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":\"notanumber\",\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unable to parse value to a number\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void ValueNaN() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":NaN,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unable to parse value to a number\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void ValueNaNCase() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":Nan,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    try {
      put.execute(tsdb, query);
      fail("Expected BadRequestException");
    } catch (BadRequestException e) { }
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void ValueINF() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":+INF,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unable to parse value to a number\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void ValueNINF() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":-INF,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unable to parse value to a number\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void ValueINFUnsigned() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":INF,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    try {
      put.execute(tsdb, query);
      fail("Expected BadRequestException");
    } catch (BadRequestException e) { }
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void ValueINFCase() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":+inf,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    try {
      put.execute(tsdb, query);
      fail("Expected BadRequestException");
    } catch (BadRequestException e) { }
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void ValueInfinity() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":+Infinity,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unable to parse value to a number\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void ValueNInfinity() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":-Infinity,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unable to parse value to a number\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void ValueInfinityUnsigned() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":Infinity,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Unable to parse value to a number\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void missingTags() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        + ":42}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Missing tags\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void nullTags() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":null}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Missing tags\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void emptyTags() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{}}");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"error\":\"Missing tags\""));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":0"));
    validateCounters(0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void syncOKNoDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync=true", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}},"
        + "{\"metric\":\"" + METRIC_B_STRING + "\","
        + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
        + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
    verify(tsdb, never()).getTimer();
  }
  
  @Test
  public void syncOKSummary() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync=true&summary", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}},"
            + "{\"metric\":\"" + METRIC_B_STRING + "\","
            + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
            + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":2"));
    assertFalse(response.contains("\"errors\":[]"));
    validateCounters(0, 1, 2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
    verify(tsdb, never()).getTimer();
  }
  
  @Test
  public void syncOKSummaryDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/put?sync=true&summary&details", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}},"
            + "{\"metric\":\"" + METRIC_B_STRING + "\","
            + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
            + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":2"));
    assertTrue(response.contains("\"errors\":[]"));
    validateCounters(0, 1, 2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
    verify(tsdb, never()).getTimer();
  }
  
  @Test
  public void syncOKDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync=true&details", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}},"
            + "{\"metric\":\"" + METRIC_B_STRING + "\","
            + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
            + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":2"));
    assertTrue(response.contains("\"errors\":[]"));
    validateCounters(0, 1, 2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
    verify(tsdb, never()).getTimer();
  }

  @Test
  public void syncOneFailed() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromResult(null))
      .thenReturn(Deferred.fromError(mock(HBaseException.class)));
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING 
        + "\"}},{\"metric\":\"" + METRIC_B_STRING + "\","
        + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
        + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"code\":400"));
    assertTrue(response.contains("\"message\":"));
    validateCounters(0, 1, 2, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(true);
    verify(tsdb, never()).getTimer();
  }
  
  @Test
  public void syncOneFailedSummary() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromResult(null))
      .thenReturn(Deferred.fromError(mock(HBaseException.class)));
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync&summary", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING 
        + "\"}},{\"metric\":\"" + METRIC_B_STRING + "\","
        + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
        + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":1"));
    assertFalse(response.contains("\"errors\":[]"));
    validateCounters(0, 1, 2, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(true);
    verify(tsdb, never()).getTimer();
  }
  
  @Test
  public void syncOneFailedDetails() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromResult(null))
      .thenReturn(Deferred.fromError(mock(HBaseException.class)));
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync&details", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING 
        + "\"}},{\"metric\":\"" + METRIC_B_STRING + "\","
        + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
        + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":1"));
    assertTrue(response.contains("\"errors\":[{"));
    validateCounters(0, 1, 2, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(true);
    verify(tsdb, never()).getTimer();
  }
  
  @Test
  public void syncTwoFailedDetails() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromError(mock(HBaseException.class)));
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync&details", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING 
            + "\"}},{\"metric\":\"" + NSUN_METRIC + "\","
            + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
            + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    System.out.println(response);
    assertTrue(response.contains("\"success\":0"));
    assertTrue(response.contains("\"failed\":2"));
    assertTrue(response.contains("\"errors\":[{"));
    validateCounters(0, 1, 2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0);
    validateSEH(true);
    verify(tsdb, never()).getTimer();
  }
  
  @Test
  public void syncOKTimeoutNoDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync&sync_timeout=30000", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING 
            + "\"}},{\"metric\":\"" + METRIC_B_STRING + "\","
            + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
            + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, http_requests.get());
    assertEquals(0, invalid_values.get());
    validateCounters(0, 1, 2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
    verify(tsdb, times(1)).getTimer();
    verify(timer.timeout, times(1)).cancel();
  }
  
  @Test
  public void syncOKTimeoutSummary() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/put?sync&sync_timeout=30000&summary", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING 
            + "\"}},{\"metric\":\"" + METRIC_B_STRING + "\","
            + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
            + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":0"));
    assertTrue(response.contains("\"success\":2"));
    assertTrue(response.contains("\"timeouts\":0"));
    assertFalse(response.contains("\"errors\":[]"));
    validateCounters(0, 1, 2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
    verify(tsdb, times(1)).getTimer();
    verify(timer.timeout, times(1)).cancel();
  }

  @Test
  public void syncTimeoutOneFailed() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromResult(null))
      .thenReturn(Deferred.fromError(mock(HBaseException.class)));
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?sync&sync_timeout=30000", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING 
            + "\"}},{\"metric\":\"" + METRIC_B_STRING + "\","
            + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
            + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"code\":400"));
    assertTrue(response.contains("\"message\":"));
    validateCounters(0, 1, 2, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(true);
    verify(tsdb, times(1)).getTimer();
    verify(timer.timeout, times(1)).cancel();
  }

  @Test
  public void syncTimeoutOneFailedSummary() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromResult(null))
      .thenReturn(Deferred.fromError(mock(HBaseException.class)));
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/put?sync&summary&sync_timeout=30000", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING 
            + "\"}},{\"metric\":\"" + METRIC_B_STRING + "\","
            + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
            + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(response.contains("\"failed\":1"));
    assertTrue(response.contains("\"success\":1"));
    assertTrue(response.contains("\"timeouts\":0"));
    assertFalse(response.contains("\"errors\":[]"));
    validateCounters(0, 1, 2, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(true);
    verify(tsdb, times(1)).getTimer();
    verify(timer.timeout, times(1)).cancel();
  }

  @Test
  public void syncTimeoutTwoFailedDetails() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(Deferred.fromError(mock(HBaseException.class)));
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/put?sync&details&sync_timeout=30000", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING 
            + "\"}},{\"metric\":\"" + NSUN_METRIC + "\","
            + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
            + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    System.out.println(response);
    assertTrue(response.contains("\"success\":0"));
    assertTrue(response.contains("\"failed\":2"));
    assertTrue(response.contains("\"timeouts\":0"));
    assertTrue(response.contains("\"errors\":[{"));
    validateCounters(0, 1, 2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0);
    validateSEH(true);
    verify(tsdb, times(1)).getTimer();
    verify(timer.timeout, times(1)).cancel();
  }

  @Test
  public void syncTimeoutOneFailedTimedoutSummary() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(new Deferred<Object>())
      .thenReturn(Deferred.fromError(mock(HBaseException.class)));
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/put?sync&summary&sync_timeout=30000", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING 
            + "\"}},{\"metric\":\"" + METRIC_B_STRING + "\","
            + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
            + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
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
    validateCounters(0, 1, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0);
    validateSEH(true);
    verify(tsdb, times(1)).getTimer();
    verify(timer.timeout, never()).cancel();
  }
  
  @Test
  public void syncTimeoutOneFailedTimedoutDetails() throws Exception {
    when(client.put(any(PutRequest.class)))
      .thenReturn(new Deferred<Object>())
      .thenReturn(Deferred.fromError(mock(HBaseException.class)));
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/put?sync&details&sync_timeout=30000", 
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\""
            + ":42,\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING 
            + "\"}},{\"metric\":\"" + METRIC_B_STRING + "\","
            + "\"timestamp\":1365465600,\"value\":1,\"tags\":"
            + "{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
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
    assertTrue(response.contains("\"errors\":[{"));
    assertTrue(response.contains("Write timedout"));
    validateCounters(0, 1, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0);
    validateSEH(true);
    verify(tsdb, times(1)).getStorageExceptionHandler();
    verify(timer.timeout, never()).cancel();
  }

}
