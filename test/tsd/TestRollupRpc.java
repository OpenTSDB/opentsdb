// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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

import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.UniqueId.UniqueIdType;

import org.hamcrest.CoreMatchers;
import org.hbase.async.HBaseException;
import org.hbase.async.PleaseThrottleException;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
           "ch.qos.*", "org.slf4j.*",
           "com.sum.*", "org.xml.*"})
public class TestRollupRpc extends BaseTestPutRpc {
  private final static byte[] FAMILY = "t".getBytes(MockBase.ASCII());
  
  private RollupConfig rollup_config;
  
  @Before
  public void beforeLocal() throws Exception {
    final List<byte[]> families = new ArrayList<byte[]>();
    families.add(FAMILY);
    
    final List<RollupInterval> rollups = new ArrayList<RollupInterval>();
    rollups.add(new RollupInterval(
        "tsdb", "tsdb-agg", "1m", "1h", true));
    rollups.add(new RollupInterval(
        "tsdb-rollup-1h", "tsdb-rollup-agg-1h", "1h", "1m"));
    
    rollup_config = new RollupConfig(rollups);
    Whitebox.setInternalState(tsdb, "rollup_config", rollup_config);
    Whitebox.setInternalState(tsdb, "default_interval", rollups.get(0));
    
    storage = new MockBase(tsdb, client, true, true, true, true);
    storage.addTable("tsdb-rollup-1h".getBytes(), families);
    storage.addTable("tsdb-rollup-agg-1h".getBytes(), families);
    
    mockUID(UniqueIdType.TAGK, "_aggregate", new byte[] { 0, 0, 42 });
    mockUID(UniqueIdType.TAGV, "SUM", new byte[] { 0, 0, 42 });
  }
  
  @Test
  public void constructor() {
    assertNotNull(new RollupDataPointRpc(tsdb.getConfig()));
  }

  // Socket RPC Tests ------------------------------------

  @Test
  public void execute() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    assertNotNull(rollup.execute(tsdb, chan, new String[] { "rollup", 
        "1h-sum", METRIC_STRING, "1365465600", "42", 
          TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly());
    validateCounters(1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    verify(chan, never()).write(any());
    verify(chan, never()).isConnected();
    validateSEH(false);
  }
  
  @Test
  public void executeRollupsDisabled() throws Exception {
    Whitebox.setInternalState(tsdb, "rollup_config", (RollupConfig) null);
    Whitebox.setInternalState(tsdb, "default_interval", (RollupInterval) null);
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup", "1h-sum", METRIC_STRING, 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
  @Test
  public void executeWithAgg() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup", "1h-sum:sum", 
        METRIC_STRING, "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    verify(chan, never()).write(any());
    verify(chan, never()).isConnected();
    validateSEH(false);
  }
  
  @Test
  public void executeBadValue() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup", "10-msum", 
        METRIC_STRING, "1365465600", "notanum", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }

  @Test
  public void executeBadValueNotWriteable() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    when(chan.isWritable()).thenReturn(false);
    rollup.execute(tsdb, chan, new String[] { "rollup", "1h-sum", 
        METRIC_STRING, "1365465600", "notanum", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0);
    verify(chan, never()).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
  @Test
  public void executeMissingMetric() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup", "1h-sum", "", 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
      .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
  @Test
  public void executeUnknownMetric() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup", "1h-sum", NSUN_METRIC, 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }

  @SuppressWarnings("unchecked")
  @Test (expected = RuntimeException.class)
  public void executeRuntimeException() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    PowerMockito.when(tsdb.addAggregatePoint(anyString(), anyLong(), anyLong(), 
        anyMap(), anyBoolean(), anyString(), anyString()))
        .thenThrow(new RuntimeException("Fail!"));
    rollup.execute(tsdb, chan, new String[] { "rollup", "rollup", "1h-sum", 
        METRIC_STRING, "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
  }
  
  @Test
  public void executeHBaseError() throws Exception {
    storage.throwException(MockBase.stringToBytes("0000015158CE00000001000001"), 
        mock(HBaseException.class));
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup", "1h-sum", 
        METRIC_STRING, "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(true);
  }
  
  @Test
  public void executeHBaseErrorNotWriteable() throws Exception {
    storage.throwException(MockBase.stringToBytes("0000015158CE00000001000001"), 
        mock(HBaseException.class));
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    when(chan.isWritable()).thenReturn(false);
    rollup.execute(tsdb, chan, new String[] { "rollup", "1h-sum", 
        METRIC_STRING, "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0);
    verify(chan, never()).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(true);
  }

  @Test
  public void executeHBaseErrorHandler() throws Exception {
    setStorageExceptionHandler();
    storage.throwException(MockBase.stringToBytes("0000015158CE00000001000001"), 
        mock(HBaseException.class));
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup", "1h-sum", 
        METRIC_STRING, "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(true);
  }

  @Test
  public void executePleaseThrottle() throws Exception {
    storage.throwException(MockBase.stringToBytes("0000015158CE00000001000001"), 
        mock(PleaseThrottleException.class));
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup", "1h-sum", 
        METRIC_STRING, "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(true);
  }

  @Test
  public void executePleaseThrottleNotWriteable() throws Exception {
    storage.throwException(MockBase.stringToBytes("0000015158CE00000001000001"), 
        mock(PleaseThrottleException.class));
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    when(chan.isWritable()).thenReturn(false);
    rollup.execute(tsdb, chan, new String[] { "rollup", "1h-sum", 
        METRIC_STRING, "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0);
    verify(chan, never()).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(true);
  }

  @Test
  public void executePleaseThrottleHandler() throws Exception {
    setStorageExceptionHandler();
    storage.throwException(MockBase.stringToBytes("0000015158CE00000001000001"), 
        mock(PleaseThrottleException.class));
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup", "1h-sum", 
        METRIC_STRING, "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(true);
  }
  
  @Test (expected = NullPointerException.class)
  public void executeNullTSDB() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(null, chan, new String[] { "rollup", "1h-sum", 
        METRIC_STRING, "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
  }
  
  @Test
  public void executeNullChannelOK() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    // we can pass in a null channel but since we only write when an error occurs
    // then we won't fail.
    rollup.execute(tsdb, null, new String[] { "rollup", "1h-sum", 
        METRIC_STRING, "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test (expected = NullPointerException.class)
  public void executeNullChannelError() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, null, new String[] { "rollup", "1h-sum", 
        METRIC_STRING, "1365465600", "notanumber", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
  }
  
  @Test (expected = NullPointerException.class)
  public void executeNullArray() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, null).joinUninterruptibly();
  }
  
  @Test (expected = ArrayIndexOutOfBoundsException.class)
  public void executeEmptyArray() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[0]).joinUninterruptibly();
  }
  
  @Test
  public void executeShortArray() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup", "1h-sum", 
        METRIC_STRING, "1365465600", "42" }).joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
  @Test
  public void executeMissingInterval() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup","", METRIC_STRING,
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
  @Test
  public void executeUnknownInterval() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup", "13m-sum", 
        METRIC_STRING, "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }

  // TODO - revisit
//  @Test
//  public void executePreAggOnly() throws Exception {
//    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
//    final Channel chan = NettyMocks.fakeChannel();
//    rollup.execute(tsdb, chan, new String[] { "rollup", "sum", METRIC_STRING, 
//        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
//        .joinUninterruptibly();
//    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
//    verify(chan, never()).write(any());
//    verify(chan, never()).isConnected();
//    validateSEH(false);
//  }
  
  @Test
  public void executeMissingAggregator() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup", METRIC_STRING, 
        "1365465600", "42", TAGK_STRING + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }

  // TODO - test unknown aggs if we decide to implement that check

  @Test
  public void executeMissingTags() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup","1h-sum", METRIC_STRING, 
        "1365465600", "42", "" })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }

  @Test
  public void executeNSUNTagk() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup","1h-sum", METRIC_STRING, 
        "1365465600", "42", NSUN_TAGK + "=" + TAGV_STRING })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
  @Test
  public void executeNSUNTagV() throws Exception {
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rollup.execute(tsdb, chan, new String[] { "rollup", "1h-sum", METRIC_STRING, 
        "1365465600", "42", TAGK_STRING + "=" + NSUN_TAGV })
        .joinUninterruptibly();
    validateCounters(1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
// HTTP RPC Tests --------------------------------------  
  
  @Test
  public void httpAddSingleRollupPoint() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/rollup",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\":42, "
            + "\"interval\":\"1h\", \"aggregator\":\"sum\","
            + "\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void httpAddTwoRollupPoints() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/rollup",
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\":42, "
            + "\"interval\":\"1h\", \"aggregator\":\"sum\","
            + "\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}, "
            + "{\"metric\":\"" + METRIC_B_STRING + "\",\"timestamp\":1365465600,\"value\":24, "
            + "\"interval\":\"1h\", \"aggregator\":\"sum\","
            + "\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 0, 2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void httpAddTwoRollupPointsOneGoodOneBad() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/rollup",
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\":42, "
            + "\"interval\":\"1h\", \"aggregator\":\"sum\","
            + "\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}, "
            + "{\"metric\":\"" + NSUN_METRIC + "\",\"timestamp\":1365465600,\"value\":24, "
            + "\"interval\":\"1h\", \"aggregator\":\"sum\","
            + "\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    validateCounters(0, 1, 0, 2, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void httpMissingInterval() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/rollup?details",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\":42, " + 
        "\"aggregator\":\"sum\",\"tags\":{\"" + TAGK_STRING + "\":\"" + 
            TAGV_STRING + "\"}}");
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response =
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertThat(response, CoreMatchers.containsString("\"error\":\"Missing interval\""));
    assertThat(response, CoreMatchers.containsString("\"failed\":1"));
    assertThat(response, CoreMatchers.containsString("\"success\":0"));
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    validateCounters(0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void httpEmptyInterval() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/rollup?details",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\":42, "
            + "\"interval\":\"\", \"aggregator\":\"sum\","
            + "\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response =
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertThat(response, CoreMatchers.containsString("\"error\":\"Missing interval\""));
    assertThat(response, CoreMatchers.containsString("\"failed\":1"));
    assertThat(response, CoreMatchers.containsString("\"success\":0"));
    validateCounters(0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void httpMissingAggregator() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/rollup?details",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\":42, " +
        "\"interval\":\"1h\",\"tags\":{\"" + TAGK_STRING + "\":\"" + 
            TAGV_STRING + "\"}}");
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response =
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertThat(response, CoreMatchers.containsString("\"error\":\"Missing aggregator\""));
    assertThat(response, CoreMatchers.containsString("\"failed\":1"));
    assertThat(response, CoreMatchers.containsString("\"success\":0"));
    validateCounters(0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void httpInvalidAggregator() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/rollup?details",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\":42, "
            + "\"interval\":\"1h\", \"aggregator\":\"what?\","
            + "\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response =
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertThat(response, CoreMatchers.containsString("\"error\":\"Invalid aggregator\""));
    assertThat(response, CoreMatchers.containsString("\"failed\":1"));
    assertThat(response, CoreMatchers.containsString("\"success\":0"));
    validateCounters(0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void httpEmptyAggregator() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/rollup?details",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\":42, "
            + "\"interval\":\"1h\", \"aggregator\":\"\","
            + "\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    final String response =
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertThat(response, CoreMatchers.containsString("\"error\":\"Missing aggregator\""));
    assertThat(response, CoreMatchers.containsString("\"failed\":1"));
    assertThat(response, CoreMatchers.containsString("\"success\":0"));
    validateCounters(0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void httpNSUNMetric() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/rollup",
        "{\"metric\":\"" + NSUN_METRIC + "\",\"timestamp\":1365465600,\"value\":42, "
            + "\"interval\":\"1h\", \"aggregator\":\"sum\","
            + "\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    validateCounters(0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void httpNSUNTagk() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/rollup",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\":42, "
            + "\"interval\":\"1h\", \"aggregator\":\"sum\","
            + "\"tags\":{\"" + NSUN_TAGK + "\":\"" + TAGV_STRING + "\"}}");
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    validateCounters(0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0);
    validateSEH(false);
  }
  
  @Test
  public void httpNSUNTagv() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/rollup",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\":42, "
            + "\"interval\":\"1h\", \"aggregator\":\"sum\","
            + "\"tags\":{\"" + TAGK_STRING + "\":\"" + NSUN_TAGV + "\"}}");
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    validateCounters(0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0);
    validateSEH(false);
  }

  @Test
  public void httpHBaseError() throws Exception {
    storage.throwException(MockBase.stringToBytes("0000015158CE00000001000001"), 
        mock(HBaseException.class));
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/rollup",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\":42, "
            + "\"interval\":\"1h\", \"aggregator\":\"sum\","
            + "\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(true);
  }

  @Test
  public void httpPleaseThrottleError() throws Exception {
    storage.throwException(MockBase.stringToBytes("0000015158CE00000001000001"), 
        mock(PleaseThrottleException.class));
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/rollup",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\":42, "
            + "\"interval\":\"1h\", \"aggregator\":\"sum\","
            + "\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0);
    validateSEH(true);
  }

  @Test
  public void httpUnknownInterval() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/rollup",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1365465600,\"value\":42, "
            + "\"interval\":\"13m\", \"aggregator\":\"sum\","
            + "\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    final RollupDataPointRpc rollup = new RollupDataPointRpc(tsdb.getConfig());
    rollup.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    validateCounters(0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0);
    validateSEH(false);
  }

}
