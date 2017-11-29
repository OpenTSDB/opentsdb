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

package net.opentsdb.tsd;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.PleaseThrottleException;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.HashedWheelTimer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Maps;

import net.opentsdb.core.HistogramCodecManager;
import net.opentsdb.core.HistogramPojo;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.SimpleHistogram;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.Threads;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
         "ch.qos.*", "org.slf4j.*",
         "com.sum.*", "org.xml.*"})
public class TestHistogramDataPointRpc extends BaseTestPutRpc {
  
  protected AtomicLong raw_histograms = new AtomicLong();
  protected AtomicLong raw_histograms_stored = new AtomicLong();
  
  private HistogramCodecManager manager;
  private SimpleHistogram test_histo;
  
  @Before
  public void before() throws Exception {
    uid_map = Maps.newHashMap();
    PowerMockito.mockStatic(Threads.class);
    timer = new FakeTaskTimer();
    PowerMockito.when(Threads.newTimer(anyString())).thenReturn(timer);
    PowerMockito.when(Threads.newTimer(anyInt(), anyString())).thenReturn(timer);
    
    PowerMockito.whenNew(HashedWheelTimer.class).withNoArguments()
      .thenReturn(timer);
    PowerMockito.whenNew(HBaseClient.class).withAnyArguments()
      .thenReturn(client);
    
    config = new Config(false);
    config.overrideConfig("tsd.storage.enable_compaction", "false");
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.SimpleHistogramDecoder\": 42}");
    tsdb = new TSDB(config);

    config.setAutoMetric(true);
    
    Whitebox.setInternalState(tsdb, "metrics", metrics);
    Whitebox.setInternalState(tsdb, "tag_names", tag_names);
    Whitebox.setInternalState(tsdb, "tag_values", tag_values);

    setupMetricMaps();
    setupTagkMaps();
    setupTagvMaps();
    
    mockUID(UniqueIdType.METRIC, HISTOGRAM_METRIC_STRING, HISTOGRAM_METRIC_BYTES);
    
    // add metrics and tags to the UIDs list for other functions to share
    uid_map.put(METRIC_STRING, METRIC_BYTES);
    uid_map.put(METRIC_B_STRING, METRIC_B_BYTES);
    uid_map.put(NSUN_METRIC, NSUI_METRIC);
    uid_map.put(HISTOGRAM_METRIC_STRING, HISTOGRAM_METRIC_BYTES);
    
    uid_map.put(TAGK_STRING, TAGK_BYTES);
    uid_map.put(TAGK_B_STRING, TAGK_B_BYTES);
    uid_map.put(NSUN_TAGK, NSUI_TAGK);
    
    uid_map.put(TAGV_STRING, TAGV_BYTES);
    uid_map.put(TAGV_B_STRING, TAGV_B_BYTES);
    uid_map.put(NSUN_TAGV, NSUI_TAGV);
    
    uid_map.putAll(UIDS);
    
    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
    
    tags = new HashMap<String, String>(1);
    tags.put(TAGK_STRING, TAGV_STRING);
    
    
    manager = new HistogramCodecManager(tsdb);
    Whitebox.setInternalState(tsdb, "histogram_manager", manager);
    
    storage = new MockBase(tsdb, client, true, true, true, true);
    
    test_histo = new SimpleHistogram(42);
    test_histo.addBucket(0F, 1F, 42L);
    test_histo.addBucket(1F, 5F, 24L);
    test_histo.setOverflow(1L);
    
    // counters
    raw_histograms = Whitebox.getInternalState(PutDataPointRpc.class, "raw_histograms");
    raw_histograms.set(0);
    raw_histograms_stored = Whitebox.getInternalState(PutDataPointRpc.class, "raw_histograms_stored");
    raw_histograms_stored.set(0);
  }
  
  @Test
  public void constructor() {
    HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    assertTrue(rpc.enabled());
    
    config.overrideConfig("tsd.core.histograms.config", null);
    rpc = new HistogramDataPointRpc(tsdb.getConfig());
    assertFalse(rpc.enabled());
  }
  
  @Test
  public void executeTelnet() throws Exception {
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    
    rpc.execute(tsdb, chan, new String[] { "histogram", METRIC_STRING, 
        "1356998400", "u=0:o=1:0,1=42:1,5=24", 
        TAGK_STRING + "=" + TAGV_STRING })
        .join();
    validateCounters(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1);
    verify(chan, never()).write(any());
    verify(chan, never()).isConnected();
    validateSEH(false);
    
    byte[] qualifier = new byte[] {0x06, 0, 0};
    byte[] value = storage.getColumn(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), qualifier);
    assertArrayEquals(test_histo.histogram(true), value);
  }
  
  @Test
  public void executeTelnetBinary() throws Exception {
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    
    rpc.execute(tsdb, chan, new String[] { "histogram", METRIC_STRING, 
        "1356998400", "42", 
        HistogramPojo.bytesToBase64String(test_histo.histogram(false)), 
        TAGK_STRING + "=" + TAGV_STRING })
        .join();
    validateCounters(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1);
    verify(chan, never()).write(any());
    verify(chan, never()).isConnected();
    validateSEH(false);
    
    byte[] qualifier = new byte[] {0x06, 0, 0};
    byte[] value = storage.getColumn(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), qualifier);
    assertArrayEquals(test_histo.histogram(true), value);
  }
  
  @Test
  public void executeTelnetHistosDisabled() throws Exception {
    config.overrideConfig("tsd.core.histograms.config", null);
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    
    rpc.execute(tsdb, chan, new String[] { "histogram", METRIC_STRING, 
        "1356998400", "42", 
        HistogramPojo.bytesToBase64String(test_histo.histogram(false)), 
        TAGK_STRING + "=" + TAGV_STRING })
        .join();
    validateCounters(1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
    
    byte[] qualifier = new byte[] {0x06, 0, 0};
    assertNull(storage.getColumn(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), qualifier));
  }
  
  @Test
  public void executeTelnetBinaryValueTooShort() throws Exception {
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    
    rpc.execute(tsdb, chan, new String[] { "histogram", METRIC_STRING, 
        "1356998400", "42", 
        HistogramPojo.bytesToBase64String(test_histo.histogram(false))
          .substring(0, 4), 
        TAGK_STRING + "=" + TAGV_STRING })
        .join();
    validateCounters(1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
    
    byte[] qualifier = new byte[] {0x06, 0, 0};
    assertNull(storage.getColumn(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), qualifier));
  }
  
  @Test
  public void executeTelnetBinaryCorruptValue() throws Exception {
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    
    rpc.execute(tsdb, chan, new String[] { "histogram", METRIC_STRING, 
        "1356998400", "42", 
        HistogramPojo.bytesToBase64String(test_histo.histogram(false))
          .substring(0, 8), 
        TAGK_STRING + "=" + TAGV_STRING })
        .join();
    validateCounters(1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
    
    byte[] qualifier = new byte[] {0x06, 0, 0};
    assertNull(storage.getColumn(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), qualifier));
  }
  
  @Test
  public void executeTelnetHBaseError() throws Exception {
    storage.throwException(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), mock(HBaseException.class));
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    
    rpc.execute(tsdb, chan, new String[] { "histogram", METRIC_STRING, 
        "1356998400", "42", 
        HistogramPojo.bytesToBase64String(test_histo.histogram(false)), 
        TAGK_STRING + "=" + TAGV_STRING })
        .join();
    validateCounters(1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(true);
    
    byte[] qualifier = new byte[] {0x06, 0, 0};
    assertNull(storage.getColumn(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), qualifier));
  }
  
  @Test
  public void executeTelnetePleaseThrottle() throws Exception {
    storage.throwException(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), mock(PleaseThrottleException.class));
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    
    rpc.execute(tsdb, chan, new String[] { "histogram", METRIC_STRING, 
        "1356998400", "42", 
        HistogramPojo.bytesToBase64String(test_histo.histogram(false)), 
        TAGK_STRING + "=" + TAGV_STRING })
        .join();
    validateCounters(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(true);
    
    byte[] qualifier = new byte[] {0x06, 0, 0};
    assertNull(storage.getColumn(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), qualifier));
  }
  
  @Test (expected = NullPointerException.class)
  public void executeTelnetNullTSDB() throws Exception {
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    
    rpc.execute(null, chan, new String[] { "histogram", METRIC_STRING, 
        "1356998400", "42", 
        HistogramPojo.bytesToBase64String(test_histo.histogram(false)), 
        TAGK_STRING + "=" + TAGV_STRING })
        .join();
  }
  
  @Test
  public void executeTelnetNullChannelOK() throws Exception {
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    rpc.execute(tsdb, null, new String[] { "histogram", METRIC_STRING, 
        "1356998400", "42", 
        HistogramPojo.bytesToBase64String(test_histo.histogram(false)), 
        TAGK_STRING + "=" + TAGV_STRING })
        .join();
    validateCounters(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1);
    validateSEH(false);
  }
  
  @Test (expected = NullPointerException.class)
  public void executeNullChannelError() throws Exception {
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    rpc.execute(tsdb, null, new String[] { "histogram" })
        .joinUninterruptibly();
  }
  
  @Test (expected = NullPointerException.class)
  public void executeNullArray() throws Exception {
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rpc.execute(tsdb, chan, null).joinUninterruptibly();
  }
  
  @Test (expected = ArrayIndexOutOfBoundsException.class)
  public void executeEmptyArray() throws Exception {
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rpc.execute(tsdb, chan, new String[0]).joinUninterruptibly();
  }
  
  @Test
  public void executeShortArray() throws Exception {
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rpc.execute(tsdb, chan, new String[] { "histogram", METRIC_STRING, 
        "1356998400", "42" }).joinUninterruptibly();
    validateCounters(1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
  @Test
  public void executeMissingTags() throws Exception {
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rpc.execute(tsdb, chan, new String[] { "histogram", METRIC_STRING, 
        "1356998400", "42", 
        HistogramPojo.bytesToBase64String(test_histo.histogram(false)), 
        "" }).joinUninterruptibly();
    validateCounters(1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }

  @Test
  public void executeNSUNTagk() throws Exception {
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rpc.execute(tsdb, chan, new String[] { "histogram", METRIC_STRING, 
        "1356998400", "42", 
        HistogramPojo.bytesToBase64String(test_histo.histogram(false)), 
        NSUN_TAGK + "=" + TAGV_STRING }).joinUninterruptibly();
    validateCounters(1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
  @Test
  public void executeNSUNTagV() throws Exception {
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    final Channel chan = NettyMocks.fakeChannel();
    rpc.execute(tsdb, chan, new String[] { "histogram", METRIC_STRING, 
        "1356998400", "42", 
        HistogramPojo.bytesToBase64String(test_histo.histogram(false)), 
        TAGK_STRING + "=" + NSUN_TAGV }).joinUninterruptibly();
    validateCounters(1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0);
    verify(chan, times(1)).write(any());
    verify(chan, times(1)).isConnected();
    validateSEH(false);
  }
  
  @Test
  public void executeHttpSingle() throws Exception {
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/histogram",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1356998400,"
            + "\"overflow\":1,\"buckets\":{\"0,1\":42,\"1,5\":24}" 
            + ",\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    rpc.execute(tsdb, query);
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateSEH(false);
    
    byte[] qualifier = new byte[] {0x06, 0, 0};
    byte[] value = storage.getColumn(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), qualifier);
    assertArrayEquals(test_histo.histogram(true), value);
  }
  
  @Test
  public void executeHttpSingleBinary() throws Exception {
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/histogram",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1356998400,"
            + "\"id\":42,\"value\":\"" 
            + HistogramPojo.bytesToBase64String(test_histo.histogram(false))
            + "\",\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    rpc.execute(tsdb, query);
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateSEH(false);
    
    byte[] qualifier = new byte[] {0x06, 0, 0};
    byte[] value = storage.getColumn(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), qualifier);
    assertArrayEquals(test_histo.histogram(true), value);
  }
  
  @Test
  public void executeHttpTwoBinary() throws Exception {
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/histogram",
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1356998400,"
            + "\"id\":42,\"value\":\"" 
            + HistogramPojo.bytesToBase64String(test_histo.histogram(false))
            + "\",\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}},"
            + "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1356998460,"
            + "\"id\":42,\"value\":\"" 
            + HistogramPojo.bytesToBase64String(test_histo.histogram(false))
            + "\",\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    rpc.execute(tsdb, query);
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateSEH(false);
    
    byte[] qualifier = new byte[] {0x06, 0, 0};
    byte[] value = storage.getColumn(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), qualifier);
    assertArrayEquals(test_histo.histogram(true), value);
    
    qualifier = new byte[] { 0x06, 0, 0x3C };
    value = storage.getColumn(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), qualifier);
    assertArrayEquals(test_histo.histogram(true), value);
  }
  
  @Test
  public void executeHttpTwoOneGoodOneBadBinary() throws Exception {
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/histogram",
        "[{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1356998400,"
            + "\"id\":42,\"value\":\"" 
            + HistogramPojo.bytesToBase64String(test_histo.histogram(false))
            + "\",\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}},"
            + "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1356998460,"
            + "\"id\":42,\"value\":\"" 
            + HistogramPojo.bytesToBase64String(test_histo.histogram(false))
              .substring(0, 4)
            + "\",\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}]");
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    rpc.execute(tsdb, query);
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 2, 1);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    validateSEH(false);
    
    byte[] qualifier = new byte[] {0x06, 0, 0};
    byte[] value = storage.getColumn(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), qualifier);
    assertArrayEquals(test_histo.histogram(true), value);
    
    qualifier = new byte[] { 0x06, 0, 0x3C };
    assertNull(storage.getColumn(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), qualifier));
  }
  
  @Test
  public void httpNSUNMetric() throws Exception {
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/histogram",
        "{\"metric\":\"" + NSUN_METRIC + "\",\"timestamp\":1356998400,"
            + "\"id\":42,\"value\":\"" 
            + HistogramPojo.bytesToBase64String(test_histo.histogram(false))
            + "\",\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0);
    validateSEH(false);
  }
  
  @Test
  public void httpNSUNTagk() throws Exception {
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/histogram",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1356998400,"
            + "\"id\":42,\"value\":\"" 
            + HistogramPojo.bytesToBase64String(test_histo.histogram(false))
            + "\",\"tags\":{\"" + NSUN_TAGK + "\":\"" + TAGV_STRING + "\"}}");
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0);
    validateSEH(false);
  }
  
  @Test
  public void httpNSUNTagv() throws Exception {
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/histogram",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1356998400,"
            + "\"id\":42,\"value\":\"" 
            + HistogramPojo.bytesToBase64String(test_histo.histogram(false))
            + "\",\"tags\":{\"" + TAGK_STRING + "\":\"" + NSUN_TAGV + "\"}}");
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0);
    validateSEH(false);
  }

  @Test
  public void httpHBaseError() throws Exception {
    storage.throwException(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), mock(HBaseException.class));
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/histogram",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1356998400,"
            + "\"id\":42,\"value\":\"" 
            + HistogramPojo.bytesToBase64String(test_histo.histogram(false))
            + "\",\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0);
    validateSEH(true);
  }

  @Test
  public void httpPleaseThrottleError() throws Exception {
    storage.throwException(getRowKey(METRIC_STRING, 1356998400, 
        TAGK_STRING, TAGV_STRING), mock(PleaseThrottleException.class));
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/histogram",
        "{\"metric\":\"" + METRIC_STRING + "\",\"timestamp\":1356998400,"
            + "\"id\":42,\"value\":\"" 
            + HistogramPojo.bytesToBase64String(test_histo.histogram(false))
            + "\",\"tags\":{\"" + TAGK_STRING + "\":\"" + TAGV_STRING + "\"}}");
    final HistogramDataPointRpc rpc = new HistogramDataPointRpc(tsdb.getConfig());
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    validateCounters(0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0);
    validateSEH(true);
  }
  
  @Override
  protected void validateSEH(final boolean called) {
    if (called) {
      if (handler != null) {
        verify(handler, times(1)).handleError((IncomingDataPoint)any(), 
            (Exception)any());
      }
    } else {
      if (handler != null) {
        verify(handler, never()).handleError((IncomingDataPoint)any(), 
            (Exception)any());
      }
    }
  }
  
  protected void validateCounters(
      final long telnet_requests,
      final long http_requests,
      final long raw_dps,
      final long rollup_dps,
      final long raw_stored,
      final long rollup_stored,
      final long hbase_errors,
      final long unknown_errors,
      final long invalid_values,
      final long illegal_arguments,
      final long unknown_metrics,
      final long inflight_exceeded,
      final long writes_blocked,
      final long writes_timedout,
      final long requests_timedout,
      final long raw_histograms,
      final long raw_histograms_stored) {
    assertEquals(telnet_requests, this.telnet_requests.get());
    assertEquals(http_requests, this.http_requests.get());
    assertEquals(raw_dps, this.raw_dps.get());
    assertEquals(rollup_dps, this.rollup_dps.get());
    assertEquals(raw_stored, this.raw_stored.get());
    assertEquals(rollup_stored, this.rollup_stored.get());
    assertEquals(hbase_errors, this.hbase_errors.get());
    assertEquals(unknown_errors, this.unknown_errors.get());
    assertEquals(invalid_values, this.invalid_values.get());
    assertEquals(illegal_arguments, this.illegal_arguments.get());
    assertEquals(unknown_metrics, this.unknown_metrics.get());
    assertEquals(inflight_exceeded, this.inflight_exceeded.get());
    assertEquals(writes_blocked, this.writes_blocked.get());
    assertEquals(writes_timedout, this.writes_timedout.get());
    assertEquals(requests_timedout, this.requests_timedout.get());
    assertEquals(raw_histograms, this.raw_histograms.get());
    assertEquals(raw_histograms_stored, this.raw_histograms_stored.get());
  }
}
