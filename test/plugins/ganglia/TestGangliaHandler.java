// This file is part of OpenTSDB.
// Copyright (C) 2014 The OpenTSDB Authors.
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
package net.opentsdb.plugins.ganglia;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyFloat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import org.acplt.oncrpc.XdrDecodingStream;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.TSDB;
import net.opentsdb.plugins.ganglia.GangliaMessage.ProtocolVersion;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.Config;


/** Tests {@link GangliaHandler}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, Deferred.class, StatsCollector.class })
public class TestGangliaHandler {

  private static final long NOW_MILLIS = 1397000152;
  private static final String FOO_IPADDR = "foo_ip";
  private static final String FOO_HOSTNAME = "foo_hostname";
  private static final List<GangliaMessage.Tag> TAGS = Lists.newArrayList(
      GangliaMessage.createHostnameTag(FOO_HOSTNAME),
      GangliaMessage.createIpAddrTag(FOO_IPADDR));

  @Mock private TSDB mock_tsdb;
  @Mock private ChannelHandlerContext mock_ctx;
  @Mock private MessageEvent mock_event;
  private InetSocketAddress inet_socket_addr;
  @Mock private GangliaV30xMessage mock_v30x_message;
  @Mock private GangliaV31xValueMessage mock_v31x_value_msg;
  private GangliaV31xMetadataMessage v31x_metadata_msg;
  @Mock private XdrDecodingStream mock_xdr_decoding;
  @Mock private InetAddress mock_inet_addr;
  @Mock private Deferred<Object> mock_deffered;
  @Mock private StatsCollector mock_stats_collector;
  @Captor private ArgumentCaptor<String> metric_name_captor;
  @Captor private ArgumentCaptor<Long> timestamp_captor;
  @Captor private ArgumentCaptor<Long> long_captor;
  @Captor private ArgumentCaptor<Float> float_captor;
  @Captor private ArgumentCaptor<HashMap<String, String>> tags_captor;
  @Captor private ArgumentCaptor<Number> number_captor;
  @Captor private ArgumentCaptor<Number> number_captor2;
  @Captor private ArgumentCaptor<Number> number_captor3;
  @Captor private ArgumentCaptor<Number> number_captor4;
  @Captor private ArgumentCaptor<Number> number_captor5;
  @Captor private ArgumentCaptor<Number> number_captor6;
  private GangliaMessage.Metric metric;
  private Config config;
  private GangliaHandler.CountersForTesting counters;
  private GangliaHandler handler;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    config = new Config(false);
    inet_socket_addr = new InetSocketAddress(mock_inet_addr, 8080);
    when(mock_inet_addr.getHostAddress()).thenReturn(FOO_IPADDR);
    when(mock_inet_addr.getCanonicalHostName()).thenReturn(FOO_HOSTNAME);
    when(mock_event.getRemoteAddress()).thenReturn(inet_socket_addr);
    when(mock_event.getMessage()).thenReturn(mock_v30x_message);
    when(mock_v30x_message.protocolVersion()).thenReturn(ProtocolVersion.V30x);
    when(mock_v30x_message.hasError()).thenReturn(false);
    when(mock_v30x_message.getTags(any(InetAddress.class))).thenReturn(TAGS);
    when(mock_v30x_message.getSpoofHeaderIfSpoofMetric()).thenReturn(null);
    when(mock_v31x_value_msg.protocolVersion()).thenReturn(ProtocolVersion.V31x);
    when(mock_v31x_value_msg.hasError()).thenReturn(false);
    when(mock_v31x_value_msg.getTags(any(InetAddress.class))).thenReturn(TAGS);
    v31x_metadata_msg = GangliaV31xMetadataMessage.decode(mock_xdr_decoding);
    when(mock_tsdb.addPoint(anyString(), anyLong(), anyLong(),
        any(HashMap.class))).thenReturn(mock_deffered);
    when(mock_tsdb.addPoint(anyString(), anyLong(), anyFloat(),
        any(HashMap.class))).thenReturn(mock_deffered);
    // Mocks final methods.
    mock_stats_collector = mock(StatsCollector.class);
    counters = new GangliaHandler.CountersForTesting();
    handler = GangliaHandler.ForTesting(config, mock_tsdb, counters);
  }

  @SuppressWarnings("unchecked")
  private void checkNoTsdbInteraction() {
    verify(mock_tsdb, never()).addPoint(anyString(), anyLong(), anyLong(),
        any(HashMap.class));
    verify(mock_tsdb, never()).addPoint(anyString(), anyLong(), anyFloat(),
        any(HashMap.class));
    verify(mock_tsdb, never()).addPoint(anyString(), anyLong(), anyDouble(),
        any(HashMap.class));
  }

  @Test
  public void testMessageReceived() throws Exception {
    // Decodes and stores a Ganglia metric message into TSDB.
    metric = new GangliaMessage.Metric("foo_metric", NOW_MILLIS, 311);
    when(mock_v30x_message.getMetricIfNumeric()).thenReturn(metric);

    handler.messageReceived(mock_ctx, mock_event);
    verify(mock_v30x_message).getMetricIfNumeric();
    verify(mock_tsdb).addPoint(metric_name_captor.capture(),
        timestamp_captor.capture(), long_captor.capture(),
        tags_captor.capture());
    assertEquals("foo_metric", metric_name_captor.getValue());
    assertEquals(NOW_MILLIS, timestamp_captor.getValue().longValue());
    assertEquals(311, long_captor.getValue().longValue());
    final HashMap<String, String> tags = tags_captor.getValue();
    assertEquals(FOO_IPADDR, tags.get("ipaddr"));
    assertEquals(FOO_HOSTNAME, tags.get("host"));
    assertNull(tags.get("cluster"));
    new ExpectedCounts().setResuests(1).setSuccesses(1).checkV30x();
  }

  @Test
  public void testMessageReceived_v31xValueMetric() throws Exception {
    // Decodes and stores a Ganglia v31x value metric message into TSDB.
    when(mock_event.getMessage()).thenReturn(mock_v31x_value_msg);
    metric = new GangliaMessage.Metric("foo_metric", NOW_MILLIS, 311);
    when(mock_v31x_value_msg.getMetricIfNumeric()).thenReturn(metric);

    handler.messageReceived(mock_ctx, mock_event);
    verify(mock_v31x_value_msg).getMetricIfNumeric();
    verify(mock_tsdb).addPoint(metric_name_captor.capture(),
        timestamp_captor.capture(), long_captor.capture(),
        tags_captor.capture());
    assertEquals("foo_metric", metric_name_captor.getValue());
    assertEquals(NOW_MILLIS, timestamp_captor.getValue().longValue());
    assertEquals(311, long_captor.getValue().longValue());
    final HashMap<String, String> tags = tags_captor.getValue();
    assertEquals(FOO_IPADDR, tags.get("ipaddr"));
    assertEquals(FOO_HOSTNAME, tags.get("host"));
    assertNull(tags.get("cluster"));
    new ExpectedCounts().setResuests(1).setSuccesses(1).checkV31x();
  }

  @Test
  public void testMessageReceived_v31xMetadata() throws Exception {
    // Decodes and stores a Ganglia v31x metadata message into TSDB.
    when(mock_event.getMessage()).thenReturn(v31x_metadata_msg);

    handler.messageReceived(mock_ctx, mock_event);
    checkNoTsdbInteraction();
    new ExpectedCounts().setResuests(1).setMetadata(1).checkV31x();
  }

  @Test
  public void testMessageReceived_floatingValue() throws Exception {
    // Decodes and stores a float Ganglia metric value into TSDB.
    metric = new GangliaMessage.Metric("foo_metric", NOW_MILLIS, 311.0F);
    when(mock_v30x_message.getMetricIfNumeric()).thenReturn(metric);

    handler.messageReceived(mock_ctx, mock_event);
    verify(mock_tsdb).addPoint(metric_name_captor.capture(),
        timestamp_captor.capture(), float_captor.capture(),
        tags_captor.capture());
    assertEquals("foo_metric", metric_name_captor.getValue());
    assertEquals(NOW_MILLIS, timestamp_captor.getValue().longValue());
    assertEquals(311, float_captor.getValue().floatValue(), 0.1);
    final HashMap<String, String> tags = tags_captor.getValue();
    assertEquals(FOO_IPADDR, tags.get("ipaddr"));
    assertEquals(FOO_HOSTNAME, tags.get("host"));
    assertNull(tags.get("cluster"));
    new ExpectedCounts().setResuests(1).setSuccesses(1).checkV30x();
  }

  @Test
  public void testMessageReceived_prefixAndClusterTag() throws Exception {
    // Adds a prefix to metric name and adds a cluster name to the tags of
    // the imported metric.
    metric = new GangliaMessage.Metric("foo_metric", NOW_MILLIS, 311);
    when(mock_v30x_message.getMetricIfNumeric()).thenReturn(metric);
    config.overrideConfig("tsd.ganglia.name.prefix", "gg_");
    config.overrideConfig("tsd.ganglia.tags.cluster", "adhoc");

    handler = new GangliaHandler(new GangliaConfig(config), mock_tsdb);
    handler.messageReceived(mock_ctx, mock_event);
    verify(mock_tsdb).addPoint(metric_name_captor.capture(),
        timestamp_captor.capture(), long_captor.capture(),
        tags_captor.capture());
    assertEquals("gg_foo_metric", metric_name_captor.getValue());
    assertEquals(NOW_MILLIS, timestamp_captor.getValue().longValue());
    assertEquals(311, long_captor.getValue().longValue());
    final HashMap<String, String> tags = tags_captor.getValue();
    assertEquals(FOO_IPADDR, tags.get("ipaddr"));
    assertEquals(FOO_HOSTNAME, tags.get("host"));
    assertEquals("adhoc", tags.get("cluster"));
  }

  @Test
  public void testMessageReceived_emptyMessasge() throws Exception {
    // Counts empty message as an error.
    when(mock_event.getMessage()).thenReturn(GangliaV30xMessage.emptyMessage());
    handler.messageReceived(mock_ctx, mock_event);
    checkNoTsdbInteraction();
    new ExpectedCounts().setResuests(1).setErrors(1).checkV30x();
  }

  @Test
  public void testExceptionCaught() throws Exception {
    // Keeps the statistics of channel exceptions.
    ExceptionEvent err = mock(ExceptionEvent.class);
    handler.exceptionCaught(mock_ctx, err);
    new ExpectedCounts().setChannelExceptions(1).checkV30x();
  }

  @Test
  public void testMessageReceived_nonNmericMetric() throws Exception {
    // Ignores non-numerics Ganglia metric message.
    when(mock_v30x_message.getMetricIfNumeric()).thenReturn(null);
    handler.messageReceived(mock_ctx, mock_event);
    checkNoTsdbInteraction();
    verify(mock_v30x_message).getMetricIfNumeric();
    new ExpectedCounts().setResuests(1).setNonNumericValues(1).checkV30x();
  }

  /** Manages expected counters after processing a Ganglia message. */
  private class ExpectedCounts {

    private long requests = 0;
    private long successes = 0;
    private long non_numeric_values = 0;
    private long errors = 0;
    private long channel_exceptions = 0;
    private long num_metadata = 0;

    private void checkV30x() {
      counters.collectStats(mock_stats_collector);
      verify(mock_stats_collector).record(eq("ganglia.v30x"),
          number_captor.capture(), eq("type=requests"));
      assertEquals(requests, number_captor.getValue().longValue());
      verify(mock_stats_collector).record(eq("ganglia.v30x"),
          number_captor2.capture(), eq("type=successes"));
      assertEquals(successes, number_captor2.getValue().longValue());
      verify(mock_stats_collector).record(eq("ganglia.v30x"),
          number_captor3.capture(), eq("type=non_numeric_values"));
      assertEquals(non_numeric_values, number_captor3.getValue().longValue());
      verify(mock_stats_collector).record(eq("ganglia.msg"),
          number_captor4.capture(), eq("type=errors"));
      assertEquals(errors, number_captor4.getValue().longValue());
      verify(mock_stats_collector).record(eq("ganglia.channel"),
          number_captor5.capture(), eq("type=exceptions"));
      assertEquals(channel_exceptions, number_captor5.getValue().longValue());
    }

    private void checkV31x() {
      counters.collectStats(mock_stats_collector);
      verify(mock_stats_collector).record(eq("ganglia.v31x"),
          number_captor.capture(), eq("type=requests"));
      assertEquals(requests, number_captor.getValue().longValue());
      verify(mock_stats_collector).record(eq("ganglia.v31x"),
          number_captor2.capture(), eq("type=successes"));
      assertEquals(successes, number_captor2.getValue().longValue());
      verify(mock_stats_collector).record(eq("ganglia.v31x"),
          number_captor3.capture(), eq("type=non_numeric_values"));
      assertEquals(non_numeric_values, number_captor3.getValue().longValue());
      verify(mock_stats_collector).record(eq("ganglia.v31x"),
          number_captor4.capture(), eq("type=metadata"));
      assertEquals(num_metadata, number_captor4.getValue().longValue());
      verify(mock_stats_collector).record(eq("ganglia.msg"),
          number_captor5.capture(), eq("type=errors"));
      assertEquals(errors, number_captor5.getValue().longValue());
      verify(mock_stats_collector).record(eq("ganglia.channel"),
          number_captor6.capture(), eq("type=exceptions"));
      assertEquals(channel_exceptions, number_captor6.getValue().longValue());
    }

    private ExpectedCounts setResuests(long requests) {
      this.requests = requests;
      return this;
    }

    private ExpectedCounts setSuccesses(long successes) {
      this.successes = successes;
      return this;
    }

    private ExpectedCounts setNonNumericValues(long non_numeric_values) {
      this.non_numeric_values = non_numeric_values;
      return this;
    }

    private ExpectedCounts setMetadata(long num_metadata) {
      this.num_metadata  = num_metadata;
      return this;
    }

    private ExpectedCounts setErrors(long errors) {
      this.errors = errors;
      return this;
    }

    private ExpectedCounts setChannelExceptions(long channel_exceptions) {
      this.channel_exceptions = channel_exceptions;
      return this;
    }
  }
}
