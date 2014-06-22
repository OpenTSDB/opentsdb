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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Iterator;

import info.ganglia.gmetric4j.gmetric.GMetricSlope;
import info.ganglia.gmetric4j.gmetric.GMetricType;
import info.ganglia.gmetric4j.xdr.v30x.Ganglia_gmetric_message;
import info.ganglia.gmetric4j.xdr.v30x.Ganglia_message;
import info.ganglia.gmetric4j.xdr.v30x.Ganglia_message_formats;
import info.ganglia.gmetric4j.xdr.v30x.Ganglia_spoof_header;
import info.ganglia.gmetric4j.xdr.v30x.Ganglia_spoof_message;

import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrBufferDecodingStream;
import org.acplt.oncrpc.XdrBufferEncodingStream;
import org.acplt.oncrpc.XdrDecodingStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import net.opentsdb.plugins.ganglia.GangliaMessage.Metric;
import net.opentsdb.plugins.ganglia.GangliaMessage.Tag;
import net.opentsdb.plugins.ganglia.GangliaV30xMessage.ForTesting;


/** Tests {@link GangliaV30xMessage}. */
public class TestGangliaV30xMessage {

  private static final String FOO_IPADDR = "foo_ip";
  private static final String FOO_HOSTNAME = "foo_hostname";

  @Mock private InetAddress mock_inet_addr;

  private XdrBufferEncodingStream xdr_encoding;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(mock_inet_addr.getHostAddress()).thenReturn(FOO_IPADDR);
    when(mock_inet_addr.getCanonicalHostName()).thenReturn(FOO_HOSTNAME);
    xdr_encoding = new XdrBufferEncodingStream(2048);
  }

  private Ganglia_gmetric_message buildGMetric(final String name,
      final String value, final GMetricType type) {
    final Ganglia_gmetric_message gmetric = new Ganglia_gmetric_message();
    gmetric.name = name;
    gmetric.value = value;
    gmetric.type = type.getGangliaType();
    // NOTE: We don't care the following fields.
    gmetric.units = "";
    gmetric.slope = GMetricSlope.BOTH.getGangliaSlope();
    gmetric.tmax = 10;
    gmetric.dmax = 100;
    return gmetric;
  }

  private Ganglia_message buildGangliaMessage(
      final Ganglia_gmetric_message gmetric) {
    final Ganglia_message msg = new Ganglia_message();
    msg.id = Ganglia_message_formats.metric_user_defined;
    msg.gmetric = gmetric;
    return msg;
  }

  private Ganglia_spoof_header buildSpoofHeader(final String ipaddr,
      final String hostname) {
    final Ganglia_spoof_header spheader = new Ganglia_spoof_header();
    spheader.spoofIP = ipaddr;
    spheader.spoofName = hostname;
    return spheader;
  }

  private Ganglia_spoof_message buildSpoofMetric(
      final Ganglia_spoof_header spheader,
      final Ganglia_gmetric_message gmetric) {
    final Ganglia_spoof_message spmetric = new Ganglia_spoof_message();
    spmetric.spheader = spheader;
    spmetric.gmetric = gmetric;
    return spmetric;
  }

  private Ganglia_message buildGangliaMessage(
      final Ganglia_spoof_message spmetric) {
    final Ganglia_message msg = new Ganglia_message();
    msg.id = Ganglia_message_formats.spoof_metric;
    msg.spmetric = spmetric;
    return msg;
  }

  private void encode(final Ganglia_message msg) throws Exception {
    InetAddress any_address = Inet4Address.getLocalHost();
    int any_port = 8080;
    xdr_encoding.beginEncoding(any_address, any_port);
    msg.xdrEncode(xdr_encoding);
    xdr_encoding.endEncoding();
  }

  private GangliaV30xMessage decode() throws OncRpcException, IOException {
    XdrDecodingStream xdr_decoding =  new XdrBufferDecodingStream(
        xdr_encoding.getXdrData(), xdr_encoding.getXdrLength());
    return GangliaV30xMessage.decode(xdr_decoding);
  }

  @Test
  public void testCanDecode() {
    for (int id = Ganglia_message_formats.metric_user_defined;
        id < Ganglia_message_formats.GANGLIA_NUM_25_METRICS; ++id) {
      assertTrue("id=" + id, GangliaV30xMessage.canDecode(id));
    }
    for (int id = Ganglia_message_formats.GANGLIA_NUM_25_METRICS;
        id < Ganglia_message_formats.spoof_metric; ++id) {
      assertFalse("id=" + id, GangliaV30xMessage.canDecode(id));
    }
    assertTrue(GangliaV30xMessage.canDecode(
        Ganglia_message_formats.spoof_metric));
    assertTrue(GangliaV30xMessage.canDecode(
        Ganglia_message_formats.spoof_heartbeat));
    assertFalse(GangliaV30xMessage.canDecode(
        Ganglia_message_formats.spoof_heartbeat + 1));
  }

  @Test
  public void testDecode_userDefined() throws Exception {
    // Decodes a user-defined gmetric message.
    Ganglia_gmetric_message gmetric =
        buildGMetric("metric_foo", "177", GMetricType.UINT32);
    encode(buildGangliaMessage(gmetric));

    GangliaV30xMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V30x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertFalse(msg.isMetadata());
    assertEquals(Ganglia_message_formats.metric_user_defined,
        ForTesting.get_ganglia_message(msg).id);
    Ganglia_spoof_header spheader = msg.getSpoofHeaderIfSpoofMetric();
    assertNull(spheader);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("metric_foo", metric.name());
    assertTrue(metric.isInteger());
    assertEquals(177, metric.longValue());
  }

  @Test
  public void testDecode_spoofMetric() throws Exception {
    // Decodes a spoof metric message.
    Ganglia_gmetric_message gmetric =
        buildGMetric("metric_spfoo", "16.7", GMetricType.FLOAT);
    Ganglia_spoof_message spmetric = buildSpoofMetric(
        buildSpoofHeader("spoof_ipaddr", "spoof_host"), gmetric);
    encode(buildGangliaMessage(spmetric));

    GangliaV30xMessage msg = decode();
    assertFalse(msg.hasError());
    assertEquals(Ganglia_message_formats.spoof_metric,
        ForTesting.get_ganglia_message(msg).id);
    Ganglia_spoof_header spheader = msg.getSpoofHeaderIfSpoofMetric();
    assertNotNull(spheader);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("metric_spfoo", metric.name());
    assertFalse(metric.isInteger());
    assertEquals(16.7, metric.floatValue(), 0.1);
  }

  @Test
  public void testDecode_userDefinedString() throws Exception {
    // Ignores a user-defined gmetric message of string values.
    Ganglia_gmetric_message gmetric =
        buildGMetric("metric_foo", "177", GMetricType.STRING);
    encode(buildGangliaMessage(gmetric));

    GangliaV30xMessage msg = decode();
    assertFalse(msg.hasError());
    assertEquals(Ganglia_message_formats.metric_user_defined,
        ForTesting.get_ganglia_message(msg).id);
    Ganglia_spoof_header spheader = msg.getSpoofHeaderIfSpoofMetric();
    assertNull(spheader);
    Metric metric = msg.getMetricIfNumeric();
    assertNull(metric);
  }

  @Test
  public void testDecode_spoofMetricString() throws Exception {
    // Ignores a spoof metric message of string values.
    Ganglia_gmetric_message gmetric =
        buildGMetric("metric_spfoo", "16.7", GMetricType.STRING);
    Ganglia_spoof_message spmetric = buildSpoofMetric(
        buildSpoofHeader("foo_ipaddr", "foo_host"), gmetric);
    encode(buildGangliaMessage(spmetric));

    GangliaV30xMessage msg = decode();
    assertFalse(msg.hasError());
    assertEquals(Ganglia_message_formats.spoof_metric,
        ForTesting.get_ganglia_message(msg).id);
    Ganglia_spoof_header spheader = msg.getSpoofHeaderIfSpoofMetric();
    assertNotNull(spheader);
    Metric metric = msg.getMetricIfNumeric();
    assertNull(metric);
  }

  @Test
  public void testDecode_cpuNum() throws Exception {
    // Decodes the base metric of two-byte cpu num metric message.
    Ganglia_message src_msg = new Ganglia_message();
    src_msg.id = Ganglia_message_formats.metric_cpu_num;
    src_msg.u_short1 = 711;
    encode(src_msg);

    GangliaV30xMessage msg = decode();
    assertFalse(msg.hasError());
    assertEquals(Ganglia_message_formats.metric_cpu_num,
        ForTesting.get_ganglia_message(msg).id);
    Ganglia_spoof_header spheader = msg.getSpoofHeaderIfSpoofMetric();
    assertNull(spheader);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("cpu_num", metric.name());
    assertTrue(metric.isInteger());
    assertEquals(711, metric.longValue());
  }

  @Test
  public void testDecode_cpuSpeed() throws Exception {
    // Decodes the base metric of four-byte cpu speed metric message.
    Ganglia_message src_msg = new Ganglia_message();
    src_msg.id = Ganglia_message_formats.metric_cpu_speed;
    src_msg.u_int1 = 811;
    encode(src_msg);

    GangliaV30xMessage msg = decode();
    assertFalse(msg.hasError());
    assertEquals(Ganglia_message_formats.metric_cpu_speed,
        ForTesting.get_ganglia_message(msg).id);
    Ganglia_spoof_header spheader = msg.getSpoofHeaderIfSpoofMetric();
    assertNull(spheader);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("cpu_speed", metric.name());
    assertTrue(metric.isInteger());
    assertEquals(811, metric.longValue());
  }

  @Test
  public void testDecode_osName() throws Exception {
    // Ignores the base metric of a string OS name.
    Ganglia_message src_msg = new Ganglia_message();
    src_msg.id = Ganglia_message_formats.metric_os_name;
    src_msg.str = "foo_os";
    encode(src_msg);

    GangliaV30xMessage msg = decode();
    assertFalse(msg.hasError());
    assertEquals(Ganglia_message_formats.metric_os_name,
        ForTesting.get_ganglia_message(msg).id);
    Ganglia_spoof_header spheader = msg.getSpoofHeaderIfSpoofMetric();
    assertNull(spheader);
    Metric metric = msg.getMetricIfNumeric();
    assertNull(metric);
  }

  @Test
  public void testDecode_pktsIn() throws Exception {
    // Decodes the base metric of a float pkt-in metric message.
    Ganglia_message src_msg = new Ganglia_message();
    src_msg.id = Ganglia_message_formats.metric_pkts_in;
    src_msg.f = 177177;
    encode(src_msg);

    GangliaV30xMessage msg = decode();
    assertFalse(msg.hasError());
    assertEquals(Ganglia_message_formats.metric_pkts_in,
        ForTesting.get_ganglia_message(msg).id);
    Ganglia_spoof_header spheader = msg.getSpoofHeaderIfSpoofMetric();
    assertNull(spheader);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("pkts_in", metric.name());
    assertFalse(metric.isInteger());
    assertEquals(177177, metric.floatValue(), 0.1);
  }

  @Test
  public void testDecode_diskTotal() throws Exception {
    // Decodes the base metric of a double disk-total metric message.
    Ganglia_message src_msg = new Ganglia_message();
    src_msg.id = Ganglia_message_formats.metric_disk_total;
    src_msg.d = 177177;
    encode(src_msg);

    GangliaV30xMessage msg = decode();
    assertFalse(msg.hasError());
    assertEquals(Ganglia_message_formats.metric_disk_total,
        ForTesting.get_ganglia_message(msg).id);
    Ganglia_spoof_header spheader = msg.getSpoofHeaderIfSpoofMetric();
    assertNull(spheader);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("disk_total", metric.name());
    assertFalse(metric.isInteger());
    assertEquals(177177, metric.floatValue(), 0.1);
  }

  @Test
  public void testEmptyMessage() {
    assertTrue(GangliaV30xMessage.emptyMessage().hasError());
  }

  @Test(expected = OutOfMemoryError.class)
  public void testDecode_OutOfMemoryError() throws Exception {
    XdrDecodingStream xdr = mock(XdrDecodingStream.class);
    when(xdr.xdrDecodeInt()).thenThrow(new OutOfMemoryError(""));
    GangliaV30xMessage.decode(xdr);
  }

  @Test(expected = IOException.class)
  public void testDecode_IOException() throws Exception {
    XdrDecodingStream xdr = mock(XdrDecodingStream.class);
    when(xdr.xdrDecodeInt()).thenThrow(new IOException(""));
    GangliaV30xMessage.decode(xdr);
  }

  @Test(expected = OncRpcException.class)
  public void testDecode_OncRpcException() throws Exception {
    XdrDecodingStream xdr = mock(XdrDecodingStream.class);
    when(xdr.xdrDecodeInt()).thenThrow(new OncRpcException(""));
    GangliaV30xMessage.decode(xdr);
  }

  @Test
  public void testGetTags() throws Exception {
    Ganglia_message src_msg = new Ganglia_message();
    src_msg.id = Ganglia_message_formats.metric_cpu_num;
    src_msg.u_short1 = 711;
    encode(src_msg);

    GangliaV30xMessage msg = decode();
    Iterator<Tag> tags = msg.getTags(mock_inet_addr).iterator();
    assertTrue(tags.hasNext());
    GangliaMessage.Tag tag = tags.next();
    assertEquals("host", tag.key);
    assertEquals(FOO_HOSTNAME, tag.value);
    assertTrue(tags.hasNext());
    tag = tags.next();
    assertEquals("ipaddr", tag.key);
    assertEquals(FOO_IPADDR, tag.value);
    assertFalse(tags.hasNext());
  }

  @Test
  public void testGetTags_spoofMetric() throws Exception {
    // Decodes a spoof metric message.
    Ganglia_gmetric_message gmetric =
        buildGMetric("metric_spfoo", "16.7", GMetricType.FLOAT);
    Ganglia_spoof_message spmetric = buildSpoofMetric(
        buildSpoofHeader("spoof_ipaddr", "spoof_host"), gmetric);
    encode(buildGangliaMessage(spmetric));

    GangliaV30xMessage msg = decode();
    Iterator<Tag> tags = msg.getTags(mock_inet_addr).iterator();
    assertTrue(tags.hasNext());
    GangliaMessage.Tag tag = tags.next();
    assertEquals("host", tag.key);
    assertEquals("spoof_host", tag.value);
    assertTrue(tags.hasNext());
    tag = tags.next();
    assertEquals("ipaddr", tag.key);
    assertEquals("spoof_ipaddr", tag.value);
    assertFalse(tags.hasNext());
  }
}
