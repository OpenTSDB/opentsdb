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

import info.ganglia.gmetric4j.xdr.v31x.Ganglia_gmetric_double;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_gmetric_float;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_gmetric_int;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_gmetric_short;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_gmetric_string;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_gmetric_uint;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_gmetric_ushort;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_metric_id;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_msg_formats;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_value_msg;

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
import net.opentsdb.plugins.ganglia.GangliaV31xValueMessage.ForTesting;


/** Tests {@link GangliaV31xValueMessage}. */
public class TestGangliaV31xValueMessage {

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

  private void encode(final Ganglia_value_msg msg) throws Exception {
    InetAddress any_address = Inet4Address.getLocalHost();
    int any_port = 8080;
    xdr_encoding.beginEncoding(any_address, any_port);
    msg.xdrEncode(xdr_encoding);
    xdr_encoding.endEncoding();
  }

  private GangliaV31xValueMessage decode() throws OncRpcException, IOException {
    XdrDecodingStream xdr_decoding =  new XdrBufferDecodingStream(
        xdr_encoding.getXdrData(), xdr_encoding.getXdrLength());
    return GangliaV31xValueMessage.decode(xdr_decoding);
  }

  private Ganglia_metric_id buildMetricId(String host, String name,
      boolean spoof) {
    Ganglia_metric_id id = new Ganglia_metric_id();
    id.host = host;
    id.name = name;
    id.spoof = spoof;
    return id;
  }

  @Test
  public void testCanDecode() {
    assertFalse(GangliaV31xValueMessage.canDecode(
        Ganglia_msg_formats.gmetadata_full));
    assertFalse(GangliaV31xValueMessage.canDecode(
        Ganglia_msg_formats.gmetadata_request));
    for (int id = Ganglia_msg_formats.gmetadata_full + 1;
        id < Ganglia_msg_formats.gmetadata_request; ++id) {
      assertTrue("id=" + id, GangliaV31xValueMessage.canDecode(id));
    }
  }

  @Test
  public void testDecode_ushort() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_ushort;
    gmsg.gu_short = new Ganglia_gmetric_ushort();
    gmsg.gu_short.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gu_short.fmt = "%d";
    gmsg.gu_short.us = 177;
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertFalse(msg.isMetadata());
    assertEquals(Ganglia_msg_formats.gmetric_ushort,
        ForTesting.get_ganglia_message(msg).id);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("metric_foo", metric.name());
    assertTrue(metric.isInteger());
    assertEquals(177, metric.longValue());
    System.out.println(msg);
  }

  @Test
  public void testDecode_short() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_short;
    gmsg.gs_short = new Ganglia_gmetric_short();
    gmsg.gs_short.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gs_short.fmt = "%d";
    gmsg.gs_short.ss = 177;
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertEquals(Ganglia_msg_formats.gmetric_short,
        ForTesting.get_ganglia_message(msg).id);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("metric_foo", metric.name());
    assertTrue(metric.isInteger());
    assertEquals(177, metric.longValue());
    System.out.println(msg);
  }

  @Test
  public void testDecode_int() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_int;
    gmsg.gs_int = new Ganglia_gmetric_int();
    gmsg.gs_int.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gs_int.fmt = "%d";
    gmsg.gs_int.si = 177;
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertEquals(Ganglia_msg_formats.gmetric_int,
        ForTesting.get_ganglia_message(msg).id);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("metric_foo", metric.name());
    assertTrue(metric.isInteger());
    assertEquals(177, metric.longValue());
    System.out.println(msg);
  }

  @Test
  public void testDecode_uint() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_uint;
    gmsg.gu_int = new Ganglia_gmetric_uint();
    gmsg.gu_int.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gu_int.fmt = "%d";
    gmsg.gu_int.ui = 177;
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertEquals(Ganglia_msg_formats.gmetric_uint,
        ForTesting.get_ganglia_message(msg).id);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("metric_foo", metric.name());
    assertTrue(metric.isInteger());
    assertEquals(177, metric.longValue());
    System.out.println(msg);
  }

  @Test
  public void testDecode_float() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_float;
    gmsg.gf = new Ganglia_gmetric_float();
    gmsg.gf.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gf.fmt = "%f";
    gmsg.gf.f = 17.7f;
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertEquals(Ganglia_msg_formats.gmetric_float,
        ForTesting.get_ganglia_message(msg).id);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("metric_foo", metric.name());
    assertFalse(metric.isInteger());
    assertEquals(17.7, metric.floatValue(), 0.0001);
    System.out.println(msg);
  }

  @Test
  public void testDecode_double() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_double;
    gmsg.gd = new Ganglia_gmetric_double();
    gmsg.gd.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gd.fmt = "%f";
    gmsg.gd.d = 17.7;
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertEquals(Ganglia_msg_formats.gmetric_double,
        ForTesting.get_ganglia_message(msg).id);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("metric_foo", metric.name());
    assertFalse(metric.isInteger());
    assertEquals(17.7, metric.floatValue(), 0.0001);
    System.out.println(msg);
  }

  @Test
  public void testDecode_integerString() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_string;
    gmsg.gstr = new Ganglia_gmetric_string();
    gmsg.gstr.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gstr.fmt = "%s";
    gmsg.gstr.str = "177";
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertEquals(Ganglia_msg_formats.gmetric_string,
        ForTesting.get_ganglia_message(msg).id);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("metric_foo", metric.name());
    assertTrue(metric.isInteger());
    assertEquals(177, metric.longValue());
    System.out.println(msg);
  }

  @Test
  public void testDecode_floatString() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_string;
    gmsg.gstr = new Ganglia_gmetric_string();
    gmsg.gstr.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gstr.fmt = "%s";
    gmsg.gstr.str = "17.6543e+3";
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertEquals(Ganglia_msg_formats.gmetric_string,
        ForTesting.get_ganglia_message(msg).id);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("metric_foo", metric.name());
    assertFalse(metric.isInteger());
    assertEquals(17654.3, metric.floatValue(), 0.01);
    System.out.println(msg);
  }

  @Test
  public void testDecode_nonNumericString() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_string;
    gmsg.gstr = new Ganglia_gmetric_string();
    gmsg.gstr.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gstr.fmt = "%s";
    gmsg.gstr.str = "linux 2.6.5";
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertEquals(Ganglia_msg_formats.gmetric_string,
        ForTesting.get_ganglia_message(msg).id);
    assertNull(msg.getMetricIfNumeric());
    System.out.println(msg);
  }

  @Test
  public void testDecode_stringSimilarToNumeric() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_string;
    gmsg.gstr = new Ganglia_gmetric_string();
    gmsg.gstr.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gstr.fmt = "%s";
    gmsg.gstr.str = "2.6.5";
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertEquals(Ganglia_msg_formats.gmetric_string,
        ForTesting.get_ganglia_message(msg).id);
    assertNull(msg.getMetricIfNumeric());
  }

  @Test
  public void testDecode_stringNonNumericEee() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_string;
    gmsg.gstr = new Ganglia_gmetric_string();
    gmsg.gstr.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gstr.fmt = "%s";
    gmsg.gstr.str = "eee";
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertEquals(Ganglia_msg_formats.gmetric_string,
        ForTesting.get_ganglia_message(msg).id);
    assertNull(msg.getMetricIfNumeric());
  }

  @Test
  public void testDecode_stringMinusSignedFloat() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_string;
    gmsg.gstr = new Ganglia_gmetric_string();
    gmsg.gstr.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gstr.fmt = "%s";
    gmsg.gstr.str = "-2.6e5";
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertEquals(Ganglia_msg_formats.gmetric_string,
        ForTesting.get_ganglia_message(msg).id);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("metric_foo", metric.name());
    assertFalse(metric.isInteger());
    assertEquals(-260000.0, metric.floatValue(), 0.01);
  }

  @Test
  public void testDecode_stringPlusSignedFloat() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_string;
    gmsg.gstr = new Ganglia_gmetric_string();
    gmsg.gstr.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gstr.fmt = "%s";
    gmsg.gstr.str = "+2.6e5";
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertEquals(Ganglia_msg_formats.gmetric_string,
        ForTesting.get_ganglia_message(msg).id);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("metric_foo", metric.name());
    assertFalse(metric.isInteger());
    assertEquals(260000.0, metric.floatValue(), 0.01);
  }

  @Test
  public void testDecode_stringTripleE() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_string;
    gmsg.gstr = new Ganglia_gmetric_string();
    gmsg.gstr.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gstr.fmt = "%s";
    gmsg.gstr.str = "eee";
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertEquals(Ganglia_msg_formats.gmetric_string,
        ForTesting.get_ganglia_message(msg).id);
    assertNull(msg.getMetricIfNumeric());
  }

  @Test(expected = OutOfMemoryError.class)
  public void testDecode_OutOfMemoryError() throws Exception {
    XdrDecodingStream xdr = mock(XdrDecodingStream.class);
    when(xdr.xdrDecodeInt()).thenThrow(new OutOfMemoryError(""));
    GangliaV31xValueMessage.decode(xdr);
  }

  @Test(expected = IOException.class)
  public void testDecode_IOException() throws Exception {
    XdrDecodingStream xdr = mock(XdrDecodingStream.class);
    when(xdr.xdrDecodeInt()).thenThrow(new IOException(""));
    GangliaV31xValueMessage.decode(xdr);
  }

  @Test(expected = OncRpcException.class)
  public void testDecode_OncRpcException() throws Exception {
    XdrDecodingStream xdr = mock(XdrDecodingStream.class);
    when(xdr.xdrDecodeInt()).thenThrow(new OncRpcException(""));
    GangliaV31xValueMessage.decode(xdr);
  }

  @Test
  public void testGetTags() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_ushort;
    gmsg.gu_short = new Ganglia_gmetric_ushort();
    gmsg.gu_short.metric_id = buildMetricId("foo_id_host", "metric_foo", false);
    gmsg.gu_short.fmt = "%d";
    gmsg.gu_short.us = 177;
    encode(gmsg);
    GangliaV31xValueMessage msg = decode();
    Iterator<Tag> tags = msg.getTags(mock_inet_addr).iterator();
    assertTrue(tags.hasNext());
    GangliaMessage.Tag tag = tags.next();
    assertEquals("host", tag.key);
    assertEquals("foo_id_host", tag.value);
    assertTrue(tags.hasNext());
    tag = tags.next();
    assertEquals("ipaddr", tag.key);
    assertEquals(FOO_IPADDR, tag.value);
    assertFalse(tags.hasNext());
    System.out.println(msg);
  }

  @Test
  public void testGetTags_spoofMetric() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_ushort;
    gmsg.gu_short = new Ganglia_gmetric_ushort();
    gmsg.gu_short.metric_id = buildMetricId("foo_ip:foo_host", "metric_foo", true);
    gmsg.gu_short.fmt = "%d";
    gmsg.gu_short.us = 177;
    encode(gmsg);

    // Does not tag with ip address if it is a spoof message.
    GangliaV31xValueMessage msg = decode();
    Iterator<Tag> tags = msg.getTags(mock_inet_addr).iterator();
    assertTrue(tags.hasNext());
    GangliaMessage.Tag tag = tags.next();
    assertEquals("ipaddr", tag.key);
    assertEquals("foo_ip", tag.value);
    assertTrue(tags.hasNext());
    tag = tags.next();
    assertEquals("host", tag.key);
    assertEquals("foo_host", tag.value);
    assertFalse(tags.hasNext());
    System.out.println(msg);
  }

  @Test
  public void testGetTags_spoofMetricWithoutIp() throws Exception {
    Ganglia_value_msg gmsg = new Ganglia_value_msg();
    gmsg.id = Ganglia_msg_formats.gmetric_ushort;
    gmsg.gu_short = new Ganglia_gmetric_ushort();
    gmsg.gu_short.metric_id = buildMetricId("foo_host", "metric_foo", true);
    gmsg.gu_short.fmt = "%d";
    gmsg.gu_short.us = 177;
    encode(gmsg);

    // Does not tag with ip address if it is a spoof message.
    GangliaV31xValueMessage msg = decode();
    Iterator<Tag> tags = msg.getTags(mock_inet_addr).iterator();
    assertTrue(tags.hasNext());
    GangliaMessage.Tag tag = tags.next();
    assertEquals("host", tag.key);
    assertEquals("foo_host", tag.value);
    assertFalse(tags.hasNext());
    System.out.println(msg);
  }
}
