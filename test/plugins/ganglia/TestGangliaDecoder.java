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
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;

import info.ganglia.gmetric4j.xdr.v30x.Ganglia_message_formats;
import info.ganglia.gmetric4j.xdr.v30x.Ganglia_spoof_header;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_msg_formats;

import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrDecodingStream;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.plugins.ganglia.GangliaMessage.Metric;


/** Tests {@link GangliaDecoder}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ GangliaV30xMessage.class, GangliaMessage.class })
public class TestGangliaDecoder {

  // Ganglia message: id = 1, value = 3*256+7, name = cpu_num.
  private static final byte[] V30X_CPU_NUM_MSG = "\0\0\0\1\0\0\3\7".getBytes();
  // A v31x ushort value message. The id (\0\0\0\3) will be replaced later.
  // The value is 17 (21 in octal).
  private static final byte[] V31X_FOO_MSG =
      ("\0\0\0\3\0\0\0\13foo_id_host\0\0\0\0\12metric_foo" +
       "\0\0\0\0\0\0\0\0\0\2\45d\0\0\0\0\0\21").getBytes();
  // A partial v31x metadata message. Just an id. It will be replaced later.
  private static final byte[] V31X_METADATA_MSG = "\0\0\0\3".getBytes();
  // Wrong id.
  private static final byte[] WRONG_ID_MSG = "\0\0\0\144".getBytes();

  static {
    // Assigns v31x message ids. It is needed because a byte bigger than 127
    // is converted to two bytes not a single byte by String.getByte.
    // Assigns gmetric_ushort to the V31x message id.
    V31X_FOO_MSG[3] = (byte)Ganglia_msg_formats.gmetric_ushort;
    // Assigns gmetadata_full to the V31x metatdata message id.
    V31X_METADATA_MSG[3] = (byte)Ganglia_msg_formats.gmetadata_full;
  }

  @Mock private ChannelBuffer mock_buffer;
  @Mock private ChannelHandlerContext mock_ctx;
  @Mock private Channel mock_channel;
  private ByteBuffer mock_byte_buffer;
  private GangliaDecoder decoder;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    mock_byte_buffer = ByteBuffer.wrap(V30X_CPU_NUM_MSG);
    when(mock_buffer.toByteBuffer()).thenReturn(mock_byte_buffer);
    decoder = new GangliaDecoder();
  }

  @Test
  public void testDecode() throws Exception {
    // Decodes a cpu_num metric message.
    Object obj = decoder.decode(mock_ctx, mock_channel, mock_buffer);
    assertTrue(obj instanceof GangliaV30xMessage);
    GangliaV30xMessage msg = (GangliaV30xMessage)obj;
    assertFalse(msg.hasError());
    assertEquals(Ganglia_message_formats.metric_cpu_num,
        GangliaV30xMessage.ForTesting.get_ganglia_message(msg).id);
    Ganglia_spoof_header spheader = msg.getSpoofHeaderIfSpoofMetric();
    assertNull(spheader);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("cpu_num", metric.name());
    assertTrue(metric.isInteger());
    assertEquals(3 * 256 + 7, metric.longValue());
  }

  @Test
  public void testDecode_GangliaV31xValueMessage() throws Exception {
    // Decodes a GangliaV31xValueMessage message.
    mock_byte_buffer = ByteBuffer.wrap(V31X_FOO_MSG);
    when(mock_buffer.toByteBuffer()).thenReturn(mock_byte_buffer);
    Object obj = decoder.decode(mock_ctx, mock_channel, mock_buffer);
    assertTrue(obj instanceof GangliaV31xValueMessage);
    GangliaV31xValueMessage msg = (GangliaV31xValueMessage)obj;
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertEquals(Ganglia_msg_formats.gmetric_ushort,
        GangliaV31xValueMessage.ForTesting.get_ganglia_message(msg).id);
    Metric metric = msg.getMetricIfNumeric();
    assertNotNull(metric);
    assertEquals("metric_foo", metric.name());
    assertTrue(metric.isInteger());
    assertEquals(17, metric.longValue());
  }

  @Test
  public void testDecode_GangliaV31xMetadataMessage() throws Exception {
    // Decodes a GangliaV31xMetadataMessage message.
    mock_byte_buffer = ByteBuffer.wrap(V31X_METADATA_MSG);
    when(mock_buffer.toByteBuffer()).thenReturn(mock_byte_buffer);
    Object obj = decoder.decode(mock_ctx, mock_channel, mock_buffer);
    assertTrue(obj instanceof GangliaV31xMetadataMessage);
    GangliaV31xMetadataMessage msg = (GangliaV31xMetadataMessage)obj;
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
  }

  @Test
  public void testDecode_wrongId() throws Exception {
    // Decodes a corrupted message.
    mock_byte_buffer = ByteBuffer.wrap(WRONG_ID_MSG);
    when(mock_buffer.toByteBuffer()).thenReturn(mock_byte_buffer);
    Object obj = decoder.decode(mock_ctx, mock_channel, mock_buffer);
    assertFalse(obj instanceof GangliaV31xMetadataMessage);
    assertTrue(obj instanceof GangliaMessage.EmptyMessage);
    GangliaMessage.EmptyMessage msg = (GangliaMessage.EmptyMessage)obj;
    assertSame(GangliaMessage.ProtocolVersion.V30x, msg.protocolVersion());
    assertTrue(msg.hasError());
  }

  @Test
  public void testDecode_wrongMessage() throws Exception {
    // Does not decode unless data is ChannelBuffer.
    Object src_obj = new Object();
    Object obj = decoder.decode(mock_ctx, mock_channel, src_obj);
    assertSame(src_obj, obj);
  }

  @Test
  public void testDecode_OutOfMemoryError() throws Exception {
    // Produces an empty Ganglia metric in case of OutOfMemoryError.
    mockStatic(GangliaV30xMessage.class);
    mockStatic(GangliaMessage.class);
    when(GangliaV30xMessage.decode(any(XdrDecodingStream.class)))
        .thenThrow(new OutOfMemoryError(""));
    when(GangliaMessage.emptyMessage()).thenCallRealMethod();
    Object obj = decoder.decode(mock_ctx, mock_channel, mock_buffer);
    assertTrue(obj instanceof GangliaMessage);
    GangliaMessage msg = (GangliaMessage)obj;
    assertTrue(msg.hasError());
  }

  @Test
  public void testDecode_IOException() throws Exception {
    // Produces an empty Ganglia metric in case of IOException.
    mockStatic(GangliaV30xMessage.class);
    mockStatic(GangliaMessage.class);
    when(GangliaV30xMessage.decode(any(XdrDecodingStream.class)))
        .thenThrow(new IOException(""));
    when(GangliaMessage.emptyMessage()).thenCallRealMethod();
    Object obj = decoder.decode(mock_ctx, mock_channel, mock_buffer);
    assertTrue(obj instanceof GangliaMessage);
    GangliaMessage msg = (GangliaMessage)obj;
    assertTrue(msg.hasError());
  }

  @Test
  public void testDecode_OncRpcException() throws Exception {
    // Produces an empty Ganglia metric in case of OncRpcException.
    mockStatic(GangliaV30xMessage.class);
    mockStatic(GangliaMessage.class);
    when(GangliaV30xMessage.decode(any(XdrDecodingStream.class)))
        .thenThrow(new OncRpcException(""));
    when(GangliaMessage.emptyMessage()).thenCallRealMethod();
    Object obj = decoder.decode(mock_ctx, mock_channel, mock_buffer);
    assertTrue(obj instanceof GangliaMessage);
    GangliaMessage msg = (GangliaMessage)obj;
    assertTrue(msg.hasError());
  }
}
