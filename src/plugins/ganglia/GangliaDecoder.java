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

import java.io.IOException;

import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrBufferDecodingStream;
import org.acplt.oncrpc.XdrDecodingStream;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes binary messages to a Ganglia messages.
 */
class GangliaDecoder extends OneToOneDecoder {

  private static final Logger LOG =
      LoggerFactory.getLogger(GangliaDecoder.class);

  /** Decodes a Ganglia message from {@link ChannelBuffer}. */
  @Override
  protected Object decode(final ChannelHandlerContext ctx, final Channel chan,
      final Object msg) throws Exception {
    if (!(msg instanceof ChannelBuffer)) {
      return msg;
    }
    ChannelBuffer buffer = (ChannelBuffer)msg;
    byte[] buf = buffer.toByteBuffer().array();
    XdrDecodingStream xdr = new XdrBufferDecodingStream(buf);
    return decodeGMetric(xdr, chan);
  }

  /** Decodes a Ganglia message from {@link XdrDecodingStream}. */
  private static GangliaMessage decodeGMetric(final XdrDecodingStream xdr,
      final Channel chan) throws OncRpcException, IOException {
    xdr.beginDecoding();
    int id = xdr.xdrDecodeInt();
    xdr.endDecoding();
    try {
      GangliaMessage msg;
      // Ganglia protocols are not binary compatible, but they can be detected
      // by the message id which is the first 4 bytes of a message.
      if (GangliaV30xMessage.canDecode(id)) {
        msg = GangliaV30xMessage.decode(xdr);
      } else if (GangliaV31xValueMessage.canDecode(id)) {
        msg = GangliaV31xValueMessage.decode(xdr);
      } else if (GangliaV31xMetadataMessage.canDecode(id)) {
        msg = GangliaV31xMetadataMessage.decode(xdr);
      } else {
        msg = GangliaMessage.emptyMessage();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("id = " + id + ", msg = " + msg);
      }
      return msg;
    } catch (OutOfMemoryError e) {
      // A malformed message could cause the OutOfMemoryError.
      LOG.debug(chan.toString() + " OutOfMemoryError while decoding");
    } catch (IOException e) {
      LOG.debug(chan.toString() + " IOException while decoding: " +
          e.getMessage());
    } catch (OncRpcException e) {
      LOG.debug(chan.toString() + " OncRpcException while decoding: " +
          e.getMessage());
    }
    return GangliaMessage.emptyMessage();
  }
}