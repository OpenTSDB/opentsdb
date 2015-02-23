// This file is part of OpenTSDB.
// Copyright (C) 2012  The OpenTSDB Authors.
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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;

/**
 * Decodes telnet-style frames delimited by new-lines.
 * <p>
 * Both "\n" and "\r\n" are handled.
 * <p>
 * This decoder is stateful and is thus <strong>NOT</strong> shareable.
 */
final class LineBasedFrameDecoder extends FrameDecoder {

  /** Maximum length of a frame we're willing to decode.  */
  private final int max_length;
  /** True if we're discarding input because we're already over max_length.  */
  private boolean discarding;

  /**
   * Creates a new decoder.
   * @param max_length Maximum length of a frame we're willing to decode.
   * If a frame is longer than that, a {@link TooLongFrameException} will
   * be fired on the channel causing it.
   */
  public LineBasedFrameDecoder(final int max_length) {
    this.max_length = max_length;
  }

  @Override
  protected Object decode(final ChannelHandlerContext ctx,
                          final Channel channel,
                          final ChannelBuffer buffer) throws Exception {
    final int eol = findEndOfLine(buffer);
    if (eol != -1) {
      final ChannelBuffer frame;
      final int length = eol - buffer.readerIndex();
      assert length >= 0: "WTF?  length=" + length;
      if (discarding) {
        frame = null;
        buffer.skipBytes(length);
      } else {
        frame = buffer.readBytes(length);
      }
      final byte delim = buffer.readByte();
      if (delim == '\r') {
        buffer.skipBytes(1);  // Skip the \n.
      }
      return frame;
    }

    final int buffered = buffer.readableBytes();
    if (!discarding && buffered > max_length) {
      discarding = true;
      Channels.fireExceptionCaught(ctx.getChannel(),
        new TooLongFrameException("Frame length exceeds " + max_length + " ("
                                  + buffered + " bytes buffered already)"));
    }
    if (discarding) {
      buffer.skipBytes(buffer.readableBytes());
    }
    return null;
  }

  /**
   * Returns the index in the buffer of the end of line found.
   * Returns -1 if no end of line was found in the buffer.
   */
  private static int findEndOfLine(final ChannelBuffer buffer) {
    final int n = buffer.writerIndex();
    for (int i = buffer.readerIndex(); i < n; i ++) {
      final byte b = buffer.getByte(i);
      if (b == '\n') {
        return i;
      } else if (b == '\r' && i < n - 1 && buffer.getByte(i + 1) == '\n') {
        return i;  // \r\n
      }
    }
    return -1;  // Not found.
  }

}
