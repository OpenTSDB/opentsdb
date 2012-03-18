// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import net.opentsdb.core.Tags;

/**
 * Splits a ChannelBuffer in multiple space separated words.
 */
final class WordSplitter extends OneToOneDecoder {

  private static final Charset CHARSET = Charset.forName("ISO-8859-1");

  /** Constructor. */
  public WordSplitter() {
  }

  @Override
  protected Object decode(final ChannelHandlerContext ctx,
                          final Channel channel,
                          final Object msg) throws Exception {
    return Tags.splitString(((ChannelBuffer) msg).toString(CHARSET), ' ');
  }

}
