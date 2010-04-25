// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import static org.jboss.netty.channel.Channels.pipeline;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringEncoder;

import net.opentsdb.core.TSDB;

/**
 * Creates a newly configured {@link ChannelPipeline} for a new channel.
 * This class is supposed to be a singleton.
 */
public final class PipelineFactory implements ChannelPipelineFactory {

  // Those are entirely stateless and thus a single instance is needed.
  private static final ChannelBuffer[] DELIMITERS = Delimiters.lineDelimiter();
  private static final StringEncoder ENCODER = new StringEncoder();
  private static final WordSplitter DECODER = new WordSplitter();

  // Those are sharable but maintain some state, so a single instance per
  // PipelineFactory is needed.
  private final ConnectionManager connmgr = new ConnectionManager();

  /** The TSDB to use. */
  private final TSDB tsdb;
  /** Stateless handler to use for simple text RPCs (not HTTP RPCs). */
  private final TextRpc textrpc;

  /**
   * Constructor.
   * @param tsdb The TSDB to use.
   */
  public PipelineFactory(final TSDB tsdb) {
    this.tsdb = tsdb;
    this.textrpc = new TextRpc(tsdb);
  }

  @Override
  public ChannelPipeline getPipeline() throws Exception {
   final ChannelPipeline pipeline = pipeline();

    pipeline.addLast("connmgr", connmgr);
    pipeline.addLast("framer",
                     new DelimiterBasedFrameDecoder(1024, DELIMITERS));
    pipeline.addLast("encoder", ENCODER);
    pipeline.addLast("decoder", DECODER);
    pipeline.addLast("handler", textrpc);

    return pipeline;
  }

}
