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

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

import net.opentsdb.core.TSDB;


/**
 * Pipeline to process messages in the Ganglia v30x and v31x protocol.
 */
class GangliaPipelineFactory implements ChannelPipelineFactory {

  /** Decodes binary messages into a Ganglia v30x or v31x message. */
  private final GangliaDecoder decoder;
  /** Stateless Ganglia message handler. */
  private final GangliaHandler handler;

  GangliaPipelineFactory(final GangliaConfig config, final TSDB tsdb) {
    decoder = new GangliaDecoder();
    handler = new GangliaHandler(config, tsdb);
  }

  @Override
  public ChannelPipeline getPipeline() throws Exception {
    // Receives, decodes and handles Ganglia messages.
    return Channels.pipeline(decoder, handler);
  }
}
