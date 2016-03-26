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

import static org.jboss.netty.channel.Channels.pipeline;

import java.util.concurrent.ThreadFactory;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import net.opentsdb.core.TSDB;

/**
 * Creates a newly configured {@link ChannelPipeline} for a new channel.
 * This class is supposed to be a singleton.
 */
public final class PipelineFactory implements ChannelPipelineFactory {

  // Those are entirely stateless and thus a single instance is needed.
  private static final StringEncoder ENCODER = new StringEncoder();
  private static final WordSplitter DECODER = new WordSplitter();

  // Those are sharable but maintain some state, so a single instance per
  // PipelineFactory is needed.
  private final ConnectionManager connmgr;
  private final DetectHttpOrRpc HTTP_OR_RPC = new DetectHttpOrRpc();
  private final Timer timer;
  private final ChannelHandler timeoutHandler;

  /** Stateless handler for RPCs. */
  private final RpcHandler rpchandler;
  
  /** The TSDB to which we belong */ 
  private final TSDB tsdb;
  
  /** The server side socket timeout. **/
  private final int socketTimeout;
  
  /**
   * Constructor that initializes the RPC router and loads HTTP formatter 
   * plugins. This constructor creates its own {@link RpcManager}.
   * @param tsdb The TSDB to use.
   * @throws RuntimeException if there is an issue loading plugins
   * @throws Exception if the HttpQuery handler is unable to load 
   * serializers
   */
  public PipelineFactory(final TSDB tsdb) {
    this(tsdb, RpcManager.instance(tsdb), tsdb.getConfig().getInt("tsd.core.connections.limit"));
  }

  /**
   * Constructor that initializes the RPC router and loads HTTP formatter 
   * plugins using an already-configured {@link RpcManager}.
   * @param tsdb The TSDB to use.
   * @param manager instance of a ready-to-use {@link RpcManager}.
   * @throws RuntimeException if there is an issue loading plugins
   * @throws Exception if the HttpQuery handler is unable to load 
   * serializers
   */
  public PipelineFactory(final TSDB tsdb, final RpcManager manager, final int connectionsLimit) {
    this.tsdb = tsdb;
    this.socketTimeout = tsdb.getConfig().getInt("tsd.core.socket.timeout");
    timer = tsdb.getTimer();
    this.timeoutHandler = new IdleStateHandler(timer, 0, 0, this.socketTimeout);
    this.rpchandler = new RpcHandler(tsdb, manager);
    this.connmgr = new ConnectionManager(connectionsLimit);
    try {
      HttpQuery.initializeSerializerMaps(tsdb);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize formatter plugins", e);
    }
  }

  @Override
  public ChannelPipeline getPipeline() throws Exception {
   final ChannelPipeline pipeline = pipeline();

    pipeline.addLast("connmgr", connmgr);
    pipeline.addLast("detect", HTTP_OR_RPC);
    return pipeline;
  }

  /**
   * Dynamically changes the {@link ChannelPipeline} based on the request.
   * If a request uses HTTP, then this changes the pipeline to process HTTP.
   * Otherwise, the pipeline is changed to processes an RPC.
   */
  final class DetectHttpOrRpc extends FrameDecoder {

    @Override
    protected Object decode(final ChannelHandlerContext ctx,
                            final Channel chan,
                            final ChannelBuffer buffer) throws Exception {
      if (buffer.readableBytes() < 1) {  // Yes sometimes we can be called
        return null;                     // with an empty buffer...
      }

      final int firstbyte = buffer.getUnsignedByte(buffer.readerIndex());
      final ChannelPipeline pipeline = ctx.getPipeline();
      // None of the commands in the RPC protocol start with a capital ASCII
      // letter for the time being, and all HTTP commands do (GET, POST, etc.)
      // so use this as a cheap way to differentiate the two.
      if ('A' <= firstbyte && firstbyte <= 'Z') {
        pipeline.addLast("decoder", new HttpRequestDecoder());
        if (tsdb.getConfig().enable_chunked_requests()) {
          pipeline.addLast("aggregator", new HttpChunkAggregator(
              tsdb.getConfig().max_chunked_requests()));
        }
        // allow client to encode the payload (ie : with gziped json)
        pipeline.addLast("inflater", new HttpContentDecompressor());
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("deflater", new HttpContentCompressor());
      } else {
        pipeline.addLast("framer", new LineBasedFrameDecoder(1024));
        pipeline.addLast("encoder", ENCODER);
        pipeline.addLast("decoder", DECODER);
      }

      pipeline.addLast("timeout", timeoutHandler);
      pipeline.remove(this);
      pipeline.addLast("handler", rpchandler);

      // Forward the buffer to the next handler.
      return buffer.readBytes(buffer.readableBytes());
    }

  }
  
}
