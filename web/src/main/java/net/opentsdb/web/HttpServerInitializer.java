
package net.opentsdb.web;

import net.opentsdb.web.resources.Resource;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.util.Map;

public class HttpServerInitializer extends ChannelInitializer<SocketChannel> {
  private final int max_content_length;

  private final HttpRouterHandler routerHandler;
  private final ExceptionHandler exceptionHandler;
  private final CorsConfig corsConfig;

  public HttpServerInitializer(final Map<String, Resource> resources,
                               final Resource defaultResource,
                               final CorsConfig corsConfig,
                               final int max_content_length) {
    this.routerHandler = new HttpRouterHandler(resources, defaultResource);
    this.exceptionHandler = new ExceptionHandler();
    this.max_content_length = max_content_length;
    this.corsConfig = corsConfig;
  }

  @Override
  public void initChannel(SocketChannel ch) {
    ChannelPipeline pipeline = ch.pipeline();

    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast(new HttpObjectAggregator(max_content_length));
    pipeline.addLast(new HttpContentDecompressor());
    pipeline.addLast(new HttpContentCompressor());
    pipeline.addLast(new ChunkedWriteHandler());
    pipeline.addLast(new CorsHandler(corsConfig));
    pipeline.addLast(routerHandler);

    // This needs to be last since exceptions propagate towards the last inbound
    // handler in the pipeline.
    pipeline.addLast("exception", exceptionHandler);
  }
}
