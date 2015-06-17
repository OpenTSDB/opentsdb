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
  private final int maxContentLength;

  private final HttpRouterHandler routerHandler;
  private final ExceptionHandler exceptionHandler;
  private final CorsConfig corsConfig;

  public HttpServerInitializer(final Map<String, Resource> resources,
                               final Resource defaultResource,
                               final CorsConfig corsConfig,
                               final int maxContentLength) {
    this.routerHandler = new HttpRouterHandler(resources, defaultResource);
    this.exceptionHandler = new ExceptionHandler();
    this.maxContentLength = maxContentLength;
    this.corsConfig = corsConfig;
  }

  @Override
  public void initChannel(SocketChannel ch) {
    ChannelPipeline pipeline = ch.pipeline();

    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast(new HttpObjectAggregator(maxContentLength));
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
