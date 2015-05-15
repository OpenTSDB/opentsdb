package net.opentsdb.web.resources;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static net.opentsdb.web.HttpConstants.HTTP_VERSION;

@ChannelHandler.Sharable
public class Resource extends SimpleChannelInboundHandler<FullHttpRequest> {
  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest request) {
    ctx.writeAndFlush(handle(request))
        .addListener(ChannelFutureListener.CLOSE);
  }

  public FullHttpResponse handle(final FullHttpRequest request) {
    if (request.method().equals(HttpMethod.GET))
      return doGet(request);

    if (request.method().equals(HttpMethod.POST))
      return doPost(request);

    return response(METHOD_NOT_ALLOWED);
  }

  protected FullHttpResponse doGet(final FullHttpRequest request) {
    return response(METHOD_NOT_ALLOWED);
  }

  protected FullHttpResponse doPost(final FullHttpRequest request) {
    return response(METHOD_NOT_ALLOWED);
  }

  protected FullHttpResponse response(final HttpResponseStatus statusCode) {
    return new DefaultFullHttpResponse(HTTP_VERSION, statusCode);
  }

  protected FullHttpResponse response(final HttpResponseStatus statusCode,
                                                               final ByteBuf content) {
    return new DefaultFullHttpResponse(HTTP_VERSION, statusCode, content);
  }
}
