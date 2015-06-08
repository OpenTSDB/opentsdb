
package net.opentsdb.web;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static net.opentsdb.web.HttpConstants.HTTP_VERSION;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class ExceptionHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(ExceptionHandler.class);

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest request) throws Exception {
    ctx.writeAndFlush(internalErrorResponse(request, null))
        .addListener(ChannelFutureListener.CLOSE);
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
    ctx.writeAndFlush(internalErrorResponse(null, cause))
        .addListener(ChannelFutureListener.CLOSE);
  }

  private FullHttpResponse internalErrorResponse(final HttpRequest request,
                                                 final Throwable cause) {
    LOG.error("Encountered an exception while processing {}", request, cause);
    return new DefaultFullHttpResponse(HTTP_VERSION, INTERNAL_SERVER_ERROR);
  }
}
