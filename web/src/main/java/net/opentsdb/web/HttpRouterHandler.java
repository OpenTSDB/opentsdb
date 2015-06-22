package net.opentsdb.web;

import static com.google.common.base.Preconditions.checkNotNull;

import net.opentsdb.web.resources.Resource;

import autovalue.shaded.com.google.common.common.base.CharMatcher;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.internal.TypeParameterMatcher;

import java.util.Map;

/**
 * A Netty handler that delegates incoming HTTP requests to other handlers based on the requests
 * path. Messages that aren't HTTP requests are ignored.
 */
@ChannelHandler.Sharable
public class HttpRouterHandler extends ChannelInboundHandlerAdapter {
  private final TypeParameterMatcher matcher;

  private final Map<String, Resource> resources;
  private final Resource defaultResource;

  private final CharMatcher routeNormalizer = CharMatcher.is('/');

  /**
   * Create a new instance with the given map of resources and default resource. The map should be
   * keyed on the path they wish to be exposed on with leading and trailing '/' removed. If no
   * resource matches a request then the provided default resource will be used.
   *
   * @param resources A map of resource keyed on the path they are to be exposed on
   * @param defaultResource The default resource if no other resource matches
   */
  public HttpRouterHandler(final Map<String, Resource> resources,
                           final Resource defaultResource) {
    this.matcher = TypeParameterMatcher.get(FullHttpRequest.class);
    this.resources = checkNotNull(resources);
    this.defaultResource = checkNotNull(defaultResource);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    // Check if this is a http request and if it is we pick a handler based on
    // the request parameters and add it to the pipeline.
    if (matcher.match(msg)) {
      final FullHttpRequest request = (FullHttpRequest) msg;

      // It is important that we add the new handler before the exception handler
      // so that the exception handler is not short-circuited.
      ctx.pipeline().addBefore("exception", null, resourceForRequest(request));
    }

    // Schedule the next inbound channel handler to be notified that it should
    // Try to handle this message
    ctx.fireChannelRead(msg);
  }

  private Resource resourceForRequest(final HttpRequest request) {
    final QueryStringDecoder query = new QueryStringDecoder(request.uri(), HttpConstants.CHARSET,
        true, 1);
    final String route = routeNormalizer.trimFrom(query.path());
    final Resource resource = resources.get(route);
    return resource == null ? defaultResource : resource;
  }
}
