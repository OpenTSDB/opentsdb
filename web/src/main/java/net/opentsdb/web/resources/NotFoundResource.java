
package net.opentsdb.web.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A resource that is useful as a default resource in {@link
 * net.opentsdb.web.HttpRouterHandler}. It returns the status code 404 for all
 * requests it receives.
 */
public class NotFoundResource extends Resource {
  private static final Logger LOG = LoggerFactory.getLogger(NotFoundResource.class);

  @Override
  public FullHttpResponse handle(final FullHttpRequest request) {
    LOG.info("No route found for path {}", request.uri());
    return response(NOT_FOUND);
  }
}
