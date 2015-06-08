
package net.opentsdb.web.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;

/**
 * A resource that exposes all metrics collected about the current process for
 * administration purposes.
 */
public class MetricsResource extends Resource {
  private final ObjectMapper objectMapper;
  private final MetricRegistry metricRegistry;

  public MetricsResource(final ObjectMapper objectMapper,
                         final MetricRegistry metricRegistry) {
    this.objectMapper = objectMapper;
    this.metricRegistry = metricRegistry;
  }

  @Override
  protected FullHttpResponse doGet(final FullHttpRequest request) {
    try {
      byte[] bytes = objectMapper.writeValueAsBytes(metricRegistry);
      return response(OK, Unpooled.wrappedBuffer(bytes));
    } catch (JsonProcessingException e) {
      // If the object mapper is configured properly to be able to serialize a
      // metric registry then this should never happen?
      throw new AssertionError(e);
    }
  }
}
