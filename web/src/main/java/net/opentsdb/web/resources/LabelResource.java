package net.opentsdb.web.resources;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.UNPROCESSABLE_ENTITY;

import net.opentsdb.core.LabelClient;
import net.opentsdb.uid.LabelType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A Resource that accepts POST requests whose body contains a JSON containing a ID.
 */
public final class LabelResource extends Resource {
  private static final Logger LOG = LoggerFactory.getLogger(LabelResource.class);

  private final LabelClient labelClient;
  private final ObjectMapper objectMapper;

  public LabelResource(final LabelClient labelClient, final ObjectMapper objectMapper) {
    this.labelClient = checkNotNull(labelClient);
    this.objectMapper = checkNotNull(objectMapper);
  }

  @Override
  protected FullHttpResponse doPost(FullHttpRequest req) {
    try {
      final JsonNode rootNode = objectMapper.readTree(new ByteBufInputStream(req.content()));

      labelClient.assignUid(LabelType.fromValue(rootNode.get("type").asText()),
          rootNode.get("name").asText());

      return response(CREATED);
    } catch (JsonProcessingException e) {
      LOG.info("Malformed JSON while adding new label", e);
      return response(UNPROCESSABLE_ENTITY);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }
}
