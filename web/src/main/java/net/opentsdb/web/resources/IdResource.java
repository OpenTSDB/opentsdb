package net.opentsdb.web.resources;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.UNPROCESSABLE_ENTITY;

import net.opentsdb.core.IdClient;
import net.opentsdb.uid.IdType;

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
public final class IdResource extends Resource {
  private static final Logger LOG = LoggerFactory.getLogger(IdResource.class);

  private final IdClient idClient;
  private final ObjectMapper objectMapper;

  public IdResource(final IdClient idClient, final ObjectMapper objectMapper) {
    this.idClient = checkNotNull(idClient);
    this.objectMapper = checkNotNull(objectMapper);
  }


  @Override
  protected FullHttpResponse doPost(FullHttpRequest req) {
    try {

      final JsonNode rootNode = objectMapper.readTree(new ByteBufInputStream(req.content()));

      idClient.assignUid(IdType.fromValue(rootNode.get("type").asText()),
          rootNode.get("name").asText());

      return response(CREATED);
    } catch (JsonProcessingException e) {
      return response(UNPROCESSABLE_ENTITY);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }
}
