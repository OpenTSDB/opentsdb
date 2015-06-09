package net.opentsdb.web.resources;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.UNPROCESSABLE_ENTITY;

import net.opentsdb.core.DataPointsClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * A resource that accepts post requests whose body contains a JSON array of datapoints.
 */
public final class DatapointsResource extends Resource {
  private static final Logger LOG = LoggerFactory.getLogger(DatapointsResource.class);

  private final DataPointsClient datapointsClient;
  private final ObjectMapper objectMapper;

  private final IntegralPredicate integralPredicate;
  private final FloatPredicate floatPredicate;
  private final DoublePredicate doublePredicate;

  /**
   * Create a new instance that adds datapoints using the provided {@link DataPointsClient} and uses
   * the provided {@link ObjectMapper} to deserialize the JSON that this resource consumes.
   */
  public DatapointsResource(final DataPointsClient datapointsClient,
                            final ObjectMapper objectMapper) {
    this.datapointsClient = checkNotNull(datapointsClient);
    this.objectMapper = checkNotNull(objectMapper);

    this.integralPredicate = new IntegralPredicate();
    this.floatPredicate = new FloatPredicate();
    this.doublePredicate = new DoublePredicate();
  }

  @Override
  protected FullHttpResponse doPost(FullHttpRequest req) {
    try {
      final JsonNode rootNode = objectMapper.readTree(new ByteBufInputStream(req.content()));

      if (rootNode.getNodeType() != JsonNodeType.ARRAY) {
        return response(UNPROCESSABLE_ENTITY);
      }

      for (final JsonNode datapointNode : rootNode) {
        readDatapoint(datapointNode);
      }

      return response(ACCEPTED);
    } catch (JsonProcessingException e) {
      LOG.info("Malformed JSON while adding datapoint", e);
      return response(UNPROCESSABLE_ENTITY);
    } catch (IOException e) {
      // We are just reading from memory so nothing should throw an IOException.
      throw new AssertionError(e);
    }
  }

  /**
   * Extract a single datapoint out of the provided {@link JsonNode}.
   *
   * @throws JsonMappingException if the provided JSON can not be parsed into a datapoint.
   */
  private void readDatapoint(final JsonNode datapointNode) throws JsonMappingException {
    final String metric = datapointNode.get("metric").textValue();
    final ImmutableMap<String, String> tags = readTags(datapointNode.get("tags"));
    final long timestamp = datapointNode.get("timestamp").asLong();

    JsonNode value;

    // Look for one of the supported JSON fields for a value and use the first
    // one when adding the datapoint.
    if ((value = checkField(datapointNode, "longValue", integralPredicate)) != null) {
      datapointsClient.addPoint(metric, timestamp, value.longValue(), tags);
    } else if ((value = checkField(datapointNode, "floatValue", floatPredicate)) != null) {
      datapointsClient.addPoint(metric, timestamp, value.floatValue(), tags);
    } else if ((value = checkField(datapointNode, "doubleValue", doublePredicate)) != null) {
      datapointsClient.addPoint(metric, timestamp, value.doubleValue(), tags);
    } else {
      throw new JsonMappingException("JSON contains no recognized value fields");
    }
  }

  /**
   * Get the field with the provided name if it passes the provided {@link Predicate}.
   *
   * @param json The json structure to look for the field in
   * @param field The name of the field to look for
   * @param predicate The predicate to make sure the field passes
   * @return The {@link JsonNode} that represents the field if it exists and the value passes the
   * provided predicate, otherwise null.
   * @throws JsonMappingException if there is a field with the provided name but the field does not
   * pass the predicate.
   */
  private JsonNode checkField(final JsonNode json,
                              final String field,
                              final Predicate<JsonNode> predicate) throws JsonMappingException {
    final JsonNode value = json.get(field);

    if (value == null) {
      return null;
    }

    if (!predicate.apply(value)) {
      throw new JsonMappingException("JSON contains '" + field +
                                     "' field but its node type is " + value.getNodeType());
    }

    return value;
  }

  /**
   * Extract the tags out of the provided {@link JsonNode}.
   */
  private ImmutableMap<String, String> readTags(final JsonNode tagsNode) throws JsonMappingException {
    if (!tagsNode.isObject()) {
      throw new JsonMappingException("The JSON object for the tags is a " + tagsNode.getNodeType());
    }

    ImmutableMap.Builder<String, String> tags = ImmutableMap.builder();
    Iterator<Map.Entry<String, JsonNode>> tagsIterator = tagsNode.fields();
    while (tagsIterator.hasNext()) {
      Map.Entry<String, JsonNode> tag = tagsIterator.next();
      tags.put(tag.getKey(), tag.getValue().textValue());
    }

    return tags.build();
  }

  /**
   * Predicate to check if a {@link JsonNode} contains an integral value.
   */
  private static class IntegralPredicate implements Predicate<JsonNode> {
    @Override
    public boolean apply(final JsonNode value) {
      return value.isIntegralNumber();
    }
  }

  /**
   * Predicate to check if a {@link JsonNode} contains a float value.
   */
  private static class FloatPredicate implements Predicate<JsonNode> {
    @Override
    public boolean apply(final JsonNode value) {
      return value.isFloat();
    }
  }

  /**
   * Predicate to check if a {@link JsonNode} contains a double value.
   */
  private static class DoublePredicate implements Predicate<JsonNode> {
    @Override
    public boolean apply(final JsonNode value) {
      return value.isDouble();
    }
  }
}