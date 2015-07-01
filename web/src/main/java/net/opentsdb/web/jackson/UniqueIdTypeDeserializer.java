package net.opentsdb.web.jackson;

import net.opentsdb.uid.LabelType;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

/**
 * Helper class for deserializing UID type enum from human readable strings.
 */
public class UniqueIdTypeDeserializer extends JsonDeserializer<LabelType> {
  @Override
  public LabelType deserialize(final JsonParser parser,
                                  final DeserializationContext context)
      throws IOException {
    return LabelType.fromValue(parser.getValueAsString());
  }
}
