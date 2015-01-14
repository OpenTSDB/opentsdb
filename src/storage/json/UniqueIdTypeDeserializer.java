package net.opentsdb.storage.json;

import java.io.IOException;

import net.opentsdb.uid.UniqueIdType;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

/**
 * Helper class for deserializing UID type enum from human readable strings
 */
public class UniqueIdTypeDeserializer
        extends JsonDeserializer<UniqueIdType> {

  @Override
  public UniqueIdType deserialize(final JsonParser parser,
                                  final DeserializationContext context)
          throws IOException {
    return UniqueIdType.fromString(parser.getValueAsString());
  }
}
