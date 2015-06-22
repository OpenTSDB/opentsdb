package net.opentsdb.web.jackson;

import net.opentsdb.uid.LabelId;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;

import java.io.IOException;

public class LabelIdJsonKeyDeserializer extends KeyDeserializer {
  private final LabelId.LabelIdDeserializer deserializer;

  public LabelIdJsonKeyDeserializer(final LabelId.LabelIdDeserializer deserializer) {
    this.deserializer = deserializer;
  }

  @Override
  public Object deserializeKey(final String key, final DeserializationContext ctxt)
      throws IOException, JsonProcessingException {
    return deserializer.deserialize(key);
  }
}
