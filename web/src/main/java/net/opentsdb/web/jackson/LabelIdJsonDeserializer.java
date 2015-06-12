package net.opentsdb.web.jackson;

import net.opentsdb.uid.LabelId;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import javax.annotation.Nonnull;

public class LabelIdJsonDeserializer extends JsonDeserializer<LabelId> {
  private final LabelId.LabelIdDeserializer deserializer;

  LabelIdJsonDeserializer(@Nonnull LabelId.LabelIdDeserializer deserializer) {
    this.deserializer = deserializer;
  }

  @Override
  public LabelId deserialize(final JsonParser p, final DeserializationContext ctxt)
      throws IOException, JsonProcessingException {
    return deserializer.deserialize(p.getValueAsString());
  }

  @Override
  public Class<?> handledType() {
    return LabelId.class;
  }
}
