package net.opentsdb.web.jackson;

import net.opentsdb.meta.LabelMeta;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.UniqueIdType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

abstract class LabelMetaMixIn {
  @JsonCreator
  static LabelMeta create(@JsonProperty("identifier") final LabelId identifier,
                          @JsonProperty("type") final UniqueIdType type,
                          @JsonProperty("name") final String name,
                          @JsonProperty("description") final String description,
                          @JsonProperty("created") final long created) {
    return LabelMeta.create(identifier, type, name, description, created);
  }

  @JsonProperty
  abstract LabelId identifier();

  /** The type of UID this metadata represents. */
  @JsonProperty
  @JsonDeserialize(using = UniqueIdTypeDeserializer.class)
  abstract UniqueIdType type();

  @JsonProperty
  abstract String name();

  @JsonProperty
  abstract String description();

  @JsonProperty
  abstract long created();
}
