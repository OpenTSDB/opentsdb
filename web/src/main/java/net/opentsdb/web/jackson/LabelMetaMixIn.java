package net.opentsdb.web.jackson;

import net.opentsdb.uid.UniqueIdType;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY)
abstract class LabelMetaMixIn {
  LabelMetaMixIn(@JacksonInject final byte[] uid,
                 @JsonProperty("type") final UniqueIdType type,
                 @JacksonInject final String name,
                 @JsonProperty("displayName") final String displayName,
                 @JsonProperty("description") final String description,
                 @JsonProperty("notes") final String notes,
                 @JsonProperty("custom") final Map<String, String> custom,
                 @JsonProperty("created") final long created) {
  }

  @JsonIgnore
  abstract byte[] getUID();

  @JsonIgnore
  abstract String getName();

  /** The type of UID this metadata represents */
  @JsonDeserialize(using = UniqueIdTypeDeserializer.class)
  @JsonProperty("type")
  abstract UniqueIdType getType();

  @JsonProperty("displayName")
  abstract String getDisplayName();

  @JsonProperty("description")
  abstract String getDescription();

  @JsonProperty("notes")
  abstract String getNotes();

  @JsonProperty("custom")
  abstract Map<String, String> getCustom();

  @JsonProperty("created")
  abstract long getCreated();
}
