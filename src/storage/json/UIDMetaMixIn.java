package net.opentsdb.storage.json;

import java.util.Map;

import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.JSON;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY)
@JsonPropertyOrder({"type", "displayName", "description", "notes", "created", "custom"})
abstract class UIDMetaMixIn {
  UIDMetaMixIn(@JacksonInject final byte[] uid,
               @JsonProperty("type") final UniqueIdType type,
               @JacksonInject final String name,
               @JsonProperty("displayName") final String display_name,
               @JsonProperty("description") final String description,
               @JsonProperty("notes") final String notes,
               @JsonProperty("custom") final Map<String, String> custom,
               @JsonProperty("created") final long created) {
  }

  @JsonIgnore abstract byte[] getUID();
  @JsonIgnore abstract String getName();

  /** The type of UID this metadata represents */
  @JsonDeserialize(using = JSON.UniqueIdTypeDeserializer.class)
  @JsonProperty("type") abstract UniqueIdType getType();

  @JsonProperty("displayName") abstract String getDisplayName();
  @JsonProperty("description") abstract String getDescription();
  @JsonProperty("notes") abstract String getNotes();
  @JsonProperty("custom") abstract Map<String, String> getCustom();
  @JsonProperty("created") abstract long getCreated();
}
