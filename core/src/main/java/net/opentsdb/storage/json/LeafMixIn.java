package net.opentsdb.storage.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"displayName", "tsuid"})
abstract class LeafMixIn {
  LeafMixIn(@JsonProperty("displayName") final String display_name,
            @JsonProperty("tsuid") final String tsuid) {
  }

  @JsonProperty("displayName") abstract String getDisplayName();
  @JsonProperty("tsuid") abstract String getTsuid();
}
