package net.opentsdb.storage.json;

import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY)
@JsonPropertyOrder({"path", "displayName"})
abstract class BranchMixIn {
  BranchMixIn(@JsonProperty("path") final TreeMap<Integer, String> path,
              @JsonProperty("displayName") final String display_name) {
  }

  @JsonProperty("path") abstract Map<Integer, String> getPath();
  @JsonProperty("displayName") abstract String getDisplayName();
}
