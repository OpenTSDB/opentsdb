package net.opentsdb.storage.json;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY)
@JsonPropertyOrder({"name", "description", "notes", "strictMatch", "created", "enabled", "storeFailures"})
abstract class TreeMixIn {
  @JsonIgnore abstract Map<String, String> getNotMatched();
  @JsonIgnore abstract Map<String, String> getCollisions();
}
