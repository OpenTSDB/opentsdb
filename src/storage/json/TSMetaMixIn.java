package net.opentsdb.storage.json;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY)
@JsonPropertyOrder({"tsuid", "displayName", "description", "notes",
        "created", "custom", "units", "dataType", "retention", "max", "min"})
abstract class TSMetaMixIn {
  TSMetaMixIn(@JsonProperty("tsuid") String tsuid,
              @JsonProperty("displayName") String displayName,
              @JsonProperty("description") String description,
              @JsonProperty("notes") String notes,
              @JsonProperty("created") long created,
              @JsonProperty("custom") Map<String, String> custom,
              @JsonProperty("units") String units,
              @JsonProperty("dataType") String dataType,
              @JsonProperty("retention") int retention,
              @JsonProperty("max") double max,
              @JsonProperty("min") double min) {
  }

  @JsonProperty("tsuid") abstract String getTSUID();
  @JsonProperty("displayName") abstract String getDisplayName();
  @JsonProperty("description") abstract String getDescription();
  @JsonProperty("notes") abstract String getNotes();
  @JsonProperty("created") abstract long getCreated();
  @JsonProperty("custom") abstract Map<String, String> getCustom();
  @JsonProperty("units") abstract String getUnits();
  @JsonProperty("dataType") abstract String getDataType();
  @JsonProperty("retention") abstract int getRetention();
  @JsonProperty("max") abstract double getMax();
  @JsonProperty("min") abstract double getMin();
}
