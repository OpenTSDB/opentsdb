package net.opentsdb.web.jackson;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
abstract class AnnotationMixIn {
  AnnotationMixIn(@JsonProperty("tsuid") final String tsuid,
                  @JsonProperty("startTime") final long startTime,
                  @JsonProperty("endTime") final long endTime,
                  @JsonProperty("description") final String description,
                  @JsonProperty("notes") final String notes,
                  @JsonProperty("custom") final Map<String, String> custom) {
  }

  @JsonProperty("tsuid")
  abstract String getTSUID();

  @JsonProperty("startTime")
  abstract long getStartTime();

  @JsonProperty("endTime")
  abstract long getEndTime();

  @JsonProperty("description")
  abstract String getDescription();

  @JsonProperty("notes")
  abstract String getNotes();

  @JsonProperty("custom")
  abstract Map<String, String> getCustom();
}
