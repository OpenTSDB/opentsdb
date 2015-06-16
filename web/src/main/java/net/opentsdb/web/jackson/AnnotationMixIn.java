package net.opentsdb.web.jackson;

import net.opentsdb.meta.Annotation;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
abstract class AnnotationMixIn {
  @JsonCreator
  static Annotation create(@JsonProperty("timeSeriesId") final String timeSeriesId,
                           @JsonProperty("startTime") final long startTime,
                           @JsonProperty("endTime") final long endTime,
                           @JsonProperty("message") final String message,
                           @JsonProperty("properties") final Map<String, String> properties) {
    return Annotation.create(timeSeriesId, startTime, endTime, message, properties);
  }

  @JsonProperty
  abstract String timeSeriesId();

  @JsonProperty
  abstract long startTime();

  @JsonProperty
  abstract long endTime();

  @JsonProperty
  abstract String message();

  @JsonProperty
  abstract ImmutableMap<String, String> properties();
}
