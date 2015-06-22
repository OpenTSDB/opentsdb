package net.opentsdb.web.jackson;

import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.LabelId;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

abstract class AnnotationMixIn {
  @JsonCreator
  static Annotation create(@JsonProperty("metric") final LabelId metric,
                           @JsonProperty("tags") final Map<LabelId, LabelId> tags,
                           @JsonProperty("startTime") final long startTime,
                           @JsonProperty("endTime") final long endTime,
                           @JsonProperty("message") final String message,
                           @JsonProperty("properties") final Map<String, String> properties) {
    return Annotation.create(metric, tags, startTime, endTime, message, properties);
  }

  @JsonProperty
  abstract LabelId metric();

  @JsonProperty
  abstract ImmutableMap<LabelId, LabelId> tags();

  @JsonProperty
  abstract long startTime();

  @JsonProperty
  abstract long endTime();

  @JsonProperty
  abstract String message();

  @JsonProperty
  abstract ImmutableMap<String, String> properties();
}
