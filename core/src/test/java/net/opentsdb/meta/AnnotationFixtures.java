package net.opentsdb.meta;

import net.opentsdb.storage.MemoryLabelId;
import net.opentsdb.uid.LabelId;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.UUID;

/**
 * Fixture class for creating annotation objects in tests.
 */
public class AnnotationFixtures {
  private static final UUID METRIC_UUID = UUID.fromString("2d11dffc-a8c6-4830-8f4e-aa3417c4f9dc");
  private static final UUID METRIC_TAGKEY = UUID.fromString("edf3c0cc-9ebb-404e-b148-8cf140e13a1e");
  private static final UUID METRIC_TAGVAL = UUID.fromString("c4aa3396-4f0e-4def-988a-513191279cb3");

  private static final LabelId GOOD_METRIC = new MemoryLabelId(METRIC_UUID);
  private static final ImmutableMap<LabelId, LabelId> GOOD_TAGS = ImmutableMap.<LabelId, LabelId>of(
      new MemoryLabelId(METRIC_TAGKEY), new MemoryLabelId(METRIC_TAGVAL));
  private static final long GOOD_START = 10L;
  private static final long GOOD_END = 12L;
  private static final String GOOD_MESSAGE = "My message";
  private static final Map<String, String> GOOD_PROPERTIES = ImmutableMap.of();

  /** Build and return a new "good" annotation instance. */
  public static Annotation provideAnnotation() {
    return Annotation.create(GOOD_METRIC, GOOD_TAGS, GOOD_START, GOOD_END, GOOD_MESSAGE,
        GOOD_PROPERTIES);
  }
}
