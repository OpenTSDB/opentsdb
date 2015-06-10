package net.opentsdb.meta;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Fixture class for creating annotation objects in tests.
 */
public class AnnotationFixtures {
  private static final String GOOD_TIME_SERIES_ID = "plxNoStrings";
  private static final long GOOD_START = 10L;
  private static final long GOOD_END = 12L;
  private static final String GOOD_MESSAGE = "My message";
  private static final Map<String, String> GOOD_PROPERTIES = ImmutableMap.of();

  /** Build and return a new "good" annotation instance. */
  public static Annotation provideAnnotation() {
    return Annotation.create(GOOD_TIME_SERIES_ID, GOOD_START, GOOD_END, GOOD_MESSAGE,
        GOOD_PROPERTIES);
  }
}
