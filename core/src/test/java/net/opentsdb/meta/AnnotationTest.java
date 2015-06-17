package net.opentsdb.meta;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Map;

public class AnnotationTest {
  private static final String GOOD_TIME_SERIES_ID = "plxNoStrings";
  private static final long GOOD_START = 10L;
  private static final long GOOD_END = 8L;
  private static final String GOOD_MESSAGE = "My message";
  private static final Map<String, String> GOOD_PROPERTIES = ImmutableMap.of();

  private static final long BAD_START = -5L;
  private static final long BAD_END = -3L;

  @Test(expected = IllegalArgumentException.class)
  public void testCreateNullTimeSeriesId() {
    Annotation.create(null, GOOD_START, GOOD_END, GOOD_MESSAGE, GOOD_PROPERTIES);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateEmptyTimeSeriesId() {
    Annotation.create("", GOOD_START, GOOD_END, GOOD_MESSAGE, GOOD_PROPERTIES);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateBadStart() {
    Annotation.create(GOOD_TIME_SERIES_ID, BAD_START, GOOD_END, GOOD_MESSAGE, GOOD_PROPERTIES);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateBadEnd() {
    Annotation.create(GOOD_TIME_SERIES_ID, GOOD_START, BAD_END, GOOD_MESSAGE, GOOD_PROPERTIES);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateStartBeforeEnd() {
    Annotation.create(GOOD_TIME_SERIES_ID, GOOD_START - 2, GOOD_START, GOOD_MESSAGE,
        GOOD_PROPERTIES);
  }

  @Test(expected = NullPointerException.class)
  public void testNullMessage() {
    Annotation.create(GOOD_TIME_SERIES_ID, GOOD_START, GOOD_END, null, GOOD_PROPERTIES);
  }

  @Test(expected = NullPointerException.class)
  public void testNullProperties() {
    Annotation.create(GOOD_TIME_SERIES_ID, GOOD_START, GOOD_END, GOOD_MESSAGE, null);
  }

  @Test
  public void testCreateOrder() {
    final Annotation annotation = Annotation.create(GOOD_TIME_SERIES_ID, GOOD_START, GOOD_END,
        GOOD_MESSAGE, GOOD_PROPERTIES);
    assertEquals(GOOD_TIME_SERIES_ID, annotation.timeSeriesId());
    assertEquals(GOOD_START, annotation.startTime());
    assertEquals(GOOD_END, annotation.endTime());
    assertEquals(GOOD_MESSAGE, annotation.message());
    assertEquals(GOOD_PROPERTIES, annotation.properties());
  }
}