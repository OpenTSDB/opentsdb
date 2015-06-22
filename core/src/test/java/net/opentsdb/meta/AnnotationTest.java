package net.opentsdb.meta;

import static org.junit.Assert.assertEquals;

import net.opentsdb.storage.MemoryLabelId;
import net.opentsdb.uid.LabelId;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;

public class AnnotationTest {
  private static final LabelId GOOD_METRIC = new MemoryLabelId(UUID.randomUUID());
  private static final Map<LabelId, LabelId> GOOD_TAGS = ImmutableMap.<LabelId, LabelId>of(
      new MemoryLabelId(UUID.randomUUID()), new MemoryLabelId(UUID.randomUUID()));
  private static final long GOOD_START = 10L;
  private static final long GOOD_END = 12L;
  private static final String GOOD_MESSAGE = "My message";
  private static final Map<String, String> GOOD_PROPERTIES = ImmutableMap.of();

  private static final long BAD_START = -5L;
  private static final long BAD_END = -3L;

  @Test(expected = NullPointerException.class)
  public void testCreateNullMetric() {
    Annotation.create(null, GOOD_TAGS, GOOD_START, GOOD_END, GOOD_MESSAGE, GOOD_PROPERTIES);
  }

  @Test(expected = NullPointerException.class)
  public void testCreateNullTags() {
    Annotation.create(GOOD_METRIC, null, GOOD_START, GOOD_END, GOOD_MESSAGE, GOOD_PROPERTIES);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateNoTags() {
    Annotation.create(GOOD_METRIC, ImmutableMap.<LabelId, LabelId>of(), GOOD_START, GOOD_END,
        GOOD_MESSAGE, GOOD_PROPERTIES);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateBadStart() {
    Annotation.create(GOOD_METRIC, GOOD_TAGS, BAD_START, GOOD_END, GOOD_MESSAGE, GOOD_PROPERTIES);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateBadEnd() {
    Annotation.create(GOOD_METRIC, GOOD_TAGS, GOOD_START, BAD_END, GOOD_MESSAGE, GOOD_PROPERTIES);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateEndBeforeStart() {
    Annotation.create(GOOD_METRIC, GOOD_TAGS, GOOD_START, GOOD_START - 2L, GOOD_MESSAGE,
        GOOD_PROPERTIES);
  }

  @Test(expected = NullPointerException.class)
  public void testNullMessage() {
    Annotation.create(GOOD_METRIC, GOOD_TAGS, GOOD_START, GOOD_END, null, GOOD_PROPERTIES);
  }

  @Test(expected = NullPointerException.class)
  public void testNullProperties() {
    Annotation.create(GOOD_METRIC, GOOD_TAGS, GOOD_START, GOOD_END, GOOD_MESSAGE, null);
  }

  @Test
  public void testCreateOrder() {
    final Annotation annotation = Annotation.create(GOOD_METRIC, GOOD_TAGS, GOOD_START, GOOD_END,
        GOOD_MESSAGE, GOOD_PROPERTIES);
    assertEquals(GOOD_METRIC, annotation.metric());
    assertEquals(GOOD_TAGS, annotation.tags());
    assertEquals(GOOD_START, annotation.startTime());
    assertEquals(GOOD_END, annotation.endTime());
    assertEquals(GOOD_MESSAGE, annotation.message());
    assertEquals(GOOD_PROPERTIES, annotation.properties());
  }
}