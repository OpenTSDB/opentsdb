package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import net.opentsdb.TestModule;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.AnnotationFixtures;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.IdUtils;

import dagger.ObjectGraph;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;

public class MetaClientAnnotationTest {
  @Inject TsdbStore store;
  @Inject MetaClient metaClient;

  private Annotation annotation;

  @Before
  public void before() throws Exception {
    ObjectGraph.create(new TestModule()).inject(this);

    annotation = AnnotationFixtures.provideAnnotation();
    store.updateAnnotation(annotation).join();
  }

  @Test(expected = IllegalArgumentException.class, timeout = MockBase.DEFAULT_TIMEOUT)
  public void getAnnotationNullTimeSeriesId() throws Exception {
    metaClient.getAnnotation(null, 5L).join();
  }

  @Test(expected = IllegalArgumentException.class, timeout = MockBase.DEFAULT_TIMEOUT)
  public void getAnnotationEmptyTimeSeriesId() throws Exception {
    metaClient.getAnnotation("", 5L).join();
  }

  @Test(expected = IllegalArgumentException.class, timeout = MockBase.DEFAULT_TIMEOUT)
  public void getAnnotationBadStartTime() throws Exception {
    metaClient.getAnnotation("000001000001000001", 0L).join();
  }

  @Test(timeout = MockBase.DEFAULT_TIMEOUT)
  public void getAnnotation() throws Exception {
    Annotation note = metaClient.getAnnotation("000001000001000001", 1388450562L).join();
    assertNotNull(note);
    assertEquals("000001000001000001", note.timeSeriesId());
    assertEquals("Hello!", note.message());
    assertEquals(1388450562L, note.startTime());
  }

  @Test(timeout = MockBase.DEFAULT_TIMEOUT)
  public void getAnnotationNotFound() throws Exception {
    assertNull(metaClient.getAnnotation("000001000001000001", 1388450564L).join());
  }

  @Test(timeout = MockBase.DEFAULT_TIMEOUT)
  public void delete() throws Exception {
    metaClient.delete(annotation).join();

    assertNull(store.getAnnotation(
        IdUtils.stringToUid(annotation.timeSeriesId()),
        annotation.startTime()).join());
  }

  @Test(timeout = MockBase.DEFAULT_TIMEOUT)
  public void deleteNotFound() throws Exception {
    final Annotation notFound = Annotation.create(annotation.timeSeriesId(),
        annotation.endTime() + 1, annotation.endTime() + 2, annotation.message());
    metaClient.delete(notFound).join();

    assertNotNull(store.getAnnotation(
        IdUtils.stringToUid(annotation.timeSeriesId()), annotation.startTime()).join());
  }

  @Test(timeout = MockBase.DEFAULT_TIMEOUT)
  public void updateAnnotation() throws Exception {
    final Annotation updatedannotation = Annotation.create(annotation.timeSeriesId(),
        annotation.startTime(), annotation.endTime(), "New message", annotation.properties());
    assertTrue(metaClient.updateAnnotation(updatedannotation).join());
  }

  @Test(timeout = MockBase.DEFAULT_TIMEOUT)
  public void updateAnnotationNoChanges() throws Exception {
    assertFalse(metaClient.updateAnnotation(annotation).join());
  }

  @Test(timeout = MockBase.DEFAULT_TIMEOUT)
  public void deleteRange() throws Exception {
    final int count = metaClient.deleteRange(
        IdUtils.stringToUid(annotation.timeSeriesId()), annotation.startTime() - 5,
        annotation.endTime() + 5).join();
    assertEquals(1, count);

    assertNull(store.getAnnotation(
        IdUtils.stringToUid(annotation.timeSeriesId()), annotation.startTime()).join());
  }

  @Test(timeout = MockBase.DEFAULT_TIMEOUT)
  public void deleteRangeNone() throws Exception {
    final int count = metaClient.deleteRange(
        IdUtils.stringToUid(annotation.timeSeriesId()), annotation.endTime() + 100,
        annotation.endTime() + 110).join();
    assertEquals(0, count);

    assertNotNull(store.getAnnotation(
        IdUtils.stringToUid(annotation.timeSeriesId()), annotation.startTime()).join());
  }

  @Test(timeout = MockBase.DEFAULT_TIMEOUT)
  public void deleteRangeMultiple() throws Exception {
    final Annotation second = Annotation.create(annotation.timeSeriesId(), annotation.endTime() + 5,
        annotation.endTime() + 10, annotation.message());

    final int count = metaClient.deleteRange(
        IdUtils.stringToUid(annotation.timeSeriesId()), annotation.startTime(),
        annotation.endTime() + 5).join();
    assertEquals(2, count);

    assertNull(store.getAnnotation(
        IdUtils.stringToUid(annotation.timeSeriesId()),
        annotation.startTime()).join());

    assertNull(store.getAnnotation(
        IdUtils.stringToUid(second.timeSeriesId()),
        second.startTime()).join());
  }

  @Test(expected = IllegalArgumentException.class, timeout = MockBase.DEFAULT_TIMEOUT)
  public void deleteRangeEmptyEnd() throws Exception {
    metaClient.deleteRange(null, 1328140799000L, 0).join();
  }

  @Test(expected = IllegalArgumentException.class, timeout = MockBase.DEFAULT_TIMEOUT)
  public void deleteRangeEndLessThanStart() throws Exception {
    metaClient.deleteRange(null, 1328140799000L, 1328140798000L).join();
  }
}