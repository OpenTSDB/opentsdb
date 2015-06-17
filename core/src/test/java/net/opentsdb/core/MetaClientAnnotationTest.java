package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import net.opentsdb.TestModule;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.AnnotationFixtures;
import net.opentsdb.utils.TestUtil;
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
    store.updateAnnotation(annotation).get();
  }

  @Test(expected = IllegalArgumentException.class, timeout = TestUtil.TIMEOUT)
  public void getAnnotationNullTimeSeriesId() throws Exception {
    metaClient.getAnnotation(null, 5L).get();
  }

  @Test(expected = IllegalArgumentException.class, timeout = TestUtil.TIMEOUT)
  public void getAnnotationEmptyTimeSeriesId() throws Exception {
    metaClient.getAnnotation("", 5L).get();
  }

  @Test(expected = IllegalArgumentException.class, timeout = TestUtil.TIMEOUT)
  public void getAnnotationBadStartTime() throws Exception {
    metaClient.getAnnotation("000001000001000001", 0L).get();
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void getAnnotation() throws Exception {
    Annotation note = metaClient.getAnnotation("000001000001000001", 1388450562L).get();
    assertNotNull(note);
    assertEquals("000001000001000001", note.timeSeriesId());
    assertEquals("Hello!", note.message());
    assertEquals(1388450562L, note.startTime());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void getAnnotationNotFound() throws Exception {
    assertNull(metaClient.getAnnotation("000001000001000001", 1388450564L).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void delete() throws Exception {
    metaClient.delete(annotation).get();

    assertNull(store.getAnnotation(
        IdUtils.stringToUid(annotation.timeSeriesId()),
        annotation.startTime()).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void deleteNotFound() throws Exception {
    final Annotation notFound = Annotation.create(annotation.timeSeriesId(),
        annotation.endTime() + 1, annotation.endTime() + 2, annotation.message());
    metaClient.delete(notFound).get();

    assertNotNull(store.getAnnotation(
        IdUtils.stringToUid(annotation.timeSeriesId()), annotation.startTime()).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void updateAnnotation() throws Exception {
    final Annotation updatedannotation = Annotation.create(annotation.timeSeriesId(),
        annotation.startTime(), annotation.endTime(), "New message", annotation.properties());
    assertTrue(metaClient.updateAnnotation(updatedannotation).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void updateAnnotationNoChanges() throws Exception {
    assertFalse(metaClient.updateAnnotation(annotation).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void deleteRange() throws Exception {
    final int count = metaClient.deleteRange(
        IdUtils.stringToUid(annotation.timeSeriesId()), annotation.startTime() - 5,
        annotation.endTime() + 5).get();
    assertEquals(1, count);

    assertNull(store.getAnnotation(
        IdUtils.stringToUid(annotation.timeSeriesId()), annotation.startTime()).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void deleteRangeNone() throws Exception {
    final int count = metaClient.deleteRange(
        IdUtils.stringToUid(annotation.timeSeriesId()), annotation.endTime() + 100,
        annotation.endTime() + 110).get();
    assertEquals(0, count);

    assertNotNull(store.getAnnotation(
        IdUtils.stringToUid(annotation.timeSeriesId()), annotation.startTime()).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void deleteRangeMultiple() throws Exception {
    final Annotation second = Annotation.create(annotation.timeSeriesId(), annotation.endTime() + 5,
        annotation.endTime() + 10, annotation.message());

    final int count = metaClient.deleteRange(
        IdUtils.stringToUid(annotation.timeSeriesId()), annotation.startTime(),
        annotation.endTime() + 5).get();
    assertEquals(2, count);

    assertNull(store.getAnnotation(
        IdUtils.stringToUid(annotation.timeSeriesId()),
        annotation.startTime()).get());

    assertNull(store.getAnnotation(
        IdUtils.stringToUid(second.timeSeriesId()),
        second.startTime()).get());
  }

  @Test(expected = IllegalArgumentException.class, timeout = TestUtil.TIMEOUT)
  public void deleteRangeEmptyEnd() throws Exception {
    metaClient.deleteRange(null, 1328140799000L, 0).get();
  }

  @Test(expected = IllegalArgumentException.class, timeout = TestUtil.TIMEOUT)
  public void deleteRangeEndLessThanStart() throws Exception {
    metaClient.deleteRange(null, 1328140799000L, 1328140798000L).get();
  }
}