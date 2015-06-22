package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import net.opentsdb.TestModule;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.AnnotationFixtures;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.LabelId;
import net.opentsdb.utils.TestUtil;

import com.google.common.collect.ImmutableMap;
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

  @Test(expected = NullPointerException.class, timeout = TestUtil.TIMEOUT)
  public void getAnnotationNullMetric() throws Exception {
    metaClient.getAnnotation(null, annotation.tags(), annotation.startTime()).get();
  }

  @Test(expected = NullPointerException.class, timeout = TestUtil.TIMEOUT)
  public void getAnnotationNullTags() throws Exception {
    metaClient.getAnnotation(annotation.metric(), null, annotation.startTime()).get();
  }

  @Test(expected = IllegalArgumentException.class, timeout = TestUtil.TIMEOUT)
  public void getAnnotationNoTags() throws Exception {
    metaClient.getAnnotation(annotation.metric(), ImmutableMap.<LabelId, LabelId>of(),
        annotation.startTime()).get();
  }

  @Test(expected = IllegalArgumentException.class, timeout = TestUtil.TIMEOUT)
  public void getAnnotationBadStartTime() throws Exception {
    metaClient.getAnnotation(annotation.metric(), annotation.tags(), 0L).get();
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void getAnnotation() throws Exception {
    Annotation fetched = metaClient.getAnnotation(annotation.metric(), annotation.tags(),
        annotation.startTime()).get();
    assertNotNull(fetched);
    assertEquals(annotation.metric(), fetched.metric());
    assertEquals(annotation.tags(), fetched.tags());
    assertEquals(annotation.message(), fetched.message());
    assertEquals(annotation.startTime(), fetched.startTime());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void getAnnotationNotFound() throws Exception {
    assertNull(metaClient.getAnnotation(annotation.metric(), annotation.tags(),
        annotation.startTime() - 5L).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void delete() throws Exception {
    metaClient.delete(annotation).get();

    assertNull(store.getAnnotation(annotation.metric(), annotation.tags(),
        annotation.startTime()).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void deleteNotFound() throws Exception {
    final Annotation notFound = Annotation.create(annotation.metric(), annotation.tags(),
        annotation.endTime() + 1, annotation.endTime() + 2, annotation.message());
    metaClient.delete(notFound).get();

    assertNotNull(store.getAnnotation(annotation.metric(), annotation.tags(),
        annotation.startTime()).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void updateAnnotation() throws Exception {
    final Annotation updatedannotation = Annotation.create(annotation.metric(), annotation.tags(),
        annotation.startTime(), annotation.endTime(), "New message", annotation.properties());
    assertTrue(metaClient.updateAnnotation(updatedannotation).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void updateAnnotationNoChanges() throws Exception {
    assertFalse(metaClient.updateAnnotation(annotation).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void deleteRange() throws Exception {
    final long count = metaClient.deleteRange(annotation.metric(), annotation.tags(),
        annotation.startTime() - 5, annotation.endTime() + 5).get();
    assertEquals(1L, count);

    assertNull(store.getAnnotation(annotation.metric(), annotation.tags(),
        annotation.startTime()).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void deleteRangeNone() throws Exception {
    final long count = metaClient.deleteRange(annotation.metric(), annotation.tags(),
        annotation.endTime() + 100, annotation.endTime() + 110).get();
    assertEquals(0L, count);

    assertNotNull(store.getAnnotation(annotation.metric(), annotation.tags(),
        annotation.startTime()).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void deleteRangeMultiple() throws Exception {
    final Annotation second = Annotation.create(annotation.metric(), annotation.tags(),
        annotation.endTime() + 5, annotation.endTime() + 10, annotation.message());

    final long count = metaClient.deleteRange(annotation.metric(), annotation.tags(),
        annotation.startTime(), annotation.endTime() + 5).get();
    assertEquals(2L, count);

    assertNull(store.getAnnotation(annotation.metric(), annotation.tags(),
        annotation.startTime()).get());

    assertNull(store.getAnnotation(annotation.metric(), annotation.tags(),
        second.startTime()).get());
  }

  @Test(expected = IllegalArgumentException.class, timeout = TestUtil.TIMEOUT)
  public void deleteRangeNoStart() throws Exception {
    metaClient.deleteRange(annotation.metric(), annotation.tags(), 0L, 1L).get();
  }

  @Test(expected = IllegalArgumentException.class, timeout = TestUtil.TIMEOUT)
  public void deleteRangeEndLessThanStart() throws Exception {
    metaClient.deleteRange(annotation.metric(), annotation.tags(),
        1328140799000L, 1328140798000L).get();
  }
}