package net.opentsdb.core;

import java.util.List;

import com.google.common.collect.ImmutableMap;
import dagger.ObjectGraph;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.IdUtils;

import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;

import static org.junit.Assert.*;

public class MetaClientAnnotationTest {
  private Annotation note;

  @Inject MemoryStore store;
  @Inject MetaClient metaClient;

  private final byte[] tsuid_row_key =
          new byte[] { 0, 0, 1, (byte) 0x52, (byte) 0xC2, (byte) 0x09, 0, 0, 0,
                  1, 0, 0, 1 };

  @Before
  public void before() throws Exception {
    ObjectGraph.create(new TestModuleMemoryStore()).inject(this);

    note = new Annotation();

    Annotation note = new Annotation(null, 1328140800, 1328140801, "Description",
        "Notes", ImmutableMap.of("owner", "ops"));
    store.updateAnnotation(null, note);

    // add another global
    note = new Annotation(null, 1328140801, 1328140803, "Description", "Notes",
        ImmutableMap.of("owner", "ops"));
    store.updateAnnotation(null, note);

    // add a local
    note = new Annotation("000001000001000001", 1388450562, 1419984000, "Hello!",
        "My Notes", ImmutableMap.of("owner", "ops"));
    store.updateAnnotation(null, note);

    // add another local
    note = new Annotation("000001000001000001", 1388450563, 1419984000, "Note2",
        "Nothing", null);
    store.updateAnnotation(null, note);
  }

  @Test
  public void getGlobalAnnotations() throws Exception {
    List<Annotation> notes = metaClient.getGlobalAnnotations(1328140000,
            1328141000).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(notes);
    assertEquals(2, notes.size());
  }

  @Test
  public void getGlobalAnnotationsEmpty() throws Exception {
    List<Annotation> notes = metaClient.getGlobalAnnotations(1328150000,
            1328160000).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(notes);
    assertEquals(0, notes.size());
  }

  @Test (expected = IllegalArgumentException.class)
  public void getGlobalAnnotationsZeroEndtime() throws Exception {
    metaClient.getGlobalAnnotations(0, 0).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = IllegalArgumentException.class)
  public void getGlobalAnnotationsEndLessThanStart() throws Exception {
    metaClient.getGlobalAnnotations(1328150000, 1328140000).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void getAnnotation() throws Exception {
    note = metaClient.getAnnotation("000001000001000001", 1388450562L)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(note);
    assertEquals("000001000001000001", note.getTSUID());
    assertEquals("Hello!", note.getDescription());
    assertEquals(1388450562L, note.getStartTime());
  }

  @Test
  public void getAnnotationNormalizeMs() throws Exception {
    note = metaClient.getAnnotation("000001000001000001", 1388450562000L)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(note);
    assertEquals("000001000001000001", note.getTSUID());
    assertEquals("Hello!", note.getDescription());
    assertEquals(1388450562L, note.getStartTime());
  }

  @Test
  public void getAnnotationGlobal() throws Exception {
    note = metaClient.getAnnotation(null, 1328140800000L)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(note);
    assertEquals("", note.getTSUID());
    assertEquals("Description", note.getDescription());
    assertEquals(1328140800L, note.getStartTime());
  }

  @Test
  public void getAnnotationNotFound() throws Exception {
    note = metaClient.getAnnotation("000001000001000001", 1388450564L)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNull(note);
  }

  @Test
  public void getAnnotationGlobalNotFound() throws Exception {
    note = metaClient.getAnnotation(null, 1388450563L)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNull(note);
  }

  @Test (expected = IllegalArgumentException.class)
  public void getAnnotationNoStartTime() throws Exception {
    metaClient.getAnnotation("000001000001000001", 0L)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void delete() throws Exception {
    final long start_time = 1388450562;
    note.setTSUID("000001000001000001");
    note.setStartTime(start_time);
    metaClient.delete(note).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertNull(store.getAnnotation(
        IdUtils.stringToUid(note.getTSUID()),
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    note.setStartTime(start_time + 1);

    assertNotNull(store.getAnnotation(
        IdUtils.stringToUid(note.getTSUID()),
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void deleteNormalizeMs() throws Exception {
    note.setTSUID("000001000001000001");
    note.setStartTime(1388450562000L);

    metaClient.delete(note).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertNull(store.getAnnotation(
        IdUtils.stringToUid(note.getTSUID()),
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    note.setStartTime(1388450563000L);

    assertNotNull(store.getAnnotation(
        IdUtils.stringToUid(note.getTSUID()),
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  // this doesn't throw an error or anything, just issues the delete request
  // and it's ignored.
  @Test
  public void deleteNotFound() throws Exception {
    note.setTSUID("000001000001000001");
    note.setStartTime(1388450561);
    metaClient.delete(note).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);


    note.setStartTime(1388450562);
    assertNotNull(store.getAnnotation(
        IdUtils.stringToUid(note.getTSUID()),
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    note.setStartTime(1388450563);
    assertNotNull(store.getAnnotation(
        IdUtils.stringToUid(note.getTSUID()),
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test (expected = IllegalArgumentException.class)
  public void deleteMissingStart() throws Exception {
    note.setTSUID("000001000001000001");
    metaClient.delete(note).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void deleteGlobal() throws Exception {
    note.setStartTime(1328140800);
    metaClient.delete(note).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertNull(store.getAnnotation(null,
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    note.setStartTime(1328140801);
    assertNotNull(store.getAnnotation(null,
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void deleteGlobalNotFound() throws Exception {
    note.setStartTime(1328140803);
    metaClient.delete(note).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    note.setStartTime(1328140800);
    assertNotNull(store.getAnnotation(null,
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    note.setStartTime(1328140801);
    assertNotNull(store.getAnnotation(null,
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

  }

  @Test
  public void syncToStorage() throws Exception {
    note.setTSUID("000001000001000001");
    note.setStartTime(1388450562L);
    note.setDescription("Synced!");
    metaClient.syncToStorage(note, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    note = store.getAnnotation(IdUtils.stringToUid(note.getTSUID()),
            note.getStartTime()).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertEquals("000001000001000001", note.getTSUID());
    assertEquals("Synced!", note.getDescription());
    assertEquals("My Notes", note.getNotes());
  }

  @Test
  public void syncToStorageMilliseconds() throws Exception {
    note.setTSUID("000001000001000001");
    note.setStartTime(1388450562500L);
    note.setDescription("Synced!");
    metaClient.syncToStorage(note, false).joinUninterruptibly(MockBase
            .DEFAULT_TIMEOUT);

    note = store.getAnnotation(IdUtils.stringToUid(note.getTSUID()),
            note.getStartTime()).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertEquals("000001000001000001", note.getTSUID());
    assertEquals("Synced!", note.getDescription());
    assertEquals("", note.getNotes());
    assertEquals(1388450562500L, note.getStartTime());
  }

  @Test
  public void syncToStorageGlobal() throws Exception {
    note.setStartTime(1328140800L);
    note.setDescription("Synced!");
    metaClient.syncToStorage(note, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    note = store.getAnnotation(null,
            note.getStartTime()).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertEquals("", note.getTSUID());
    assertEquals("Synced!", note.getDescription());
    assertEquals("Notes", note.getNotes());
  }

  @Test
  public void syncToStorageGlobalMilliseconds() throws Exception {
    note.setStartTime(1328140800500L);
    note.setDescription("Synced!");
    metaClient.syncToStorage(note, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    note = store.getAnnotation(null,
            note.getStartTime()).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertEquals("", note.getTSUID());
    assertEquals("Synced!", note.getDescription());
    assertEquals("", note.getNotes());
  }

  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageMissingStart() throws Exception {
    note.setTSUID("000001000001000001");
    note.setDescription("Synced!");
    metaClient.syncToStorage(note, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = IllegalStateException.class)
  public void syncToStorageNoChanges() throws Exception {
    note.setTSUID("000001000001000001");
    note.setStartTime(1388450562L);
    metaClient.syncToStorage(note, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void deleteRange() throws Exception {
    note.setTSUID("000001000001000001");

    final int count = metaClient.deleteRange(
            IdUtils.stringToUid(note.getTSUID()), 1388450560000L,
            1388450562000L).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(1, count);

    note.setStartTime(1388450562);
    assertNull(store.getAnnotation(
        IdUtils.stringToUid(note.getTSUID()),
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    note.setStartTime(1388450563);
    assertNotNull(store.getAnnotation(
        IdUtils.stringToUid(note.getTSUID()),
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void deleteRangeNone() throws Exception {
    note.setTSUID("000001000001000001");
    final int count = metaClient.deleteRange(
            IdUtils.stringToUid(note.getTSUID()), 1388450560000L,
            1388450561000L).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(0, count);

    note.setStartTime(1388450562);
    assertNotNull(store.getAnnotation(
        IdUtils.stringToUid(note.getTSUID()),
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    note.setStartTime(1388450563);
    assertNotNull(store.getAnnotation(
        IdUtils.stringToUid(note.getTSUID()),
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void deleteRangeMultiple() throws Exception {
    note.setTSUID("000001000001000001");
    final int count = metaClient.deleteRange(
            new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1}, 1388450560000L,
            1388450568000L).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(2, count);

    note.setStartTime(1388450562);
    assertNull(store.getAnnotation(
        IdUtils.stringToUid(note.getTSUID()),
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    note.setStartTime(1388450563);
    assertNull(store.getAnnotation(
        IdUtils.stringToUid(note.getTSUID()),
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void deleteRangeGlobal() throws Exception {
    final int count = metaClient.deleteRange(null, 1328140799000L,
            1328140800000L).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(1, count);

    note.setStartTime(1328140800);
    assertNull(store.getAnnotation(null,
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    note.setStartTime(1328140801);
    assertNotNull(store.getAnnotation(null,
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void deleteRangeGlobalNone() throws Exception {
    final int count = metaClient.deleteRange(null, 1328140798000L,
            1328140799000L).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(0, count);
    note.setStartTime(1328140800);
    assertNotNull(store.getAnnotation(null,
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    note.setStartTime(1328140801);
    assertNotNull(store.getAnnotation(null,
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void deleteRangeGlobalMultiple() throws Exception {
    final int count = metaClient.deleteRange(null, 1328140799000L,
            1328140900000L).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(2, count);
    note.setStartTime(1328140800);
    assertNull(store.getAnnotation(null,
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    note.setStartTime(1328140801);
    assertNull(store.getAnnotation(null,
        note.getStartTime()
    ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test (expected = IllegalArgumentException.class)
  public void deleteRangeEmptyEnd() throws Exception {
    metaClient.deleteRange(null, 1328140799000L, 0).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = IllegalArgumentException.class)
  public void deleteRangeEndLessThanStart() throws Exception {
    metaClient.deleteRange(null, 1328140799000L, 1328140798000L)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }
}