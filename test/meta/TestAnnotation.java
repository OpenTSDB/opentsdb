// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;

import net.opentsdb.storage.MemoryStore;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

import org.junit.Before;
import org.junit.Test;

public final class TestAnnotation {
  private TSDB tsdb;
  private MemoryStore tsdb_store;
  private Annotation note = new Annotation();
  
  final private byte[] global_row_key = 
      new byte[] { 0, 0, 0, (byte) 0x4F, (byte) 0x29, (byte) 0xD2, 0 };
  final private byte[] tsuid_row_key = 
      new byte[] { 0, 0, 1, (byte) 0x52, (byte) 0xC2, (byte) 0x09, 0, 0, 0, 
        1, 0, 0, 1 };
  
  @Before
  public void before() throws Exception {
    final Config config = new Config(false);
    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, config);

    // add a global
    String json = "{\"startTime\":1328140800,\"endTime\":1328140801,\"" +
            "description\":\"Description\",\"notes\":\"Notes\",\"custom\"" +
            ":{\"owner\":\"ops\"}}";

   Annotation note = JSON.parseToObject(json, Annotation.class);
    tsdb_store.updateAnnotation(null, note);

    // add another global
    json = "{\"startTime\":1328140801,\"endTime\":1328140803,\"description\":" +
            "\"Description\",\"notes\":\"Notes\",\"custom\":{\"owner\":" +
            "\"ops\"}}";
    note = JSON.parseToObject(json, Annotation.class);
    tsdb_store.updateAnnotation(null, note);

    // add a local
    json = "{\"tsuid\":\"000001000001000001\",\"startTime\":1388450562," +
            "\"endTime\":1419984000,\"description\":\"Hello!\",\"notes\":" +
            "\"My Notes\",\"custom\":{\"owner\":\"ops\"}}";
    note = JSON.parseToObject(json, Annotation.class);
    tsdb_store.updateAnnotation(null, note);

    // add another local
    json = "{\"tsuid\":\"000001000001000001\",\"startTime\":1388450563," +
            "\"endTime\":1419984000,\"description\":\"Note2\",\"notes\":" +
            "\"Nothing\"}";
    note = JSON.parseToObject(json, Annotation.class);
    tsdb_store.updateAnnotation(null, note);

    // add some data points too maybe not relevant any more
    tsdb_store.addColumn(tsuid_row_key,
      new byte[]{0x50, 0x10}, new byte[]{1});
    
    tsdb_store.addColumn(tsuid_row_key,
      new byte[]{0x50, 0x18}, new byte[]{2});
  }
  
  @Test
  public void constructor() {
    assertNotNull(new Annotation());
  }

  @Test
  public void serialize() throws Exception {
    assertNotNull(JSON.serializeToString(note));
  }
  
  @Test
  public void deserialize() throws Exception {
    String json = "{\"tsuid\":\"ABCD\",\"description\":\"Description\"," + 
    "\"notes\":\"Notes\",\"custom\":null,\"endTime\":1328140801,\"startTime" + 
    "\":1328140800}";
    Annotation note = JSON.parseToObject(json, Annotation.class);
    assertNotNull(note);
    assertEquals(note.getTSUID(), "ABCD");
  }

  @Test
  public void getAnnotation() throws Exception {
    note = tsdb.getAnnotation("000001000001000001", 1388450562L)
      .joinUninterruptibly();
    assertNotNull(note);
    assertEquals("000001000001000001", note.getTSUID());
    assertEquals("Hello!", note.getDescription());
    assertEquals(1388450562L, note.getStartTime());
  }
  
  @Test
  public void getAnnotationNormalizeMs() throws Exception {
    note = tsdb.getAnnotation("000001000001000001", 1388450562000L)
      .joinUninterruptibly();
    assertNotNull(note);
    assertEquals("000001000001000001", note.getTSUID());
    assertEquals("Hello!", note.getDescription());
    assertEquals(1388450562L, note.getStartTime());
  }
  
  @Test
  public void getAnnotationGlobal() throws Exception {
    note = tsdb.getAnnotation(null, 1328140800000L)
      .joinUninterruptibly();
    assertNotNull(note);
    assertEquals("", note.getTSUID());
    assertEquals("Description", note.getDescription());
    assertEquals(1328140800L, note.getStartTime());
  }

  @Test
  public void getAnnotationNotFound() throws Exception {
    note = tsdb.getAnnotation("000001000001000001", 1388450564L)
      .joinUninterruptibly();
    assertNull(note);
  }
  
  @Test
  public void getAnnotationGlobalNotFound() throws Exception {
    note = tsdb.getAnnotation(null, 1388450563L)
      .joinUninterruptibly();
    assertNull(note);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getAnnotationNoStartTime() throws Exception {
    tsdb.getAnnotation("000001000001000001", 0L)
      .joinUninterruptibly();  
  }
  
  @Test
  public void getGlobalAnnotations() throws Exception {
    List<Annotation> notes = tsdb.getGlobalAnnotations(1328140000,
            1328141000).joinUninterruptibly();
    assertNotNull(notes);
    assertEquals(2, notes.size());
  }
  
  @Test
  public void getGlobalAnnotationsEmpty() throws Exception {
    List<Annotation> notes = tsdb.getGlobalAnnotations(1328150000,
            1328160000).joinUninterruptibly();
    assertNotNull(notes);
    assertEquals(0, notes.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getGlobalAnnotationsZeroEndtime() throws Exception {
    tsdb.getGlobalAnnotations(0, 0).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getGlobalAnnotationsEndLessThanStart() throws Exception {
    tsdb.getGlobalAnnotations(1328150000, 1328140000).joinUninterruptibly();
  }
  
  @Test
  public void syncToStorage() throws Exception {
    note.setTSUID("000001000001000001");
    note.setStartTime(1388450562L);
    note.setDescription("Synced!");
    tsdb.syncToStorage(note, false).joinUninterruptibly();

    note = tsdb_store.getAnnotation(UniqueId.stringToUid(note.getTSUID()),
            note.getStartTime()).joinUninterruptibly();

    assertEquals("000001000001000001", note.getTSUID());
    assertEquals("Synced!", note.getDescription());
    assertEquals("My Notes", note.getNotes());
  }
  
  @Test
  public void syncToStorageMilliseconds() throws Exception {
    note.setTSUID("000001000001000001");
    note.setStartTime(1388450562500L);
    note.setDescription("Synced!");
    tsdb.syncToStorage(note, false).joinUninterruptibly();

    note = tsdb_store.getAnnotation(UniqueId.stringToUid(note.getTSUID()),
            note.getStartTime()).joinUninterruptibly();

    assertEquals("000001000001000001", note.getTSUID());
    assertEquals("Synced!", note.getDescription());
    assertEquals("", note.getNotes());
    assertEquals(1388450562500L, note.getStartTime());
  }
  
  @Test
  public void syncToStorageGlobal() throws Exception {
    note.setStartTime(1328140800L);
    note.setDescription("Synced!");
    tsdb.syncToStorage(note, false).joinUninterruptibly();

    note = tsdb_store.getAnnotation(null,
            note.getStartTime()).joinUninterruptibly();

    assertEquals("", note.getTSUID());
    assertEquals("Synced!", note.getDescription());
    assertEquals("Notes", note.getNotes());
  }
  
  @Test
  public void syncToStorageGlobalMilliseconds() throws Exception {
    note.setStartTime(1328140800500L);
    note.setDescription("Synced!");
    tsdb.syncToStorage(note, false).joinUninterruptibly();

    note = tsdb_store.getAnnotation(null,
            note.getStartTime()).joinUninterruptibly();

    assertEquals("", note.getTSUID());
    assertEquals("Synced!", note.getDescription());
    assertEquals("", note.getNotes());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageMissingStart() throws Exception {
    note.setTSUID("000001000001000001");
    note.setDescription("Synced!");
    tsdb.syncToStorage(note, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalStateException.class)
  public void syncToStorageNoChanges() throws Exception {
    note.setTSUID("000001000001000001");
    note.setStartTime(1388450562L);
    tsdb.syncToStorage(note, false).joinUninterruptibly();
  }
  
  @Test
  public void delete() throws Exception {
    final long start_time = 1388450562;
    note.setTSUID("000001000001000001");
    note.setStartTime(start_time);
    tsdb.delete(note).joinUninterruptibly();

    assertNull(tsdb_store.getAnnotation(
            UniqueId.stringToUid(note.getTSUID()),
            note.getStartTime()
    ).joinUninterruptibly());

    note.setStartTime(start_time + 1);

    assertNotNull(tsdb_store.getAnnotation(
            UniqueId.stringToUid(note.getTSUID()),
            note.getStartTime()
    ).joinUninterruptibly());


    assertNotNull(tsdb_store.getColumn(tsuid_row_key,
      new byte[]{0x50, 0x10}));
    assertNotNull(tsdb_store.getColumn(tsuid_row_key,
        new byte[] { 0x50, 0x18 }));
  }
  
  @Test
  public void deleteNormalizeMs() throws Exception {
    note.setTSUID("000001000001000001");
    note.setStartTime(1388450562000L);
    tsdb.delete(note).joinUninterruptibly();

    assertNull(tsdb_store.getAnnotation(
            UniqueId.stringToUid(note.getTSUID()),
            note.getStartTime()
    ).joinUninterruptibly());

    note.setStartTime(1388450563000L);

    assertNotNull(tsdb_store.getAnnotation(
            UniqueId.stringToUid(note.getTSUID()),
            note.getStartTime()
    ).joinUninterruptibly());

    assertNotNull(tsdb_store.getColumn(tsuid_row_key,
      new byte[]{0x50, 0x10}));
    assertNotNull(tsdb_store.getColumn(tsuid_row_key,
        new byte[] { 0x50, 0x18 }));
  }
  
  // this doesn't throw an error or anything, just issues the delete request
  // and it's ignored.
  @Test
  public void deleteNotFound() throws Exception {
    note.setTSUID("000001000001000001");
    note.setStartTime(1388450561);
    tsdb.delete(note).joinUninterruptibly();


    note.setStartTime(1388450562);
    assertNotNull(tsdb_store.getAnnotation(
            UniqueId.stringToUid(note.getTSUID()),
            note.getStartTime()
    ).joinUninterruptibly());

    note.setStartTime(1388450563);
    assertNotNull(tsdb_store.getAnnotation(
            UniqueId.stringToUid(note.getTSUID()),
            note.getStartTime()
    ).joinUninterruptibly());

    assertNotNull(tsdb_store.getColumn(tsuid_row_key,
      new byte[]{0x50, 0x10}));
    assertNotNull(tsdb_store.getColumn(tsuid_row_key,
        new byte[] { 0x50, 0x18 }));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void deleteMissingStart() throws Exception {
    note.setTSUID("000001000001000001");
    tsdb.delete(note).joinUninterruptibly();
  }
  
  @Test
  public void deleteGlobal() throws Exception {
    note.setStartTime(1328140800);
    tsdb.delete(note).joinUninterruptibly();

    assertNull(tsdb_store.getAnnotation(null,
            note.getStartTime()
    ).joinUninterruptibly());

    note.setStartTime(1328140801);
    assertNotNull(tsdb_store.getAnnotation(null,
            note.getStartTime()
    ).joinUninterruptibly());
  }
  
  @Test
  public void deleteGlobalNotFound() throws Exception {
    note.setStartTime(1328140803);
    tsdb.delete(note).joinUninterruptibly();

    note.setStartTime(1328140800);
    assertNotNull(tsdb_store.getAnnotation(null,
            note.getStartTime()
    ).joinUninterruptibly());

    note.setStartTime(1328140801);
    assertNotNull(tsdb_store.getAnnotation(null,
            note.getStartTime()
    ).joinUninterruptibly());

  }
  
  @Test
  public void deleteRange() throws Exception {
    note.setTSUID("000001000001000001");
    final int count = tsdb.deleteRange(
            UniqueId.stringToUid(note.getTSUID()), 1388450560000L,
            1388450562000L).joinUninterruptibly();
    assertEquals(1, count);

    note.setStartTime(1388450562);
    assertNull(tsdb_store.getAnnotation(
            UniqueId.stringToUid(note.getTSUID()),
            note.getStartTime()
    ).joinUninterruptibly());

    note.setStartTime(1388450563);
    assertNotNull(tsdb_store.getAnnotation(
            UniqueId.stringToUid(note.getTSUID()),
            note.getStartTime()
    ).joinUninterruptibly());

    assertNotNull(tsdb_store.getColumn(tsuid_row_key,
      new byte[]{0x50, 0x10}));
    assertNotNull(tsdb_store.getColumn(tsuid_row_key,
        new byte[] { 0x50, 0x18 }));
  }
  
  @Test
  public void deleteRangeNone() throws Exception {
    note.setTSUID("000001000001000001");
    final int count = tsdb.deleteRange(
            UniqueId.stringToUid(note.getTSUID()), 1388450560000L,
            1388450561000L).joinUninterruptibly();
    assertEquals(0, count);

    note.setStartTime(1388450562);
    assertNotNull(tsdb_store.getAnnotation(
            UniqueId.stringToUid(note.getTSUID()),
            note.getStartTime()
    ).joinUninterruptibly());

    note.setStartTime(1388450563);
    assertNotNull(tsdb_store.getAnnotation(
            UniqueId.stringToUid(note.getTSUID()),
            note.getStartTime()
    ).joinUninterruptibly());

    assertNotNull(tsdb_store.getColumn(tsuid_row_key,
      new byte[]{0x50, 0x10}));
    assertNotNull(tsdb_store.getColumn(tsuid_row_key,
        new byte[] { 0x50, 0x18 }));
  }
  
  @Test
  public void deleteRangeMultiple() throws Exception {
    note.setTSUID("000001000001000001");
    final int count = tsdb.deleteRange(
            new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1}, 1388450560000L,
            1388450568000L).joinUninterruptibly();
    assertEquals(2, count);

    note.setStartTime(1388450562);
    assertNull(tsdb_store.getAnnotation(
            UniqueId.stringToUid(note.getTSUID()),
            note.getStartTime()
    ).joinUninterruptibly());

    note.setStartTime(1388450563);
    assertNull(tsdb_store.getAnnotation(
            UniqueId.stringToUid(note.getTSUID()),
            note.getStartTime()
    ).joinUninterruptibly());

    assertNotNull(tsdb_store.getColumn(tsuid_row_key,
      new byte[]{0x50, 0x10}));
    assertNotNull(tsdb_store.getColumn(tsuid_row_key,
        new byte[] { 0x50, 0x18 }));
  }
  
  @Test
  public void deleteRangeGlobal() throws Exception {
    final int count = tsdb.deleteRange(null, 1328140799000L,
            1328140800000L).joinUninterruptibly();
    assertEquals(1, count);


    note.setStartTime(1328140800);
    assertNull(tsdb_store.getAnnotation(null,
            note.getStartTime()
    ).joinUninterruptibly());

    note.setStartTime(1328140801);
    assertNotNull(tsdb_store.getAnnotation( null,
            note.getStartTime()
    ).joinUninterruptibly());
  }
  
  @Test
  public void deleteRangeGlobalNone() throws Exception {
    final int count = tsdb.deleteRange(null, 1328140798000L,
            1328140799000L).joinUninterruptibly();
    assertEquals(0, count);
    note.setStartTime(1328140800);
    assertNotNull(tsdb_store.getAnnotation(null,
            note.getStartTime()
    ).joinUninterruptibly());

    note.setStartTime(1328140801);
    assertNotNull(tsdb_store.getAnnotation( null,
            note.getStartTime()
    ).joinUninterruptibly());
  }
  
  @Test
  public void deleteRangeGlobalMultiple() throws Exception {
    final int count = tsdb.deleteRange(null, 1328140799000L,
            1328140900000L).joinUninterruptibly();
    assertEquals(2, count);
    note.setStartTime(1328140800);
    assertNull(tsdb_store.getAnnotation(null,
            note.getStartTime()
    ).joinUninterruptibly());

    note.setStartTime(1328140801);
    assertNull(tsdb_store.getAnnotation( null,
            note.getStartTime()
    ).joinUninterruptibly());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void deleteRangeEmptyEnd() throws Exception {
    tsdb.deleteRange(null, 1328140799000L, 0).joinUninterruptibly();
  }

  @Test (expected = IllegalArgumentException.class)
  public void deleteRangeEndLessThanStart() throws Exception {
    tsdb.deleteRange(null, 1328140799000L, 1328140798000L)
      .joinUninterruptibly();
  }
}
