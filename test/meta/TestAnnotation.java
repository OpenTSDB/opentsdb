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

import net.opentsdb.core.BaseTsdbTest;
import net.opentsdb.core.Const;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.utils.JSON;

import org.hbase.async.Bytes;
import org.hbase.async.Scanner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({ Annotation.class, Const.class, Scanner.class })
public final class TestAnnotation extends BaseTsdbTest {
  private Annotation note = new Annotation();
  private final static String TSUID = "000001000001000001";
  private byte[] global_row_key;
  private byte[] tsuid_row_key;
  // 1425715200 - Sat Mar 7 00:00:00 PST 2015
  private byte[] global_row_key_2015_midnight;

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
    setupStorage(false);

    note = Annotation.getAnnotation(tsdb, TSUID, 1388450562L)
      .joinUninterruptibly();
    assertNotNull(note);
    assertEquals(TSUID, note.getTSUID());
    assertEquals("Hello!", note.getDescription());
    assertEquals(1388450562L, note.getStartTime());
  }
  
  @Test
  public void getAnnotationSalted() throws Exception {
    setupStorage(true);
    note = Annotation.getAnnotation(tsdb, TSUID, 1388450562L)
      .joinUninterruptibly();
    assertNotNull(note);
    assertEquals(TSUID, note.getTSUID());
    assertEquals("Hello!", note.getDescription());
    assertEquals(1388450562L, note.getStartTime());
  }
  
  @Test
  public void getAnnotationNormalizeMs() throws Exception {
    setupStorage(false);
    note = Annotation.getAnnotation(tsdb, TSUID, 1388450562000L)
      .joinUninterruptibly();
    assertNotNull(note);
    assertEquals(TSUID, note.getTSUID());
    assertEquals("Hello!", note.getDescription());
    assertEquals(1388450562L, note.getStartTime());
  }
  
  @Test
  public void getAnnotationGlobal() throws Exception {
    setupStorage(false);
    note = Annotation.getAnnotation(tsdb, 1328140800000L)
      .joinUninterruptibly();
    assertNotNull(note);
    assertEquals("", note.getTSUID());
    assertEquals("Description", note.getDescription());
    assertEquals(1328140800L, note.getStartTime());
  }

  @Test
  public void getAnnotationGlobalSalted() throws Exception {
    setupStorage(true);
    note = Annotation.getAnnotation(tsdb, 1328140800000L)
      .joinUninterruptibly();
    assertNotNull(note);
    assertEquals("", note.getTSUID());
    assertEquals("Description", note.getDescription());
    assertEquals(1328140800L, note.getStartTime());
  }
  
  @Test
  public void getAnnotationNotFound() throws Exception {
    setupStorage(false);
    note = Annotation.getAnnotation(tsdb, TSUID, 1388450564L)
      .joinUninterruptibly();
    assertNull(note);
  }
  
  @Test
  public void getAnnotationGlobalNotFound() throws Exception {
    setupStorage(false);
    note = Annotation.getAnnotation(tsdb, 1388450563L)
      .joinUninterruptibly();
    assertNull(note);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getAnnotationNoStartTime() throws Exception {
    Annotation.getAnnotation(tsdb, TSUID, 0L)
      .joinUninterruptibly();  
  }
  
  @Test
  public void getGlobalAnnotations() throws Exception {
    setupStorage(false);
    List<Annotation> notes = Annotation.getGlobalAnnotations(tsdb, 1328140000, 
        1328141000).joinUninterruptibly();
    assertNotNull(notes);
    assertEquals(2, notes.size());
    Annotation note0 = notes.get(0);
    Annotation note1 = notes.get(1);
    assertEquals("Description", note0.getDescription());
    assertEquals("Global 2", note1.getDescription());
  }

  @Test
  public void getGlobalAnnotationsSalted() throws Exception {
    setupStorage(true);
    List<Annotation> notes = Annotation.getGlobalAnnotations(tsdb, 1328140000, 
        1328141000).joinUninterruptibly();
    assertNotNull(notes);
    assertEquals(2, notes.size());
    Annotation note0 = notes.get(0);
    Annotation note1 = notes.get(1);
    assertEquals("Description", note0.getDescription());
    assertEquals("Global 2", note1.getDescription());
  }
  
  @Test
  public void getGlobalAnnotationOutsideCurrentHour() throws Exception {
    setupStorage(false);
    // 1425716000 - Sat Mar  7 00:13:20 PST 2015
    storage.addColumn(global_row_key_2015_midnight,
        new byte[] { 1, 3, (byte) 0x20 },
        ("{\"startTime\":1425716000,\"endTime\":1425716001,\"description\":" +
            "\"Global 3\",\"notes\":\"Issue #457\"}").getBytes(MockBase.ASCII()));

    int right_now = 1425717000;  // Sat Mar  7 00:30:00 PST 2015
    int fourty_minutes_ago = 1425714600; // Fri Mar  6 23:50:00 PST 2015

    List<Annotation> recentGlobalAnnotation = Annotation.getGlobalAnnotations(
        tsdb, fourty_minutes_ago, right_now).joinUninterruptibly();
    assertNotNull(recentGlobalAnnotation);
    assertEquals(1, recentGlobalAnnotation.size());
    Annotation note0 = recentGlobalAnnotation.get(0);
    assertEquals("Global 3", note0.getDescription());
    assertEquals("Issue #457", note0.getNotes());
  }

  @Test
  public void getGlobalAnnotationOutsideCurrentHourSalt() throws Exception {
    setupStorage(true);
    // 1425716000 - Sat Mar  7 00:13:20 PST 2015
    storage.addColumn(global_row_key_2015_midnight,
        new byte[] { 1, 3, (byte) 0x20 },
        ("{\"startTime\":1425716000,\"endTime\":1425716001,\"description\":" +
            "\"Global 3\",\"notes\":\"Issue #457\"}").getBytes(MockBase.ASCII()));

    int right_now = 1425717000;  // Sat Mar  7 00:30:00 PST 2015
    int fourty_minutes_ago = 1425714600; // Fri Mar  6 23:50:00 PST 2015

    List<Annotation> recentGlobalAnnotation = Annotation.getGlobalAnnotations(
        tsdb, fourty_minutes_ago, right_now).joinUninterruptibly();
    assertNotNull(recentGlobalAnnotation);
    assertEquals(1, recentGlobalAnnotation.size());
    Annotation note0 = recentGlobalAnnotation.get(0);
    assertEquals("Global 3", note0.getDescription());
    assertEquals("Issue #457", note0.getNotes());
  }
  
  @Test
  public void getGlobalAnnotationsEmpty() throws Exception {
    setupStorage(false);
    List<Annotation> notes = Annotation.getGlobalAnnotations(tsdb, 1328150000, 
        1328160000).joinUninterruptibly();
    assertNotNull(notes);
    assertEquals(0, notes.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getGlobalAnnotationsZeroEndtime() throws Exception {
    Annotation.getGlobalAnnotations(tsdb, 0, 0).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getGlobalAnnotationsEndLessThanStart() throws Exception {
    Annotation.getGlobalAnnotations(tsdb, 1328150000, 1328140000).joinUninterruptibly();
  }
  
  @Test
  public void syncToStorage() throws Exception {
    setupStorage(false);
    note.setTSUID(TSUID);
    note.setStartTime(1388450562L);
    note.setDescription("Synced!");
    note.syncToStorage(tsdb, false).joinUninterruptibly();
    final byte[] col = storage.getColumn(tsuid_row_key,
        new byte[] { 1, 0x0A, 0x02 });
    note = JSON.parseToObject(col, Annotation.class);
    assertEquals(TSUID, note.getTSUID());
    assertEquals("Synced!", note.getDescription());
    assertEquals("My Notes", note.getNotes());
  }
  
  @Test
  public void syncToStorageSalted() throws Exception {
    setupStorage(true);
    note.setTSUID(TSUID);
    note.setStartTime(1388450562L);
    note.setDescription("Synced!");
    note.syncToStorage(tsdb, false).joinUninterruptibly();
    final byte[] col = storage.getColumn(tsuid_row_key,
        new byte[] { 1, 0x0A, 0x02 });
    note = JSON.parseToObject(col, Annotation.class);
    assertEquals(TSUID, note.getTSUID());
    assertEquals("Synced!", note.getDescription());
    assertEquals("My Notes", note.getNotes());
  }
  
  @Test
  public void syncToStorageMilliseconds() throws Exception {
    setupStorage(false);
    note.setTSUID(TSUID);
    note.setStartTime(1388450562500L);
    note.setDescription("Synced!");
    note.syncToStorage(tsdb, false).joinUninterruptibly();
    final byte[] col = storage.getColumn(tsuid_row_key,
        new byte[] { 1, 0x00, 0x27, 0x19, (byte) 0xC4 });
    note = JSON.parseToObject(col, Annotation.class);
    assertEquals(TSUID, note.getTSUID());
    assertEquals("Synced!", note.getDescription());
    assertEquals("", note.getNotes());
    assertEquals(1388450562500L, note.getStartTime());
  }
  
  @Test
  public void syncToStorageGlobal() throws Exception {
    setupStorage(false);
    note.setStartTime(1328140800L);
    note.setDescription("Synced!");
    note.syncToStorage(tsdb, false).joinUninterruptibly();
    final byte[] col = storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 0 });
    note = JSON.parseToObject(col, Annotation.class);
    assertEquals("", note.getTSUID());
    assertEquals("Synced!", note.getDescription());
    assertEquals("Notes", note.getNotes());
  }
  
  @Test
  public void syncToStorageGlobalSalted() throws Exception {
    setupStorage(true);
    note.setStartTime(1328140800L);
    note.setDescription("Synced!");
    note.syncToStorage(tsdb, false).joinUninterruptibly();
    final byte[] col = storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 0 });
    note = JSON.parseToObject(col, Annotation.class);
    assertEquals("", note.getTSUID());
    assertEquals("Synced!", note.getDescription());
    assertEquals("Notes", note.getNotes());
  }
  
  @Test
  public void syncToStorageGlobalMilliseconds() throws Exception {
    setupStorage(false);
    note.setStartTime(1328140800500L);
    note.setDescription("Synced!");
    note.syncToStorage(tsdb, false).joinUninterruptibly();
    final byte[] col = storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 0, 1, (byte) 0xF4 });
    note = JSON.parseToObject(col, Annotation.class);
    assertEquals("", note.getTSUID());
    assertEquals("Synced!", note.getDescription());
    assertEquals("", note.getNotes());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageMissingStart() throws Exception {
    note.setTSUID(TSUID);
    note.setDescription("Synced!");
    note.syncToStorage(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalStateException.class)
  public void syncToStorageNoChanges() throws Exception {
    note.setTSUID(TSUID);
    note.setStartTime(1388450562L);
    note.syncToStorage(tsdb, false).joinUninterruptibly();
  }
  
  @Test
  public void delete() throws Exception {
    setupStorage(false);
    note.setTSUID(TSUID);
    note.setStartTime(1388450562);
    note.delete(tsdb).joinUninterruptibly();
    assertNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x02 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x03 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x10 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x18 }));
  }
  
  @Test
  public void deleteSalted() throws Exception {
    setupStorage(true);
    note.setTSUID(TSUID);
    note.setStartTime(1388450562);
    note.delete(tsdb).joinUninterruptibly();
    assertNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x02 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x03 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x10 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x18 }));
  }
  
  @Test
  public void deleteNormalizeMs() throws Exception {
    setupStorage(false);
    note.setTSUID(TSUID);
    note.setStartTime(1388450562000L);
    note.delete(tsdb).joinUninterruptibly();
    assertNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x02 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x03 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x10 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x18 }));
  }
  
  // this doesn't throw an error or anything, just issues the delete request
  // and it's ignored.
  @Test
  public void deleteNotFound() throws Exception {
    setupStorage(false);
    note.setTSUID(TSUID);
    note.setStartTime(1388450561);
    note.delete(tsdb).joinUninterruptibly();
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x02 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x03 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x10 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x18 }));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void deleteMissingStart() throws Exception {
    note.setTSUID(TSUID);
    note.delete(tsdb).joinUninterruptibly();
  }
  
  @Test
  public void deleteGlobal() throws Exception {
    setupStorage(false);
    note.setStartTime(1328140800);
    note.delete(tsdb).joinUninterruptibly();
    assertNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 0 }));
    assertNotNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 1 }));
  }
  
  @Test
  public void deleteGlobalSalted() throws Exception {
    setupStorage(true);
    note.setStartTime(1328140800);
    note.delete(tsdb).joinUninterruptibly();
    assertNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 0 }));
    assertNotNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 1 }));
  }
  
  @Test
  public void deleteGlobalNotFound() throws Exception {
    setupStorage(false);
    note.setStartTime(1328140803);
    note.delete(tsdb).joinUninterruptibly();
    assertNotNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 0 }));
    assertNotNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 1 }));
  }
  
  @Test
  public void deleteRange() throws Exception {
    setupStorage(false);
    final int count = Annotation.deleteRange(tsdb, 
        new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1}, 1388450560000L, 
        1388450562000L).joinUninterruptibly();
    assertEquals(1, count);
    assertNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x02 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x03 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x10 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x18 }));
  }
  
  @Test
  public void deleteRangeSalted() throws Exception {
    setupStorage(true);
    final int count = Annotation.deleteRange(tsdb, 
        new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1}, 1388450560000L, 
        1388450562000L).joinUninterruptibly();
    assertEquals(1, count);
    assertNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x02 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x03 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x10 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x18 }));
  }
  
  @Test
  public void deleteRangeNone() throws Exception {
    setupStorage(false);
    final int count = Annotation.deleteRange(tsdb, 
        new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1}, 1388450560000L, 
        1388450561000L).joinUninterruptibly();
    assertEquals(0, count);
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x02 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x03 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x10 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x18 }));
  }
  
  @Test
  public void deleteRangeNoneSalted() throws Exception {
    setupStorage(true);
    final int count = Annotation.deleteRange(tsdb, 
        new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1}, 1388450560000L, 
        1388450561000L).joinUninterruptibly();
    assertEquals(0, count);
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x02 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x03 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x10 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x18 }));
  }
  
  @Test
  public void deleteRangeMultiple() throws Exception {
    setupStorage(false);
    final int count = Annotation.deleteRange(tsdb, 
        new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1}, 1388450560000L, 
        1388450568000L).joinUninterruptibly();
    assertEquals(2, count);
    assertNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x02 }));
    assertNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x03 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x10 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x18 }));
  }
  
  @Test
  public void deleteRangeMultipleSalted() throws Exception {
    setupStorage(true);
    final int count = Annotation.deleteRange(tsdb, 
        new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1}, 1388450560000L, 
        1388450568000L).joinUninterruptibly();
    assertEquals(2, count);
    assertNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x02 }));
    assertNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x03 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x10 }));
    assertNotNull(storage.getColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x18 }));
  }
  
  @Test
  public void deleteRangeGlobal() throws Exception {
    setupStorage(false);
    final int count = Annotation.deleteRange(tsdb, null, 1328140799000L, 
        1328140800000L).joinUninterruptibly();
    assertEquals(1, count);
    assertNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 0 }));
    assertNotNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 1 }));
  }
  
  @Test
  public void deleteRangeGlobalSalted() throws Exception {
    setupStorage(true);
    final int count = Annotation.deleteRange(tsdb, null, 1328140799000L, 
        1328140800000L).joinUninterruptibly();
    assertEquals(1, count);
    assertNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 0 }));
    assertNotNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 1 }));
  }
  
  @Test
  public void deleteRangeGlobalNone() throws Exception {
    setupStorage(false);
    final int count = Annotation.deleteRange(tsdb, null, 1328140798000L, 
        1328140799000L).joinUninterruptibly();
    assertEquals(0, count);
    assertNotNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 0 }));
    assertNotNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 1 }));
  }
  
  @Test
  public void deleteRangeGlobalMultiple() throws Exception {
    setupStorage(false);
    final int count = Annotation.deleteRange(tsdb, null, 1328140799000L, 
        1328140900000L).joinUninterruptibly();
    assertEquals(2, count);
    assertNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 0 }));
    assertNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 1 }));
  }
  
  @Test
  public void deleteRangeGlobalMultipleSalted() throws Exception {
    setupStorage(true);
    final int count = Annotation.deleteRange(tsdb, null, 1328140799000L, 
        1328140900000L).joinUninterruptibly();
    assertEquals(2, count);
    assertNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 0 }));
    assertNull(storage.getColumn(global_row_key, 
        new byte[] { 1, 0, 1 }));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void deleteRangeEmptyEnd() throws Exception {
    Annotation.deleteRange(tsdb, null, 1328140799000L, 0).joinUninterruptibly();
  }

  @Test (expected = IllegalArgumentException.class)
  public void deleteRangeEndLessThanStart() throws Exception {
    Annotation.deleteRange(tsdb, null, 1328140799000L, 1328140798000L)
      .joinUninterruptibly();
  }

  /**
   * Sets up storage with or without salting and writes a few bits of data
   * @param salted Whether or not to use salting
   */
  private void setupStorage(final boolean salted) {
    if (salted) {
      PowerMockito.mockStatic(Const.class);
      PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
      PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(2);
    }
    
    global_row_key = new byte[Const.SALT_WIDTH() + TSDB.metrics_width() + 
                              Const.TIMESTAMP_BYTES];
    System.arraycopy(Bytes.fromInt(1328140800), 0, global_row_key, 
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    
    global_row_key_2015_midnight = 
        new byte[Const.SALT_WIDTH() + TSDB.metrics_width() + 
                 Const.TIMESTAMP_BYTES];
    System.arraycopy(Bytes.fromInt(1425715200), 0, global_row_key_2015_midnight, 
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    
    tsuid_row_key = getRowKeyTemplate();
    System.arraycopy(Bytes.fromInt(1388448000), 0, tsuid_row_key, 
        Const.SALT_WIDTH() + TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
    RowKey.prefixKeyWithSalt(tsuid_row_key);

    storage = new MockBase(tsdb, client, true, true, true, true);
    
    // add a global
    storage.addColumn(global_row_key, 
        new byte[] { 1, 0, 0 }, 
        ("{\"startTime\":1328140800,\"endTime\":1328140801,\"description\":" + 
            "\"Description\",\"notes\":\"Notes\",\"custom\":{\"owner\":" + 
            "\"ops\"}}").getBytes(MockBase.ASCII()));

    storage.addColumn(global_row_key, 
        new byte[] { 1, 0, 1 }, 
        ("{\"startTime\":1328140801,\"endTime\":1328140803,\"description\":" + 
            "\"Global 2\",\"notes\":\"Nothing\"}").getBytes(MockBase.ASCII()));

    // add a local
    storage.addColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x02 }, 
        ("{\"tsuid\":\"000001000001000001\",\"startTime\":1388450562," +
            "\"endTime\":1419984000,\"description\":\"Hello!\",\"notes\":" + 
            "\"My Notes\",\"custom\":{\"owner\":\"ops\"}}")
            .getBytes(MockBase.ASCII()));
    
    storage.addColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x03 }, 
        ("{\"tsuid\":\"000001000001000001\",\"startTime\":1388450563," +
            "\"endTime\":1419984000,\"description\":\"Note2\",\"notes\":" + 
            "\"Nothing\"}")
            .getBytes(MockBase.ASCII()));
    
    // add some data points too
    storage.addColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x10 }, new byte[] { 1 });
    
    storage.addColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x18 }, new byte[] { 2 });
  }
}
