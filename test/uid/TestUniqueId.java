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
package net.opentsdb.uid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.hbase.HBaseStore;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.utils.Config;

import org.hbase.async.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static net.opentsdb.uid.UniqueId.UniqueIdType;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(PowerMockRunner.class)
// "Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
// because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
public final class TestUniqueId {
  private TsdbStore client;
  private static final byte[] table = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] ID = { 'i', 'd' };
  private UniqueId uid;
  private static final String kind = "metrics";
  private static final byte[] kind_array = { 'm', 'e', 't', 'r', 'i', 'c' };
  private Config config;

  @Before
  public void setUp() throws IOException{
    client = new MemoryStore();
    config = new Config(false);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoTsdbStore() {
    uid = new UniqueId(null, table, UniqueIdType.METRIC);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoTable() {
    uid = new UniqueId(client, null, UniqueIdType.METRIC);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoType() {
    uid = new UniqueId(client, table, null);
  }

  @Test
  public void kindEqual() {
    uid = new UniqueId(client, table, UniqueIdType.METRIC);
    assertEquals(kind, uid.kind());
  }

  @Test
  public void widthEqual() {
    uid = new UniqueId(client, table, UniqueIdType.METRIC);
    assertEquals(3, uid.width());
  }

  @Test
  public void testMaxPossibleId() {
    assertEquals(16777215L, (new UniqueId(client, table, UniqueIdType.METRIC)).maxPossibleId());
    assertEquals(16777215L, (new UniqueId(client, table, UniqueIdType.TAGK)).maxPossibleId());
    assertEquals(16777215L, (new UniqueId(client, table, UniqueIdType.TAGV)).maxPossibleId());
  } 
  
  @Test
  public void getNameSuccessfulLookup() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC);

    final byte[] id = { 0, 'a', 0x42 };
    client.allocateUID("foo", id, UniqueIdType.METRIC);

    assertEquals("foo", uid.getNameAsync(id).joinUninterruptibly());
    // Should be a cache hit ...
    assertEquals("foo", uid.getNameAsync(id).joinUninterruptibly());

    assertEquals(1, uid.cacheHits());
    assertEquals(1, uid.cacheMisses());
    assertEquals(2, uid.cacheSize());
  }

  @Test(expected=NoSuchUniqueId.class)
  public void getNameForNonexistentId() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC);
    uid.getNameAsync(new byte[] { 1, 2, 3 }).joinUninterruptibly();
  }

  @Test(expected=IllegalArgumentException.class)
  public void getNameWithInvalidId() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC);
    uid.getNameAsync(new byte[] { 1 }).joinUninterruptibly();
  }

  @Test
  public void getIdSuccessfulLookup() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC);

    final byte[] id = { 0, 'a', 0x42 };
    client.allocateUID("foo", id, UniqueIdType.METRIC);

    assertArrayEquals(id, uid.getIdAsync("foo").joinUninterruptibly());
    // Should be a cache hit ...
    assertArrayEquals(id, uid.getIdAsync("foo").joinUninterruptibly());
    // Should be a cache hit too ...
    assertArrayEquals(id, uid.getIdAsync("foo").joinUninterruptibly());

    assertEquals(2, uid.cacheHits());
    assertEquals(1, uid.cacheMisses());
    assertEquals(2, uid.cacheSize());
  }

  // The table contains IDs encoded on 2 bytes but the instance wants 3.
  @Test(expected=IllegalStateException.class)
  public void getIdMisconfiguredWidth() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC);

    final byte[] id = { 'a', 0x42 };
    client.allocateUID("foo", id, UniqueIdType.METRIC);

    uid.getIdAsync("foo").joinUninterruptibly();
  }

  @Test(expected=NoSuchUniqueName.class)
  public void getIdForNonexistentName() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC);
    uid.getIdAsync("foo").joinUninterruptibly();
  }

  @Test
  public void createIdWithExistingId() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC);

    final byte[] id = { 0, 0, 1};
    uid.createId("foo").joinUninterruptibly();

    try {
      uid.createId("foo").joinUninterruptibly();
    } catch(Exception e) {
      assertEquals("A UID with name foo already exists", e.getMessage());
    }
    // Should be a cache hit ...
    assertArrayEquals(id, uid.getIdAsync("foo").joinUninterruptibly());
    assertEquals(1, uid.cacheHits());
    assertEquals(2, uid.cacheSize());
  }

  @Test  // Test the creation of an ID with no problem.
  public void createIdIdWithSuccess() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC);
    // Due to the implementation in the memoryStore used for testing the first
    // call will always return 1
    final byte[] id = { 0, 0, 1 };

    assertArrayEquals(id, uid.createId("foo").joinUninterruptibly());

    // Should be a cache hit since we created that entry.
    assertArrayEquals(id, uid.getIdAsync("foo").joinUninterruptibly());
    // Should be a cache hit too for the same reason.
    assertEquals("foo", uid.getNameAsync(id).joinUninterruptibly());

    assertEquals(2, uid.cacheHits());
    assertEquals(0, uid.cacheMisses());
  }

  @PrepareForTest({HBaseStore.class, Scanner.class})
  @Test
  public void suggestWithNoMatch() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC);


    // Watch this! ______,^   I'm writing C++ in Java!

    final List<String> suggestions = uid.suggest("nomatch").joinUninterruptibly();
    assertEquals(0, suggestions.size());  // No results.

    //verify(fake_scanner).setStartKey("nomatch".getBytes());
    //verify(fake_scanner).setStopKey("nomatci".getBytes());
    //verify(fake_scanner).setFamily(ID);
    //verify(fake_scanner).setQualifier(kind_array);
  }

  @PrepareForTest({Scanner.class})
  @Test
  public void suggestWithMatches() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC);



    final ArrayList<ArrayList<KeyValue>> rows = new ArrayList<ArrayList<KeyValue>>(2);
    final byte[] foo_bar_id = { 0, 0, 1 };
    {
      ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
      row.add(new KeyValue("foo.bar".getBytes(), ID, kind_array, foo_bar_id));
      rows.add(row);
      row = new ArrayList<KeyValue>(1);
      row.add(new KeyValue("foo.baz".getBytes(), ID, kind_array,
                           new byte[] { 0, 0, 2 }));
      rows.add(row);
    }
    //when(fake_scanner.nextRows())
    //  .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(rows))
    //  .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    final List<String> suggestions = uid.suggest("foo").joinUninterruptibly();
    final ArrayList<String> expected = new ArrayList<String>(2);
    expected.add("foo.bar");
    expected.add("foo.baz");
    assertEquals(expected, suggestions);
    // Verify that we cached the forward + backwards mapping for both results
    // we "discovered" as a result of the scan.
    assertEquals(4, uid.cacheSize());
    assertEquals(0, uid.cacheHits());

    // Verify that the cached results are usable.
    // Should be a cache hit ...
    assertArrayEquals(foo_bar_id, uid.getIdAsync("foo.bar").joinUninterruptibly());
    assertEquals(1, uid.cacheHits());
    // ... so verify there was no HBase Get.
    verify(client, never()).get(anyGet());
  }

  @Test
  public void uidToString() {
    assertEquals("01", UniqueId.uidToString(new byte[] { 1 }));
  }
  
  @Test
  public void uidToString2() {
    assertEquals("0A0B", UniqueId.uidToString(new byte[] { 10, 11 }));
  }
  
  @Test
  public void uidToString3() {
    assertEquals("1A1B", UniqueId.uidToString(new byte[] { 26, 27 }));
  }
  
  @Test
  public void uidToStringZeros() {
    assertEquals("00", UniqueId.uidToString(new byte[] { 0 }));
  }
  
  @Test
  public void uidToString255() {
    assertEquals("FF", UniqueId.uidToString(new byte[] { (byte) 255 }));
  }
  
  @Test (expected = NullPointerException.class)
  public void uidToStringNull() {
    UniqueId.uidToString(null);
  }
  
  @Test
  public void stringToUid() {
    assertArrayEquals(new byte[] { 0x0a, 0x0b }, UniqueId.stringToUid("0A0B"));
  }
  
  @Test
  public void stringToUidNormalize() {
    assertArrayEquals(new byte[] { (byte) 171 }, UniqueId.stringToUid("AB"));
  }
  
  @Test
  public void stringToUidCase() {
    assertArrayEquals(new byte[] { (byte) 11 }, UniqueId.stringToUid("B"));
  }
  
  @Test
  public void stringToUidWidth() {
    assertArrayEquals(new byte[] { (byte) 0, (byte) 42, (byte) 12 }, 
        UniqueId.stringToUid("2A0C", (short)3));
  }
  
  @Test
  public void stringToUidWidth2() {
    assertArrayEquals(new byte[] { (byte) 0, (byte) 0, (byte) 0 }, 
        UniqueId.stringToUid("0", (short)3));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToUidNull() {
    UniqueId.stringToUid(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToUidEmpty() {
    UniqueId.stringToUid("");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToUidNotHex() {
    UniqueId.stringToUid("HelloWorld");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToUidNotHex2() {
    UniqueId.stringToUid(" ");
  }
  
  @Test
  public void getTSUIDFromKey() {
    final byte[] tsuid = UniqueId.getTSUIDFromKey(new byte[] 
      { 0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3 }, (short)3, (short)4);
    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 2, 0, 0, 3 }, 
        tsuid);
  }
  
  @Test
  public void getTSUIDFromKeyMissingTags() {
    final byte[] tsuid = UniqueId.getTSUIDFromKey(new byte[] 
      { 0, 0, 1, 1, 1, 1, 1 }, (short)3, (short)4);
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        tsuid);
  }
  
  @Test
  public void getTagPairsFromTSUIDBytes() {
    List<byte[]> tags = UniqueId.getTagPairsFromTSUID(
        new byte[] { 0, 0, 0, 0, 0, 1, 0, 0, 2, 0, 0, 3, 0, 0, 4 });
    assertNotNull(tags);
    assertEquals(2, tags.size());
    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 2 }, tags.get(0));
    assertArrayEquals(new byte[] { 0, 0, 3, 0, 0, 4 }, tags.get(1));
  }
  
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDBytesNonStandardWidth() {
    //PowerMockito.mockStatic(TSDB.class);
    //when(Const.METRICS_WIDTH).thenReturn(eq((short)3));
    //when(Const.TAG_NAME_WIDTH).thenReturn(eq((short)4));
    //when(Const.TAG_VALUE_WIDTH).thenReturn(eq((short)3));
    
    List<byte[]> tags = UniqueId.getTagPairsFromTSUID(
        new byte[] { 0, 0, 0, 0, 0, 0, 1, 0, 0, 2, 0, 0, 0, 3, 0, 0, 4 });
    assertNotNull(tags);
    assertEquals(2, tags.size());
    assertArrayEquals(new byte[] { 0, 0, 0, 1, 0, 0, 2 }, tags.get(0));
    assertArrayEquals(new byte[] { 0, 0, 0, 3, 0, 0, 4 }, tags.get(1));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDBytesMissingTags() {
    UniqueId.getTagPairsFromTSUID(new byte[] { 0, 0, 1 });
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDBytesMissingMetric() {
    UniqueId.getTagPairsFromTSUID(new byte[] { 0, 0, 1, 0, 0, 2 });
  }

  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDBytesMissingTagv() {
    UniqueId.getTagPairsFromTSUID(new byte[] { 0, 0, 8, 0, 0, 2 });
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDBytesNull() {
    UniqueId.getTagPairsFromTSUID((byte[])null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDBytesEmpty() {
    UniqueId.getTagPairsFromTSUID(new byte[0]);
  }
  
  @Test
  public void getTagFromTSUID() {
    List<byte[]> tags = UniqueId.getTagsFromTSUID(
        "000000000001000002000003000004");
    assertNotNull(tags);
    assertEquals(4, tags.size());
    assertArrayEquals(new byte[] { 0, 0, 1 }, tags.get(0));
    assertArrayEquals(new byte[] { 0, 0, 2 }, tags.get(1));
    assertArrayEquals(new byte[] { 0, 0, 3 }, tags.get(2));
    assertArrayEquals(new byte[] { 0, 0, 4 }, tags.get(3));
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void getTagFromTSUIDNonStandardWidth() {
    List<byte[]> tags = UniqueId.getTagsFromTSUID(
        "0000000000000100000200000003000004");
    assertNotNull(tags);
    assertEquals(4, tags.size());
    assertArrayEquals(new byte[] { 0, 0, 0, 1 }, tags.get(0));
    assertArrayEquals(new byte[] { 0, 0, 2 }, tags.get(1));
    assertArrayEquals(new byte[] { 0, 0, 0, 3 }, tags.get(2));
    assertArrayEquals(new byte[] { 0, 0, 4 }, tags.get(3));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagFromTSUIDMissingTags() {
    UniqueId.getTagsFromTSUID("123456");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagFromTSUIDMissingMetric() {
    UniqueId.getTagsFromTSUID("000001000002");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagFromTSUIDOddNumberOfCharacters() {
    UniqueId.getTagsFromTSUID("0000080000010000020");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagFromTSUIDMissingTagv() {
    UniqueId.getTagsFromTSUID("000008000001");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagFromTSUIDNull() {
    UniqueId.getTagsFromTSUID(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagFromTSUIDEmpty() {
    UniqueId.getTagsFromTSUID("");
  }
  
  @Test
  public void getUsedUIDs() throws Exception {
    final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(3);
    final byte[] metrics = { 'm', 'e', 't', 'r', 'i', 'c', 's' };
    final byte[] tagk = { 't', 'a', 'g', 'k' };
    final byte[] tagv = { 't', 'a', 'g', 'v' };
    kvs.add(new KeyValue(MAXID, ID, metrics, Bytes.fromLong(64L)));
    kvs.add(new KeyValue(MAXID, ID, tagk, Bytes.fromLong(42L)));
    kvs.add(new KeyValue(MAXID, ID, tagv, Bytes.fromLong(1024L)));
    TSDB tsdb = new TSDB(client, config);

    
    final byte[][] kinds = { metrics, tagk, tagv };
    final Map<String, Long> uids = UniqueId.getUsedUIDs(tsdb, kinds)
      .joinUninterruptibly();
    assertNotNull(uids);
    assertEquals(3, uids.size());
    assertEquals(64L, uids.get("metrics").longValue());
    assertEquals(42L, uids.get("tagk").longValue());
    assertEquals(1024L, uids.get("tagv").longValue());
  }
  
  @Test
  public void getUsedUIDsEmptyRow() throws Exception {
    final byte[] metrics = { 'm', 'e', 't', 'r', 'i', 'c', 's' };
    final byte[] tagk = { 't', 'a', 'g', 'k' };
    final byte[] tagv = { 't', 'a', 'g', 'v' };
    TSDB tsdb = new TSDB(client, config);
    
    final byte[][] kinds = { metrics, tagk, tagv };
    final Map<String, Long> uids = UniqueId.getUsedUIDs(tsdb, kinds)
      .joinUninterruptibly();
    assertNotNull(uids);
    assertEquals(3, uids.size());
    assertEquals(0L, uids.get("metrics").longValue());
    assertEquals(0L, uids.get("tagk").longValue());
    assertEquals(0L, uids.get("tagv").longValue());
  }
  
  @Test
  public void uidToLong() throws Exception {
    assertEquals(42, UniqueId.uidToLong(new byte[] { 0, 0, 0x2A }, (short)3));
  }

  @Test
  public void uidToLongFromString() throws Exception {
    assertEquals(42L, UniqueId.uidToLong("00002A", (short) 3));
  }

  @Test (expected = IllegalArgumentException.class)
  public void uidToLongTooLong() throws Exception {
    UniqueId.uidToLong(new byte[] { 0, 0, 0, 0x2A }, (short)3);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void uidToLongTooShort() throws Exception {
    UniqueId.uidToLong(new byte[] { 0, 0x2A }, (short)3);
  }
  
  @Test (expected = NullPointerException.class)
  public void uidToLongNull() throws Exception {
    UniqueId.uidToLong((byte[])null, (short)3);
  }
  
  @Test
  public void longToUID() throws Exception {
    assertArrayEquals(new byte[] { 0, 0, 0x2A }, 
        UniqueId.longToUID(42L, (short)3));
  }
  
  @Test (expected = IllegalStateException.class)
  public void longToUIDTooBig() throws Exception {
    UniqueId.longToUID(257, (short)1);
  }

  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private static byte[] emptyArray() {
    return eq(HBaseClient.EMPTY_ARRAY);
  }

  private static GetRequest anyGet() {
    return any(GetRequest.class);
  }

  private static AtomicIncrementRequest incrementForRow(final byte[] row) {
    return argThat(new ArgumentMatcher<AtomicIncrementRequest>() {
      public boolean matches(Object incr) {
        return Arrays.equals(((AtomicIncrementRequest) incr).key(), row);
      }
      public void describeTo(org.hamcrest.Description description) {
        description.appendText("AtomicIncrementRequest for row "
                               + Arrays.toString(row));
      }
    });
  }

  private static PutRequest anyPut() {
    return any(PutRequest.class);
  }
  
  @SuppressWarnings("unchecked")
  private static Callback<byte[], ArrayList<KeyValue>> anyByteCB() {
    return any(Callback.class);
  }

  private static PutRequest putForRow(final byte[] row) {
    return argThat(new ArgumentMatcher<PutRequest>() {
      public boolean matches(Object put) {
        return Arrays.equals(((PutRequest) put).key(), row);
      }
      public void describeTo(org.hamcrest.Description description) {
        description.appendText("PutRequest for row " + Arrays.toString(row));
      }
    });
  }

  private static final byte[] MAXID = { 0 };

}
