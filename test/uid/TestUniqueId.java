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
import java.util.SortedMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.EventBus;
import com.stumbleupon.async.Callback;

import dagger.ObjectGraph;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TsdbStore;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.mockito.ArgumentMatcher;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public final class TestUniqueId {
  private TsdbStore client;
  private TSDB tsdb;
  private static final byte[] table = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] ID = { 'i', 'd' };
  private UniqueId uid;
  private static final String kind = "metrics";
  private static final byte[] kind_array = { 'm', 'e', 't', 'r', 'i', 'c' };
  private Metrics metrics;
  private MetricRegistry registry;
  private EventBus idEventBus;

  @Before
  public void setUp() throws IOException{
    ObjectGraph objectGraph = ObjectGraph.create(new TestModuleMemoryStore());
    client = objectGraph.get(TsdbStore.class);
    tsdb = objectGraph.get(TSDB.class);

    registry = new MetricRegistry();
    metrics = new Metrics(registry);
    idEventBus = mock(EventBus.class);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoTsdbStore() {
    uid = new UniqueId(null, table, UniqueIdType.METRIC, metrics, idEventBus);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoTable() {
    uid = new UniqueId(client, null, UniqueIdType.METRIC, metrics, idEventBus);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoType() {
    uid = new UniqueId(client, table, null, metrics, idEventBus);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoEventbus() {
    uid = new UniqueId(client, table, UniqueIdType.METRIC, metrics, null);
  }

  @Test
  public void typeEqual() {
    uid = new UniqueId(client, table, UniqueIdType.METRIC, metrics, idEventBus);
    assertEquals(UniqueIdType.METRIC, uid.type());
  }

  @Test
  public void widthEqual() {
    uid = new UniqueId(client, table, UniqueIdType.METRIC, metrics, idEventBus);
    assertEquals(3, uid.width());
  }
  
  @Test
  public void getNameSuccessfulLookup() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC, metrics, idEventBus);

    final byte[] id = { 0, 'a', 0x42 };
    client.allocateUID("foo", id, UniqueIdType.METRIC);

    assertEquals("foo", uid.getName(id).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    // Should be a cache hit ...
    assertEquals("foo", uid.getName(id).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    final SortedMap<String, Counter> counters = registry.getCounters();
    assertEquals(1, counters.get("uid.cache-hit:kind=metrics").getCount());
    assertEquals(1, counters.get("uid.cache-miss:kind=metrics").getCount());
    assertEquals(2, registry.getGauges().get("uid.cache-size:kind=metrics").getValue());
  }

  @Test(expected=NoSuchUniqueId.class)
  public void getNameForNonexistentId() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC, metrics, idEventBus);
    uid.getName(new byte[]{1, 2, 3}).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test(expected=IllegalArgumentException.class)
  public void getNameWithInvalidId() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC, metrics, idEventBus);
    uid.getName(new byte[]{1}).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void getIdSuccessfulLookup() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC, metrics, idEventBus);

    final byte[] id = { 0, 'a', 0x42 };
    client.allocateUID("foo", id, UniqueIdType.METRIC);

    assertArrayEquals(id, uid.getId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    // Should be a cache hit ...
    assertArrayEquals(id, uid.getId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    // Should be a cache hit too ...
    assertArrayEquals(id, uid.getId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    final SortedMap<String, Counter> counters = registry.getCounters();
    assertEquals(2, counters.get("uid.cache-hit:kind=metrics").getCount());
    assertEquals(1, counters.get("uid.cache-miss:kind=metrics").getCount());
    assertEquals(2, registry.getGauges().get("uid.cache-size:kind=metrics").getValue());
  }

  // The table contains IDs encoded on 2 bytes but the instance wants 3.
  @Test(expected=IllegalStateException.class)
  public void getIdMisconfiguredWidth() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC, metrics, idEventBus);

    final byte[] id = { 'a', 0x42 };
    client.allocateUID("foo", id, UniqueIdType.METRIC);

    uid.getId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test(expected=NoSuchUniqueName.class)
  public void getIdForNonexistentName() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC, metrics, idEventBus);
    uid.getId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void createIdWithExistingId() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC, metrics, idEventBus);

    final byte[] id = { 0, 0, 1};
    uid.createId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    try {
      uid.createId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    } catch(Exception e) {
      assertEquals("A UID with name foo already exists", e.getMessage());
    }
    // Should be a cache hit ...
    assertArrayEquals(id, uid.getId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    final SortedMap<String, Counter> counters = registry.getCounters();
    assertEquals(1, counters.get("uid.cache-hit:kind=metrics").getCount());
    assertEquals(2, registry.getGauges().get("uid.cache-size:kind=metrics").getValue());
  }

  @Test  // Test the creation of an ID with no problem.
  public void createIdIdWithSuccess() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC, metrics, idEventBus);
    // Due to the implementation in the memoryStore used for testing the first
    // call will always return 1
    final byte[] id = { 0, 0, 1 };

    assertArrayEquals(id, uid.createId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    // Should be a cache hit since we created that entry.
    assertArrayEquals(id, uid.getId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    // Should be a cache hit too for the same reason.
    assertEquals("foo", uid.getName(id).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    final SortedMap<String, Counter> counters = registry.getCounters();
    assertEquals(2, counters.get("uid.cache-hit:kind=metrics").getCount());
    assertEquals(0, counters.get("uid.cache-miss:kind=metrics").getCount());
  }

  @Test
  public void createIdPublishesEventOnSuccess() throws Exception {
    uid = new UniqueId(client, table, UniqueIdType.METRIC, metrics, idEventBus);
    uid.createId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    verify(idEventBus).post(any(IdCreatedEvent.class));
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

  public void uidToLong() {
    assertEquals(42, UniqueId.uidToLong(new byte[] { 0, 0, 0x2A }, (short)3));
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
      @Override
      public boolean matches(Object incr) {
        return Arrays.equals(((AtomicIncrementRequest) incr).key(), row);
      }
      @Override
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
      @Override
      public boolean matches(Object put) {
        return Arrays.equals(((PutRequest) put).key(), row);
      }
      @Override
      public void describeTo(org.hamcrest.Description description) {
        description.appendText("PutRequest for row " + Arrays.toString(row));
      }
    });
  }

  private static final byte[] MAXID = { 0 };

}
