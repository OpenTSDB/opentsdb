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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

import org.junit.Test;
import org.junit.runner.RunWith;
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
@PrepareForTest({ HBaseClient.class, TSDB.class, Config.class })
public final class TestUniqueId {

  private HBaseClient client = mock(HBaseClient.class);
  private static final byte[] table = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] ID = { 'i', 'd' };
  private UniqueId uid;
  private static final String kind = "metric";
  private static final byte[] kind_array = { 'm', 'e', 't', 'r', 'i', 'c' };

  @Test(expected=IllegalArgumentException.class)
  public void testCtorZeroWidth() {
    uid = new UniqueId(client, table, kind, 0);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCtorNegativeWidth() {
    uid = new UniqueId(client, table, kind, -1);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCtorEmptyKind() {
    uid = new UniqueId(client, table, "", 3);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCtorLargeWidth() {
    uid = new UniqueId(client, table, kind, 9);
  }

  @Test
  public void kindEqual() {
    uid = new UniqueId(client, table, kind, 3);
    assertEquals(kind, uid.kind());
  }

  @Test
  public void widthEqual() {
    uid = new UniqueId(client, table, kind, 3);
    assertEquals(3, uid.width());
  }

  @Test
  public void testMaxPossibleId() {
    assertEquals(255, (new UniqueId(client, table, kind, 1)).maxPossibleId());
    assertEquals(65535, (new UniqueId(client, table, kind, 2)).maxPossibleId());
    assertEquals(16777215L, (new UniqueId(client, table, kind, 3)).maxPossibleId());
  } 
  
  @Test
  public void getNameSuccessfulHBaseLookup() {
    uid = new UniqueId(client, table, kind, 3);
    final byte[] id = { 0, 'a', 0x42 };
    final byte[] byte_name = { 'f', 'o', 'o' };

    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(id, ID, kind_array, byte_name));
    when(client.get(anyGet()))
      .thenReturn(Deferred.fromResult(kvs));

    assertEquals("foo", uid.getName(id));
    // Should be a cache hit ...
    assertEquals("foo", uid.getName(id));

    assertEquals(1, uid.cacheHits());
    assertEquals(1, uid.cacheMisses());
    assertEquals(2, uid.cacheSize());

    // ... so verify there was only one HBase Get.
    verify(client).get(anyGet());
  }

  @Test
  public void getNameWithErrorDuringHBaseLookup() {
    uid = new UniqueId(client, table, kind, 3);
    final byte[] id = { 0, 'a', 0x42 };
    final byte[] byte_name = { 'f', 'o', 'o' };

    HBaseException hbe = mock(HBaseException.class);

    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(id, ID, kind_array, byte_name));
    when(client.get(anyGet()))
      .thenThrow(hbe)
      .thenReturn(Deferred.fromResult(kvs));

    // 1st calls fails.
    try {
      uid.getName(id);
      fail("HBaseException should have been thrown.");
    } catch (HBaseException e) {
      assertSame(hbe, e);  // OK.
    }

    // 2nd call succeeds.
    assertEquals("foo", uid.getName(id));

    assertEquals(0, uid.cacheHits());
    assertEquals(2, uid.cacheMisses());  // 1st (failed) attempt + 2nd.
    assertEquals(2, uid.cacheSize());

    verify(client, times(2)).get(anyGet());
  }

  @Test(expected=NoSuchUniqueId.class)
  public void getNameForNonexistentId() {
    uid = new UniqueId(client, table, kind, 3);

    when(client.get(anyGet()))
      .thenReturn(Deferred.fromResult(new ArrayList<KeyValue>(0)));

    uid.getName(new byte[] { 1, 2, 3 });
  }

  @Test(expected=IllegalArgumentException.class)
  public void getNameWithInvalidId() {
    uid = new UniqueId(client, table, kind, 3);

    uid.getName(new byte[] { 1 });
  }

  @Test
  public void getIdSuccessfulHBaseLookup() {
    uid = new UniqueId(client, table, kind, 3);
    final byte[] id = { 0, 'a', 0x42 };
    final byte[] byte_name = { 'f', 'o', 'o' };

    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(byte_name, ID, kind_array, id));
    when(client.get(anyGet()))
      .thenReturn(Deferred.fromResult(kvs));

    assertArrayEquals(id, uid.getId("foo"));
    // Should be a cache hit ...
    assertArrayEquals(id, uid.getId("foo"));
    // Should be a cache hit too ...
    assertArrayEquals(id, uid.getId("foo"));

    assertEquals(2, uid.cacheHits());
    assertEquals(1, uid.cacheMisses());
    assertEquals(2, uid.cacheSize());

    // ... so verify there was only one HBase Get.
    verify(client).get(anyGet());
  }

  // The table contains IDs encoded on 2 bytes but the instance wants 3.
  @Test(expected=IllegalStateException.class)
  public void getIdMisconfiguredWidth() {
    uid = new UniqueId(client, table, kind, 3);
    final byte[] id = { 'a', 0x42 };
    final byte[] byte_name = { 'f', 'o', 'o' };

    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(byte_name, ID, kind_array, id));
    when(client.get(anyGet()))
      .thenReturn(Deferred.fromResult(kvs));

    uid.getId("foo");
  }

  @Test(expected=NoSuchUniqueName.class)
  public void getIdForNonexistentName() {
    uid = new UniqueId(client, table, kind, 3);

    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    uid.getId("foo");
  }

  @Test
  public void getOrCreateIdWithExistingId() {
    uid = new UniqueId(client, table, kind, 3);
    final byte[] id = { 0, 'a', 0x42 };
    final byte[] byte_name = { 'f', 'o', 'o' };

    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(byte_name, ID, kind_array, id));
    when(client.get(anyGet()))
      .thenReturn(Deferred.fromResult(kvs));

    assertArrayEquals(id, uid.getOrCreateId("foo"));
    // Should be a cache hit ...
    assertArrayEquals(id, uid.getOrCreateId("foo"));
    assertEquals(1, uid.cacheHits());
    assertEquals(1, uid.cacheMisses());
    assertEquals(2, uid.cacheSize());

    // ... so verify there was only one HBase Get.
    verify(client).get(anyGet());
  }

  @Test  // Test the creation of an ID with no problem.
  public void getOrCreateIdAssignIdWithSuccess() {
    uid = new UniqueId(client, table, kind, 3);
    final byte[] id = { 0, 0, 5 };
    final Config config = mock(Config.class);
    when(config.enable_realtime_uid()).thenReturn(false);
    final TSDB tsdb = mock(TSDB.class);
    when(tsdb.getConfig()).thenReturn(config);
    uid.setTSDB(tsdb);
    
    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    when(client.atomicIncrement(incrementForRow(MAXID)))
      .thenReturn(Deferred.fromResult(5L));

    when(client.compareAndSet(anyPut(), emptyArray()))
      .thenReturn(Deferred.fromResult(true))
      .thenReturn(Deferred.fromResult(true));

    assertArrayEquals(id, uid.getOrCreateId("foo"));
    // Should be a cache hit since we created that entry.
    assertArrayEquals(id, uid.getOrCreateId("foo"));
    // Should be a cache hit too for the same reason.
    assertEquals("foo", uid.getName(id));

    verify(client).get(anyGet()); // Initial Get.
    verify(client).atomicIncrement(incrementForRow(MAXID));
    // Reverse + forward mappings.
    verify(client, times(2)).compareAndSet(anyPut(), emptyArray());
  }

  @PrepareForTest({HBaseClient.class, UniqueId.class})
  @Test  // Test the creation of an ID when unable to increment MAXID
  public void getOrCreateIdUnableToIncrementMaxId() throws Exception {
    PowerMockito.mockStatic(Thread.class);

    uid = new UniqueId(client, table, kind, 3);

    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    HBaseException hbe = fakeHBaseException();
    when(client.atomicIncrement(incrementForRow(MAXID)))
      .thenThrow(hbe);
    PowerMockito.doNothing().when(Thread.class); Thread.sleep(anyInt());

    try {
      uid.getOrCreateId("foo");
      fail("HBaseException should have been thrown!");
    } catch (HBaseException e) {
      assertSame(hbe, e);
    }
  }

  @Test  // Test the creation of an ID with a race condition.
  @PrepareForTest({HBaseClient.class, Deferred.class})
   public void getOrCreateIdAssignIdWithRaceCondition() {
    // Simulate a race between client A and client B.
    // A does a Get and sees that there's no ID for this name.
    // B does a Get and sees that there's no ID too, and B actually goes
    // through the entire process to create the ID.
    // Then A attempts to go through the process and should discover that the
    // ID has already been assigned.

    uid = new UniqueId(client, table, kind, 3); // Used by client A.
    HBaseClient client_b = mock(HBaseClient.class); // For client B.
    final UniqueId uid_b = new UniqueId(client_b, table, kind, 3);

    final byte[] id = { 0, 0, 5 };
    final byte[] byte_name = { 'f', 'o', 'o' };
    final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(byte_name, ID, kind_array, id));

    @SuppressWarnings("unchecked")
    final Deferred<ArrayList<KeyValue>> d = PowerMockito.spy(new Deferred<ArrayList<KeyValue>>());
    when(client.get(anyGet()))
      .thenReturn(d)
      .thenReturn(Deferred.fromResult(kvs));

    final Answer<byte[]> the_race = new Answer<byte[]>() {
      public byte[] answer(final InvocationOnMock unused_invocation) throws Exception {
        // While answering A's first Get, B doest a full getOrCreateId.
        assertArrayEquals(id, uid_b.getOrCreateId("foo"));
        d.callback(null);
        return (byte[]) ((Deferred) d).join();
      }
    };

    // Start the race when answering A's first Get.
    try {
      PowerMockito.doAnswer(the_race).when(d).joinUninterruptibly();
    } catch (Exception e) {
      fail("Should never happen: " + e);
    }

    when(client_b.get(anyGet())) // null => ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^ I'm writing C++ in Java!

    when(client_b.atomicIncrement(incrementForRow(MAXID)))
      .thenReturn(Deferred.fromResult(5L));

    when(client_b.compareAndSet(anyPut(), emptyArray()))
      .thenReturn(Deferred.fromResult(true))
      .thenReturn(Deferred.fromResult(true));

    // Now that B is finished, A proceeds and allocates a UID that will be
    // wasted, and creates the reverse mapping, but fails at creating the
    // forward mapping.
    when(client.atomicIncrement(incrementForRow(MAXID)))
      .thenReturn(Deferred.fromResult(6L));

    when(client.compareAndSet(anyPut(), emptyArray()))
      .thenReturn(Deferred.fromResult(true)) // Orphan reverse mapping.
      .thenReturn(Deferred.fromResult(false)); // Already CAS'ed by A.

    // Start the execution.
    assertArrayEquals(id, uid.getOrCreateId("foo"));

    // Verify the order of execution too.
    final InOrder order = inOrder(client, client_b);
    order.verify(client).get(anyGet()); // 1st Get for A.
    order.verify(client_b).get(anyGet()); // 1st Get for B.
    order.verify(client_b).atomicIncrement(incrementForRow(MAXID));
    order.verify(client_b, times(2)).compareAndSet(anyPut(), // both mappings.
                                                   emptyArray());
    order.verify(client).atomicIncrement(incrementForRow(MAXID));
    order.verify(client, times(2)).compareAndSet(anyPut(), // both mappings.
                                                 emptyArray());
    order.verify(client).get(anyGet()); // A retries and gets it.
  }

  @Test
  // Test the creation of an ID when all possible IDs are already in use
  public void getOrCreateIdWithOverflow() {
    uid = new UniqueId(client, table, kind, 1);  // IDs are only on 1 byte.

    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    // Update once HBASE-2292 is fixed:
    when(client.atomicIncrement(incrementForRow(MAXID)))
      .thenReturn(Deferred.fromResult(256L));

    try {
      final byte[] id = uid.getOrCreateId("foo");
      fail("IllegalArgumentException should have been thrown but instead "
           + " this was returned id=" + Arrays.toString(id));
    } catch (IllegalStateException e) {
      // OK.
    }

    verify(client, times(1)).get(anyGet());  // Initial Get.
    verify(client).atomicIncrement(incrementForRow(MAXID));
  }

  @Test  // ICV throws an exception, we can't get an ID.
  public void getOrCreateIdWithICVFailure() {
    uid = new UniqueId(client, table, kind, 3);
    final Config config = mock(Config.class);
    when(config.enable_realtime_uid()).thenReturn(false);
    final TSDB tsdb = mock(TSDB.class);
    when(tsdb.getConfig()).thenReturn(config);
    uid.setTSDB(tsdb);
    
    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    // Update once HBASE-2292 is fixed:
    HBaseException hbe = fakeHBaseException();
    when(client.atomicIncrement(incrementForRow(MAXID)))
      .thenReturn(Deferred.<Long>fromError(hbe))
      .thenReturn(Deferred.fromResult(5L));

    when(client.compareAndSet(anyPut(), emptyArray()))
      .thenReturn(Deferred.fromResult(true))
      .thenReturn(Deferred.fromResult(true));

    final byte[] id = { 0, 0, 5 };
    assertArrayEquals(id, uid.getOrCreateId("foo"));
    verify(client, times(1)).get(anyGet()); // Initial Get.
    // First increment (failed) + retry.
    verify(client, times(2)).atomicIncrement(incrementForRow(MAXID));
    // Reverse + forward mappings.
    verify(client, times(2)).compareAndSet(anyPut(), emptyArray());
  }

  @Test  // Test that the reverse mapping is created before the forward one.
  public void getOrCreateIdPutsReverseMappingFirst() {
    uid = new UniqueId(client, table, kind, 3);
    final Config config = mock(Config.class);
    when(config.enable_realtime_uid()).thenReturn(false);
    final TSDB tsdb = mock(TSDB.class);
    when(tsdb.getConfig()).thenReturn(config);
    uid.setTSDB(tsdb);
    
    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    when(client.atomicIncrement(incrementForRow(MAXID)))
      .thenReturn(Deferred.fromResult(6L));

    when(client.compareAndSet(anyPut(), emptyArray()))
      .thenReturn(Deferred.fromResult(true))
      .thenReturn(Deferred.fromResult(true));

    final byte[] id = { 0, 0, 6 };
    final byte[] row = { 'f', 'o', 'o' };
    assertArrayEquals(id, uid.getOrCreateId("foo"));

    final InOrder order = inOrder(client);
    order.verify(client).get(anyGet());            // Initial Get.
    order.verify(client).atomicIncrement(incrementForRow(MAXID));
    order.verify(client).compareAndSet(putForRow(id), emptyArray());
    order.verify(client).compareAndSet(putForRow(row), emptyArray());
  }

  @PrepareForTest({HBaseClient.class, Scanner.class})
  @Test
  public void suggestWithNoMatch() {
    uid = new UniqueId(client, table, kind, 3);

    final Scanner fake_scanner = mock(Scanner.class);
    when(client.newScanner(table))
      .thenReturn(fake_scanner);

    when(fake_scanner.nextRows())
      .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    final List<String> suggestions = uid.suggest("nomatch");
    assertEquals(0, suggestions.size());  // No results.

    verify(fake_scanner).setStartKey("nomatch".getBytes());
    verify(fake_scanner).setStopKey("nomatci".getBytes());
    verify(fake_scanner).setFamily(ID);
    verify(fake_scanner).setQualifier(kind_array);
  }

  @PrepareForTest({HBaseClient.class, Scanner.class})
  @Test
  public void suggestWithMatches() {
    uid = new UniqueId(client, table, kind, 3);

    final Scanner fake_scanner = mock(Scanner.class);
    when(client.newScanner(table))
      .thenReturn(fake_scanner);

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
    when(fake_scanner.nextRows())
      .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(rows))
      .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    final List<String> suggestions = uid.suggest("foo");
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
    assertArrayEquals(foo_bar_id, uid.getOrCreateId("foo.bar"));
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
  public void getTagPairsFromTSUID() {
    List<byte[]> tags = UniqueId.getTagPairsFromTSUID(
        "000000000001000002000003000004", 
        (short)3, (short)3, (short)3);
    assertNotNull(tags);
    assertEquals(4, tags.size());
    assertArrayEquals(new byte[] { 0, 0, 1 }, tags.get(0));
    assertArrayEquals(new byte[] { 0, 0, 2 }, tags.get(1));
    assertArrayEquals(new byte[] { 0, 0, 3 }, tags.get(2));
    assertArrayEquals(new byte[] { 0, 0, 4 }, tags.get(3));
  }
  
  @Test
  public void getTagPairsFromTSUIDNonStandardWidth() {
    List<byte[]> tags = UniqueId.getTagPairsFromTSUID(
        "0000000000000100000200000003000004",  
        (short)3, (short)4, (short)3);
    assertNotNull(tags);
    assertEquals(4, tags.size());
    assertArrayEquals(new byte[] { 0, 0, 0, 1 }, tags.get(0));
    assertArrayEquals(new byte[] { 0, 0, 2 }, tags.get(1));
    assertArrayEquals(new byte[] { 0, 0, 0, 3 }, tags.get(2));
    assertArrayEquals(new byte[] { 0, 0, 4 }, tags.get(3));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDMissingTags() {
    UniqueId.getTagPairsFromTSUID("123456", (short)3, (short)3, (short)3);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDMissingMetric() {
    UniqueId.getTagPairsFromTSUID("000001000002", (short)3, (short)3, (short)3);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDOddNumberOfCharacters() {
    UniqueId.getTagPairsFromTSUID("0000080000010000020", 
        (short)3, (short)3, (short)3);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDMissingTagv() {
    UniqueId.getTagPairsFromTSUID("000008000001", 
        (short)3, (short)3, (short)3);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDNull() {
    UniqueId.getTagPairsFromTSUID(null, (short)3, (short)3, (short)3);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDEmpty() {
    UniqueId.getTagPairsFromTSUID("", (short)3, (short)3, (short)3);
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
    final TSDB tsdb = mock(TSDB.class);
    when(tsdb.getClient()).thenReturn(client);
    when(tsdb.uidTable()).thenReturn(new byte[] { 'u', 'i', 'd' });
    when(client.get(anyGet()))
      .thenReturn(Deferred.fromResult(kvs));
    
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
    final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(0);
    final byte[] metrics = { 'm', 'e', 't', 'r', 'i', 'c', 's' };
    final byte[] tagk = { 't', 'a', 'g', 'k' };
    final byte[] tagv = { 't', 'a', 'g', 'v' };
    final TSDB tsdb = mock(TSDB.class);
    when(tsdb.getClient()).thenReturn(client);
    when(tsdb.uidTable()).thenReturn(new byte[] { 'u', 'i', 'd' });
    when(client.get(anyGet()))
      .thenReturn(Deferred.fromResult(kvs));
    
    final byte[][] kinds = { metrics, tagk, tagv };
    final Map<String, Long> uids = UniqueId.getUsedUIDs(tsdb, kinds)
      .joinUninterruptibly();
    assertNotNull(uids);
    assertEquals(3, uids.size());
    assertEquals(0L, uids.get("metrics").longValue());
    assertEquals(0L, uids.get("tagk").longValue());
    assertEquals(0L, uids.get("tagv").longValue());
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

  private static HBaseException fakeHBaseException() {
    final HBaseException hbe = mock(HBaseException.class);
    when(hbe.getStackTrace())
      // Truncate the stack trace because otherwise it's gigantic.
      .thenReturn(Arrays.copyOf(new RuntimeException().getStackTrace(), 3));
    when(hbe.getMessage())
      .thenReturn("fake exception");
    return hbe;
  }

  private static final byte[] MAXID = { 0 };

}