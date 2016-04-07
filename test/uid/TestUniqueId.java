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

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.utils.Config;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
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
@PrepareForTest({ HBaseClient.class, TSDB.class, Config.class, Scanner.class, 
  RandomUniqueId.class, Const.class, Deferred.class })
public final class TestUniqueId {
  private static final byte[] table = { 't', 's', 'd', 'b', '-', 'u', 'i', 'd' };
  private static final byte[] ID = { 'i', 'd' };
  private static final byte[] NAME = { 'n', 'a', 'm', 'e' };
  private static final String METRIC = "metric";
  private static final byte[] METRIC_ARRAY = { 'm', 'e', 't', 'r', 'i', 'c' };
  private static final String TAGK = "tagk";
  private static final byte[] TAGK_ARRAY = { 't', 'a', 'g', 'k' };
  private static final String TAGV = "tagv";
  private static final byte[] TAGV_ARRAY = { 't', 'a', 'g', 'v' };
  private static final byte[] UID = new byte[] { 0, 0, 1 };
  private TSDB tsdb = mock(TSDB.class);
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueId uid;  
  private MockBase storage;

  @Test(expected=IllegalArgumentException.class)
  public void testCtorZeroWidth() {
    uid = new UniqueId(client, table, METRIC, 0);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCtorNegativeWidth() {
    uid = new UniqueId(client, table, METRIC, -1);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCtorEmptyKind() {
    uid = new UniqueId(client, table, "", 3);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCtorLargeWidth() {
    uid = new UniqueId(client, table, METRIC, 9);
  }

  @Test
  public void kindEqual() {
    uid = new UniqueId(client, table, METRIC, 3);
    assertEquals(METRIC, uid.kind());
  }

  @Test
  public void widthEqual() {
    uid = new UniqueId(client, table, METRIC, 3);
    assertEquals(3, uid.width());
  }
  
  @Test
  public void getNameSuccessfulHBaseLookup() {
    uid = new UniqueId(client, table, METRIC, 3);
    final byte[] id = { 0, 'a', 0x42 };
    final byte[] byte_name = { 'f', 'o', 'o' };

    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(id, ID, METRIC_ARRAY, byte_name));
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
    uid = new UniqueId(client, table, METRIC, 3);
    final byte[] id = { 0, 'a', 0x42 };
    final byte[] byte_name = { 'f', 'o', 'o' };

    HBaseException hbe = mock(HBaseException.class);

    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(id, ID, METRIC_ARRAY, byte_name));
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
    uid = new UniqueId(client, table, METRIC, 3);

    when(client.get(anyGet()))
      .thenReturn(Deferred.fromResult(new ArrayList<KeyValue>(0)));

    uid.getName(new byte[] { 1, 2, 3 });
  }

  @Test(expected=IllegalArgumentException.class)
  public void getNameWithInvalidId() {
    uid = new UniqueId(client, table, METRIC, 3);

    uid.getName(new byte[] { 1 });
  }

  @Test
  public void getIdSuccessfulHBaseLookup() {
    uid = new UniqueId(client, table, METRIC, 3);
    final byte[] id = { 0, 'a', 0x42 };
    final byte[] byte_name = { 'f', 'o', 'o' };

    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(byte_name, ID, METRIC_ARRAY, id));
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
    uid = new UniqueId(client, table, METRIC, 3);
    final byte[] id = { 'a', 0x42 };
    final byte[] byte_name = { 'f', 'o', 'o' };

    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(byte_name, ID, METRIC_ARRAY, id));
    when(client.get(anyGet()))
      .thenReturn(Deferred.fromResult(kvs));

    uid.getId("foo");
  }

  @Test(expected=NoSuchUniqueName.class)
  public void getIdForNonexistentName() {
    uid = new UniqueId(client, table, METRIC, 3);

    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    uid.getId("foo");
  }

  @Test
  public void getOrCreateIdWithExistingId() {
    uid = new UniqueId(client, table, METRIC, 3);
    final byte[] id = { 0, 'a', 0x42 };
    final byte[] byte_name = { 'f', 'o', 'o' };

    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(byte_name, ID, METRIC_ARRAY, id));
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
    uid = new UniqueId(client, table, METRIC, 3);
    final byte[] id = { 0, 0, 5 };
    final Config config = mock(Config.class);
    when(config.enable_realtime_uid()).thenReturn(false);
    when(config.auto_whitelist()).thenReturn(false);
    when(config.auto_metric_patterns()).thenReturn(".*");
    when(config.auto_tagk_patterns()).thenReturn(".*");
    when(config.auto_tagv_patterns()).thenReturn(".*");
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

  @Test  // Test the creation of an ID with no problem.
  public void getOrCreateIdAssignWhitelistedIdWithSuccess() {
    uid = new UniqueId(client, table, METRIC, 3);
    final byte[] id = { 0, 0, 5 };
    final Config config = mock(Config.class);
    when(config.enable_realtime_uid()).thenReturn(false);
    when(config.auto_whitelist()).thenReturn(true);
    when(config.auto_metric_patterns()).thenReturn(".*");
    when(config.auto_tagk_patterns()).thenReturn(".*");
    when(config.auto_tagv_patterns()).thenReturn(".*");
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

  @Test(expected=RuntimeException.class)
  public void getOrCreateIdAssignWhitelistedIdWithFailedWhitelist() {
    uid = new UniqueId(client, table, METRIC, 3);
    final byte[] id = { 0, 0, 5 };
    final Config config = mock(Config.class);
    when(config.enable_realtime_uid()).thenReturn(false);
    when(config.auto_whitelist()).thenReturn(true);
    when(config.auto_metric_patterns()).thenReturn("^nomatch.*$");
    when(config.auto_tagk_patterns()).thenReturn("^sys\\.cpu\\.*$");
    when(config.auto_tagv_patterns()).thenReturn("^sys\\.cpu\\.*$");
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

  @Test  // Test the creation of an ID with no problem.
  public void checkMetricAgainstWhitelist() {
    setupWhitelists(METRIC);
    assertTrue(uid.checkNameIsValid("sys.cpu.user"));
  }

  @Test  // Test the creation of an ID with no problem.
  public void checkTagKAgainstWhitelist() {
    setupWhitelists(TAGK);
    assertTrue(uid.checkNameIsValid("sys.cpu.user"));
  }

  @Test  // Test the creation of an ID with no problem.
  public void checkTagVAgainstWhitelist() {
    setupWhitelists(TAGV);
    assertTrue(uid.checkNameIsValid("sys.cpu.user"));
  }

  @Test
  public void checkMetricAgainstWhitelistFails() {
    setupWhitelists(METRIC);
    assertFalse(uid.checkNameIsValid("foo.badmetric"));
  }

  @Test
  public void checkTagKAgainstWhitelistFails() {
    setupWhitelists(TAGK);
    assertFalse(uid.checkNameIsValid("foo.badmetric"));
  }

  @Test
  public void checkTagVAgainstWhitelistFails() {
    setupWhitelists(TAGV);
    assertFalse(uid.checkNameIsValid("foo.badmetric"));
  }

  private void setupWhitelists(String type) {
    uid = new UniqueId(client, table, type, 3);
    final Config config = mock(Config.class);
    when(config.enable_realtime_uid()).thenReturn(false);
    when(config.auto_whitelist()).thenReturn(true);
    when(config.auto_metric_patterns()).thenReturn("sys.*");
    when(config.auto_tagk_patterns()).thenReturn("sys.*");
    when(config.auto_tagv_patterns()).thenReturn("sys.*");
    final TSDB tsdb = mock(TSDB.class);
    when(tsdb.getConfig()).thenReturn(config);
    uid.setTSDB(tsdb);
  }

  @Test  // Test the creation of an ID when unable to increment MAXID
  public void getOrCreateIdUnableToIncrementMaxId() throws Exception {
    PowerMockito.mockStatic(Thread.class);

    uid = new UniqueId(client, table, METRIC, 3);

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
  public void getOrCreateIdAssignIdWithRaceCondition() {
    // Simulate a race between client A and client B.
    // A does a Get and sees that there's no ID for this name.
    // B does a Get and sees that there's no ID too, and B actually goes
    // through the entire process to create the ID.
    // Then A attempts to go through the process and should discover that the
    // ID has already been assigned.

    uid = new UniqueId(client, table, METRIC, 3); // Used by client A.
    HBaseClient client_b = mock(HBaseClient.class); // For client B.
    final UniqueId uid_b = new UniqueId(client_b, table, METRIC, 3);

    final byte[] id = { 0, 0, 5 };
    final byte[] byte_name = { 'f', 'o', 'o' };
    final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(byte_name, ID, METRIC_ARRAY, id));
    
    final Deferred<ArrayList<KeyValue>> d = 
      PowerMockito.spy(new Deferred<ArrayList<KeyValue>>());
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
    uid = new UniqueId(client, table, METRIC, 1);  // IDs are only on 1 byte.

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
    uid = new UniqueId(client, table, METRIC, 3);
    final Config config = mock(Config.class);
    when(config.enable_realtime_uid()).thenReturn(false);
    when(config.auto_whitelist()).thenReturn(false);
    when(config.auto_metric_patterns()).thenReturn(".*");
    when(config.auto_tagk_patterns()).thenReturn(".*");
    when(config.auto_tagv_patterns()).thenReturn(".*");
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
    uid = new UniqueId(client, table, METRIC, 3);
    final Config config = mock(Config.class);
    when(config.enable_realtime_uid()).thenReturn(false);
    when(config.auto_whitelist()).thenReturn(false);
    when(config.auto_metric_patterns()).thenReturn(".*");
    when(config.auto_tagk_patterns()).thenReturn(".*");
    when(config.auto_tagv_patterns()).thenReturn(".*");
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
  
  @Test
  public void getOrCreateIdRandom() {
    PowerMockito.mockStatic(RandomUniqueId.class);
    uid = new UniqueId(client, table, METRIC, 3, true);
    final long id = 42L;
    final byte[] id_array = { 0, 0, 0x2A };

    when(RandomUniqueId.getRandomUID()).thenReturn(id);
    when(client.get(any(GetRequest.class)))
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    
    when(client.compareAndSet(any(PutRequest.class), any(byte[].class)))
      .thenReturn(Deferred.fromResult(true))
      .thenReturn(Deferred.fromResult(true));

    assertArrayEquals(id_array, uid.getOrCreateId("foo"));
    // Should be a cache hit ...
    assertArrayEquals(id_array, uid.getOrCreateId("foo"));
    assertEquals(1, uid.cacheHits());
    assertEquals(1, uid.cacheMisses());
    assertEquals(2, uid.cacheSize());
    assertEquals(0, uid.randomIdCollisions());
    // ... so verify there was only one HBase Get.
    verify(client).get(any(GetRequest.class));
  }
  
  @Test
  public void getOrCreateIdRandomCollision() {
    PowerMockito.mockStatic(RandomUniqueId.class);
    uid = new UniqueId(client, table, METRIC, 3, true);
    final long id = 42L;
    final byte[] id_array = { 0, 0, 0x2A };

    when(RandomUniqueId.getRandomUID()).thenReturn(24L).thenReturn(id);
    
    when(client.get(any(GetRequest.class)))
      .thenReturn(Deferred.fromResult((ArrayList<KeyValue>)null));
    
    when(client.compareAndSet(anyPut(), any(byte[].class)))
      .thenReturn(Deferred.fromResult(false))
      .thenReturn(Deferred.fromResult(true))
      .thenReturn(Deferred.fromResult(true));

    assertArrayEquals(id_array, uid.getOrCreateId("foo"));
    // Should be a cache hit ...
    assertArrayEquals(id_array, uid.getOrCreateId("foo"));
    assertEquals(1, uid.cacheHits());
    assertEquals(1, uid.cacheMisses());
    assertEquals(2, uid.cacheSize());
    assertEquals(1, uid.randomIdCollisions());

    // ... so verify there was only one HBase Get.
    verify(client).get(anyGet());
  }
  
  @Test
  public void getOrCreateIdRandomCollisionTooManyAttempts() {
    PowerMockito.mockStatic(RandomUniqueId.class);
    uid = new UniqueId(client, table, METRIC, 3, true);
    final long id = 42L;

    when(RandomUniqueId.getRandomUID()).thenReturn(24L).thenReturn(id);
    
    when(client.get(any(GetRequest.class)))
      .thenReturn(Deferred.fromResult((ArrayList<KeyValue>)null));
    
    when(client.compareAndSet(any(PutRequest.class), any(byte[].class)))
      .thenReturn(Deferred.fromResult(false))
      .thenReturn(Deferred.fromResult(false))
      .thenReturn(Deferred.fromResult(false))
      .thenReturn(Deferred.fromResult(false))
      .thenReturn(Deferred.fromResult(false))
      .thenReturn(Deferred.fromResult(false))
      .thenReturn(Deferred.fromResult(false))
      .thenReturn(Deferred.fromResult(false))
      .thenReturn(Deferred.fromResult(false))
      .thenReturn(Deferred.fromResult(false));

    try {
      final byte[] assigned_id = uid.getOrCreateId("foo");
      fail("FailedToAssignUniqueIdException should have been thrown but instead "
           + " this was returned id=" + Arrays.toString(assigned_id));
    } catch (FailedToAssignUniqueIdException e) {
      // OK
    }
    assertEquals(0, uid.cacheHits());
    assertEquals(1, uid.cacheMisses());
    assertEquals(0, uid.cacheSize());
    assertEquals(9, uid.randomIdCollisions());

    // ... so verify there was only one HBase Get.
    verify(client).get(any(GetRequest.class));
  }
  
  @Test
  public void getOrCreateIdRandomWithRaceCondition() {
    PowerMockito.mockStatic(RandomUniqueId.class);
    uid = new UniqueId(client, table, METRIC, 3, true);
    final long id = 24L;
    final byte[] id_array = { 0, 0, 0x2A };
    final byte[] byte_name = { 'f', 'o', 'o' };
    
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(byte_name, ID, METRIC_ARRAY, id_array));
    
    when(RandomUniqueId.getRandomUID()).thenReturn(id);
    
    when(client.get(any(GetRequest.class)))
      .thenReturn(Deferred.fromResult((ArrayList<KeyValue>)null))
      .thenReturn(Deferred.fromResult(kvs));
    
    when(client.compareAndSet(any(PutRequest.class), any(byte[].class)))
      .thenReturn(Deferred.fromResult(true))
      .thenReturn(Deferred.fromResult(false));

    assertArrayEquals(id_array, uid.getOrCreateId("foo"));
    assertEquals(0, uid.cacheHits());
    assertEquals(2, uid.cacheMisses());
    assertEquals(2, uid.cacheSize());
    assertEquals(1, uid.randomIdCollisions());

    // ... so verify there was only one HBase Get.
    verify(client, times(2)).get(any(GetRequest.class));
  }
  
  @Test
  public void suggestWithNoMatch() {
    uid = new UniqueId(client, table, METRIC, 3);

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
    verify(fake_scanner).setQualifier(METRIC_ARRAY);
  }
  
  @Test
  public void suggestWithMatches() {
    uid = new UniqueId(client, table, METRIC, 3);

    final Scanner fake_scanner = mock(Scanner.class);
    when(client.newScanner(table))
      .thenReturn(fake_scanner);

    final ArrayList<ArrayList<KeyValue>> rows = new ArrayList<ArrayList<KeyValue>>(2);
    final byte[] foo_bar_id = { 0, 0, 1 };
    {
      ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
      row.add(new KeyValue("foo.bar".getBytes(), ID, METRIC_ARRAY, foo_bar_id));
      rows.add(row);
      row = new ArrayList<KeyValue>(1);
      row.add(new KeyValue("foo.baz".getBytes(), ID, METRIC_ARRAY,
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
  public void getTSUIDFromKeySalted() {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    
    final byte[] expected = { 0, 0, 1, 0, 0, 2, 0, 0, 3 };
    byte[] tsuid = UniqueId.getTSUIDFromKey(new byte[] 
      { 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3 }, (short)3, (short)4);
    assertArrayEquals(expected, tsuid);
    
    tsuid = UniqueId.getTSUIDFromKey(new byte[] 
      { 1, 0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3 }, (short)3, (short)4);
    assertArrayEquals(expected, tsuid);
    
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(4);
    tsuid = UniqueId.getTSUIDFromKey(new byte[] 
      { 1, 2, 3, 4, 0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3 }, (short)3, (short)4);
    assertArrayEquals(expected, tsuid);
    
    tsuid = UniqueId.getTSUIDFromKey(new byte[] 
      { 4, 3, 2, 1, 0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3 }, (short)3, (short)4);
    assertArrayEquals(expected, tsuid);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTSUIDFromKeyMissingTags() {
    UniqueId.getTSUIDFromKey(new byte[] 
      { 0, 0, 1, 1, 1, 1, 1 }, (short)3, (short)4);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTSUIDFromKeyMissingTagsSalted() {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1); 
    
    UniqueId.getTSUIDFromKey(new byte[] 
      { 0, 0, 0, 1, 1, 1, 1, 1 }, (short)3, (short)4);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTSUIDFromKeyMissingSalt() {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    
    UniqueId.getTSUIDFromKey(new byte[] 
        { 0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3 }, (short)3, (short)4);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTSUIDFromKeySaltButShouldntBe() {
    UniqueId.getTSUIDFromKey(new byte[] 
        { 1, 0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3 }, (short)3, (short)4);
  }
  
  @Test
  public void getTagPairsFromTSUIDString() {
    List<byte[]> tags = UniqueId.getTagPairsFromTSUID(
        "000000000001000002000003000004");
    assertNotNull(tags);
    assertEquals(2, tags.size());
    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 2 }, tags.get(0));
    assertArrayEquals(new byte[] { 0, 0, 3, 0, 0, 4 }, tags.get(1));
  }
  
  
  @Test
  public void getTagPairsFromTSUIDStringNonStandardWidth() {
    PowerMockito.mockStatic(TSDB.class);
    when(TSDB.metrics_width()).thenReturn((short)3);
    when(TSDB.tagk_width()).thenReturn((short)4);
    when(TSDB.tagv_width()).thenReturn((short)3);
    
    List<byte[]> tags = UniqueId.getTagPairsFromTSUID(
        "0000000000000100000200000003000004");
    assertNotNull(tags);
    assertEquals(2, tags.size());
    assertArrayEquals(new byte[] { 0, 0, 0, 1, 0, 0, 2 }, tags.get(0));
    assertArrayEquals(new byte[] { 0, 0, 0, 3, 0, 0, 4 }, tags.get(1));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDStringMissingTags() {
    UniqueId.getTagPairsFromTSUID("123456");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDStringMissingMetric() {
    UniqueId.getTagPairsFromTSUID("000001000002");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDStringOddNumberOfCharacters() {
    UniqueId.getTagPairsFromTSUID("0000080000010000020");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDStringMissingTagv() {
    UniqueId.getTagPairsFromTSUID("000008000001");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDStringNull() {
    UniqueId.getTagPairsFromTSUID((String)null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDStringEmpty() {
    UniqueId.getTagPairsFromTSUID("");
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
  
  
  @Test
  public void getTagPairsFromTSUIDBytesNonStandardWidth() {
    PowerMockito.mockStatic(TSDB.class);
    when(TSDB.metrics_width()).thenReturn((short)3);
    when(TSDB.tagk_width()).thenReturn((short)4);
    when(TSDB.tagv_width()).thenReturn((short)3);
    
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
  
  @Test
  public void getTagFromTSUIDNonStandardWidth() {
    PowerMockito.mockStatic(TSDB.class);
    when(TSDB.metrics_width()).thenReturn((short)3);
    when(TSDB.tagk_width()).thenReturn((short)4);
    when(TSDB.tagv_width()).thenReturn((short)3);
    
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

  @Test
  public void rename() throws Exception {
    uid = new UniqueId(client, table, METRIC, 3);
    final byte[] foo_id = { 0, 'a', 0x42 };
    final byte[] foo_name = { 'f', 'o', 'o' };

    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(foo_name, ID, METRIC_ARRAY, foo_id));
    when(client.get(anyGet()))
        .thenReturn(Deferred.fromResult(kvs))
        .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    when(client.put(anyPut())).thenAnswer(answerTrue());
    when(client.delete(anyDelete())).thenAnswer(answerTrue());

    uid.rename("foo", "bar");
  }

  @Test (expected = IllegalArgumentException.class)
  public void renameNewNameExists() throws Exception {
    uid = new UniqueId(client, table, METRIC, 3);
    final byte[] foo_id = { 0, 'a', 0x42 };
    final byte[] foo_name = { 'f', 'o', 'o' };
    final byte[] bar_id = { 1, 'b', 0x43 };
    final byte[] bar_name = { 'b', 'a', 'r' };

    ArrayList<KeyValue> foo_kvs = new ArrayList<KeyValue>(1);
    ArrayList<KeyValue> bar_kvs = new ArrayList<KeyValue>(1);
    foo_kvs.add(new KeyValue(foo_name, ID, METRIC_ARRAY, foo_id));
    bar_kvs.add(new KeyValue(bar_name, ID, METRIC_ARRAY, bar_id));
    when(client.get(anyGet()))
        .thenReturn(Deferred.fromResult(foo_kvs))
        .thenReturn(Deferred.fromResult(bar_kvs));
    when(client.put(anyPut())).thenAnswer(answerTrue());
    when(client.delete(anyDelete())).thenAnswer(answerTrue());

    uid.rename("foo", "bar");
  }

  @Test (expected = IllegalStateException.class)
  public void renameRaceCondition() throws Exception {
    // Simulate a race between client A(default) and client B.
    // A and B rename same UID to different name.
    // B waits till A start to invoke PutRequest to start.

    uid = new UniqueId(client, table, METRIC, 3);
    HBaseClient client_b = mock(HBaseClient.class);
    final UniqueId uid_b = new UniqueId(client_b, table, METRIC, 3);

    final byte[] foo_id = { 0, 'a', 0x42 };
    final byte[] foo_name = { 'f', 'o', 'o' };

    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(foo_name, ID, METRIC_ARRAY, foo_id));

    when(client_b.get(anyGet()))
        .thenReturn(Deferred.fromResult(kvs))
        .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    when(client_b.put(anyPut())).thenAnswer(answerTrue());
    when(client_b.delete(anyDelete())).thenAnswer(answerTrue());

    final Answer<Deferred<Boolean>> the_race = new Answer<Deferred<Boolean>>() {
      public Deferred<Boolean> answer(final InvocationOnMock inv) throws Exception {
        uid_b.rename("foo", "xyz");
        return Deferred.fromResult(true);
      }
    };

    when(client.get(anyGet()))
        .thenReturn(Deferred.fromResult(kvs))
        .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    when(client.put(anyPut())).thenAnswer(the_race);
    when(client.delete(anyDelete())).thenAnswer(answerTrue());

    uid.rename("foo", "bar");
  }

  @Test
  public void deleteCached() throws Exception {
    setupStorage();
    uid = new UniqueId(client, table, METRIC, 3);
    uid.setTSDB(tsdb);
    assertArrayEquals(UID, uid.getId("sys.cpu.user"));
    assertEquals("sys.cpu.user", uid.getName(UID));
    
    uid.deleteAsync("sys.cpu.user").join();
    try {
      uid.getId("sys.cpu.user");
      fail("Expected a NoSuchUniqueName");
    } catch (NoSuchUniqueName nsun) { }
    
    try {
      uid.getName(UID);
      fail("Expected a NoSuchUniqueId");
    } catch (NoSuchUniqueId nsui) { }
    
    uid = new UniqueId(client, table, TAGK, 3);
    uid.setTSDB(tsdb);
    assertArrayEquals(UID, uid.getId("host"));
    assertEquals("host", uid.getName(UID));
    
    uid = new UniqueId(client, table, TAGV, 3);
    uid.setTSDB(tsdb);
    assertArrayEquals(UID, uid.getId("web01"));
    assertEquals("web01", uid.getName(UID));
  }
  
  @Test
  public void deleteNotCached() throws Exception {
    setupStorage();
    uid = new UniqueId(client, table, METRIC, 3);
    uid.setTSDB(tsdb);
    uid.deleteAsync("sys.cpu.user").join();
    try {
      uid.getId("sys.cpu.user");
      fail("Expected a NoSuchUniqueName");
    } catch (NoSuchUniqueName nsun) { }
    
    try {
      uid.getName(UID);
      fail("Expected a NoSuchUniqueId");
    } catch (NoSuchUniqueId nsui) { }
    
    uid = new UniqueId(client, table, TAGK, 3);
    uid.setTSDB(tsdb);
    assertArrayEquals(UID, uid.getId("host"));
    assertEquals("host", uid.getName(UID));
    
    uid = new UniqueId(client, table, TAGV, 3);
    uid.setTSDB(tsdb);
    assertArrayEquals(UID, uid.getId("web01"));
    assertEquals("web01", uid.getName(UID));
  }
  
  @Test
  public void deleteFailForwardDelete() throws Exception {
    setupStorage();
    uid = new UniqueId(client, table, METRIC, 3);
    uid.setTSDB(tsdb);
    assertArrayEquals(UID, uid.getId("sys.cpu.user"));
    assertEquals("sys.cpu.user", uid.getName(UID));
    
    storage.throwException("sys.cpu.user".getBytes(), fakeHBaseException());
    try {
      uid.deleteAsync("sys.cpu.user").join();
      fail("Expected HBaseException");
    } catch (HBaseException e) { }
    catch (Exception e) { }
    storage.clearExceptions();
    try {
      uid.getName(UID);
      fail("Expected a NoSuchUniqueId");
    } catch (NoSuchUniqueId nsui) { }
    assertArrayEquals(UID, uid.getId("sys.cpu.user"));
    // now it pollutes the cache
    assertEquals("sys.cpu.user", uid.getName(UID));
  }
  
  @Test
  public void deleteFailReverseDelete() throws Exception {
    setupStorage();
    storage.throwException(UID, fakeHBaseException());
    uid = new UniqueId(client, table, METRIC, 3);
    uid.setTSDB(tsdb);
    try {
      uid.deleteAsync("sys.cpu.user").join();
      fail("Expected HBaseException");
    } catch (HBaseException e) { }
    catch (Exception e) { }
    storage.clearExceptions();
    try {
      uid.getId("sys.cpu.user");
      fail("Expected a NoSuchUniqueName");
    } catch (NoSuchUniqueName nsun) { }
    assertEquals("sys.cpu.user", uid.getName(UID));
  }
  
  @Test
  public void deleteNoSuchUniqueName() throws Exception {
    setupStorage();
    uid = new UniqueId(client, table, METRIC, 3);
    uid.setTSDB(tsdb);
    storage.flushRow(table, "sys.cpu.user".getBytes());
    try {
      uid.deleteAsync("sys.cpu.user").join();
      fail("Expected NoSuchUniqueName");
    } catch (NoSuchUniqueName e) { }
    assertEquals("sys.cpu.user", uid.getName(UID));
  }
  
  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private void setupStorage() throws Exception {
    final Config config = mock(Config.class);
    when(config.auto_whitelist()).thenReturn(false);
    when(config.auto_metric_patterns()).thenReturn(".*");
    when(config.auto_tagk_patterns()).thenReturn(".*");
    when(config.auto_tagv_patterns()).thenReturn(".*");
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getClient()).thenReturn(client);
    storage = new MockBase(tsdb, client, true, true, true, true);
    
    final List<byte[]> families = new ArrayList<byte[]>();
    families.add(ID);
    families.add(NAME);
    storage.addTable(table, families);
    
    storage.addColumn(table, "sys.cpu.user".getBytes(), ID, METRIC_ARRAY, UID);
    storage.addColumn(table, UID, NAME, METRIC_ARRAY, "sys.cpu.user".getBytes());
    storage.addColumn(table, "host".getBytes(), ID, TAGK_ARRAY, UID);
    storage.addColumn(table, UID, NAME, TAGK_ARRAY, "host".getBytes());
    storage.addColumn(table, "web01".getBytes(), ID, TAGV_ARRAY, UID);
    storage.addColumn(table, UID, NAME,TAGV_ARRAY, "web01".getBytes());
  }
  
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
  
  private static DeleteRequest anyDelete() {
    return any(DeleteRequest.class);
  }

  private static Answer<Deferred<Boolean>> answerTrue() {
    return new Answer<Deferred<Boolean>>() {
      public Deferred<Boolean> answer(final InvocationOnMock inv) {
        return Deferred.fromResult(true);
      }
    };
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
