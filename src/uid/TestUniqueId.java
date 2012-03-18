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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.stumbleupon.async.Deferred;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.HBaseRpc;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.hbase.async.RowLockRequest;
import org.hbase.async.Scanner;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.argThat;
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
@PrepareForTest({ HBaseClient.class, RowLock.class })
public final class TestUniqueId {

  private HBaseClient client = mock(HBaseClient.class);
  private static final byte[] table = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] ID = { 'i', 'd' };
  private UniqueId uid;
  private static final String kind = "kind";
  private static final byte[] kind_array = { 'k', 'i', 'n', 'd' };

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

    RowLock fake_lock = mock(RowLock.class);
    when(client.lockRow(anyRowLockRequest()))
      .thenReturn(Deferred.fromResult(fake_lock));

    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!
    when(client.put(anyPut()))
      .thenReturn(Deferred.fromResult(null));

    // Update once HBASE-2292 is fixed:
    whenFakeIcvThenReturn(4L);

    assertArrayEquals(id, uid.getOrCreateId("foo"));
    // Should be a cache hit since we created that entry.
    assertArrayEquals(id, uid.getOrCreateId("foo"));
    // Should be a cache hit too for the same reason.
    assertEquals("foo", uid.getName(id));

    // The +1's below are due to the whenFakeIcvThenReturn() hack.
    verify(client, times(2+1)).get(anyGet()); // Initial Get + double check.
    verify(client).lockRow(anyRowLockRequest());  // The .maxid row.
    verify(client, times(2+1)).put(anyPut()); // reverse + forward mappings.
    verify(client).unlockRow(fake_lock);     // The .maxid row.
  }

  @PrepareForTest({HBaseClient.class, UniqueId.class})
  @Test  // Test the creation of an ID when unable to acquire the row lock.
  public void getOrCreateIdUnableToAcquireRowLock() throws Exception {
    PowerMockito.mockStatic(Thread.class);

    uid = new UniqueId(client, table, kind, 3);

    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    HBaseException hbe = fakeHBaseException();
    when(client.lockRow(anyRowLockRequest()))
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
  @PrepareForTest({HBaseClient.class, RowLock.class, Deferred.class})
  public void getOrCreateIdAssignIdWithRaceCondition() {
    // Simulate a race between client A and client B.
    // A does a Get and sees that there's no ID for this name.
    // B does a Get and sees that there's no ID too, and B actually goes
    // through the entire process to create the ID.
    // Then A attempts to go through the process and should discover that the
    // ID has already been assigned.

    uid = new UniqueId(client, table, kind, 3);  // Used by client A.
    HBaseClient client_b = mock(HBaseClient.class);
    final UniqueId uid_b = new UniqueId(client_b, table, kind, 3);  // for client B.

    final byte[] id = { 0, 0, 5 };
    final byte[] byte_name = { 'f', 'o', 'o' };

    @SuppressWarnings("unchecked")
    final Deferred<ArrayList<KeyValue>> d = mock(Deferred.class);
    when(client.get(anyGet()))
      .thenReturn(d);

    final Answer<byte[]> the_race = new Answer<byte[]>() {
      public byte[] answer(final InvocationOnMock unused_invocation) {
        // While answering A's first Get, B doest a full getOrCreateId.
        assertArrayEquals(id, uid_b.getOrCreateId("foo"));
        return null;
      }
    };

    try {
      ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
      kvs.add(new KeyValue(byte_name, ID, kind_array, id));
      when(d.joinUninterruptibly())
        .thenAnswer(the_race)  // Start the race when answering A's first Get.
        .thenReturn(kvs);      // The 2nd Get succeeds because B created the ID.
    } catch (Exception e) {
      fail("Should never happen: " + e);
    }

    RowLock fake_lock_a = mock(RowLock.class);
    when(client.lockRow(anyRowLockRequest()))
      .thenReturn(Deferred.fromResult(fake_lock_a));

    when(client_b.get(anyGet()))      // null  =>  ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    RowLock fake_lock_b = mock(RowLock.class);
    when(client_b.lockRow(anyRowLockRequest()))
      .thenReturn(Deferred.fromResult(fake_lock_b));

    // Update once HBASE-2292 is fixed:
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(MAXID, ID, kind_array, Bytes.fromLong(4L)));
    when(client_b.get(getForRow(MAXID)))
      .thenReturn(Deferred.fromResult(kvs));

    when(client_b.put(anyPut()))
      .thenReturn(Deferred.fromResult(null));

    // Start the execution.
    assertArrayEquals(id, uid.getOrCreateId("foo"));

    // The +1's below are due to the whenFakeIcvThenReturn() hack.
    // Verify the order of execution too.
    final InOrder order = inOrder(client, client_b);
    order.verify(client).get(anyGet());             // 1st Get for A.
    order.verify(client_b).get(anyGet());           // 1st Get for B.
    order.verify(client_b).lockRow(anyRowLockRequest());  // B starts the process...
    order.verify(client_b, times(1+1)).get(anyGet()); // double check for B.
    order.verify(client_b, times(2+1)).put(anyPut()); // both mappings.
    order.verify(client_b).unlockRow(fake_lock_b);  // ... B finishes.
    order.verify(client).lockRow(anyRowLockRequest());  // A starts the process...
    order.verify(client).get(anyGet());             // Finds the ID added by B
    order.verify(client).unlockRow(fake_lock_a);    // ... and stops here.
    // Things A shouldn't do because B did them already:
    verify(client, never()).atomicIncrement(any(AtomicIncrementRequest.class));
    verify(client, never()).put(anyPut());
  }

  @Test
  // Test the creation of an ID when all possible IDs are already in use
  public void getOrCreateIdWithOverflow() {
    uid = new UniqueId(client, table, kind, 1);  // IDs are only on 1 byte.

    RowLock fake_lock = mock(RowLock.class);
    when(client.lockRow(anyRowLockRequest()))
      .thenReturn(Deferred.fromResult(fake_lock));

    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    // Update once HBASE-2292 is fixed:
    whenFakeIcvThenReturn(Byte.MAX_VALUE - Byte.MIN_VALUE);

    try {
      final byte[] id = uid.getOrCreateId("foo");
      fail("IllegalArgumentException should have been thrown but instead "
           + " this was returned id=" + Arrays.toString(id));
    } catch (IllegalStateException e) {
      // OK.
    }

    // The +1 below is due to the whenFakeIcvThenReturn() hack.
    verify(client, times(2+1)).get(anyGet());// Initial Get + double check.
    verify(client).lockRow(anyRowLockRequest());      // The .maxid row.
    verify(client).unlockRow(fake_lock);     // The .maxid row.
  }

  @Test  // ICV throws an exception, we can't get an ID.
  public void getOrCreateIdWithICVFailure() {
    uid = new UniqueId(client, table, kind, 3);

    RowLock fake_lock = mock(RowLock.class);
    when(client.lockRow(anyRowLockRequest()))
      .thenReturn(Deferred.fromResult(fake_lock));

    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    // Update once HBASE-2292 is fixed:
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(MAXID, ID, kind_array, Bytes.fromLong(4L)));

    HBaseException hbe = fakeHBaseException();
    when(client.get(getForRow(MAXID)))
      .thenThrow(hbe)
      .thenReturn(Deferred.fromResult(kvs));

    when(client.put(anyPut()))
      .thenReturn(Deferred.fromResult(null));

    final byte[] id = { 0, 0, 5 };
    assertArrayEquals(id, uid.getOrCreateId("foo"));
    // The +2/+1 below are due to the whenFakeIcvThenReturn() hack.
    verify(client, times(4+2)).get(anyGet());  // Initial Get + double check x2.
    verify(client, times(2)).lockRow(anyRowLockRequest());   // The .maxid row x2.
    verify(client, times(2+1)).put(anyPut());         // Both mappings.
    verify(client, times(2)).unlockRow(fake_lock);  // The .maxid row x2.
  }

  @Test  // Test that the reverse mapping is created before the forward one.
  public void getOrCreateIdPutsReverseMappingFirst() {
    uid = new UniqueId(client, table, kind, 3);

    RowLock fake_lock = mock(RowLock.class);
    when(client.lockRow(anyRowLockRequest()))
      .thenReturn(Deferred.fromResult(fake_lock));

    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    when(client.put(anyPut()))
      .thenReturn(Deferred.fromResult(null));

    // Update once HBASE-2292 is fixed:
    whenFakeIcvThenReturn(5L);

    final byte[] id = { 0, 0, 6 };
    final byte[] row = { 'f', 'o', 'o' };
    assertArrayEquals(id, uid.getOrCreateId("foo"));

    final InOrder order = inOrder(client);
    order.verify(client).get(anyGet());            // Initial Get.
    order.verify(client).lockRow(anyRowLockRequest());  // The .maxid row.
    // Update once HBASE-2292 is fixed:
    // HACK HACK HACK
    order.verify(client).get(getForRow(new byte[] { 'f', 'o', 'o' }));
    order.verify(client).get(getForRow(MAXID));    // "ICV".
    order.verify(client).put(putForRow(MAXID));    // "ICV".
    // end HACK HACK HACK
    order.verify(client).put(putForRow(id));
    order.verify(client).put(putForRow(row));
    order.verify(client).unlockRow(fake_lock);     // The .maxid row.
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

  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private static GetRequest anyGet() {
    return any(GetRequest.class);
  }

  private static byte[] extractKey(final HBaseRpc rpc) {
    try {
      final Field key = HBaseRpc.class.getDeclaredField("key");
      key.setAccessible(true);
      return (byte[]) key.get(rpc);
    } catch (Exception e) {
      throw new RuntimeException("failed to extract the key out of " + rpc, e);
    }
  }

  private static GetRequest getForRow(final byte[] row) {
    return argThat(new ArgumentMatcher<GetRequest>() {
      public boolean matches(Object get) {
        return Arrays.equals(extractKey((GetRequest) get), row);
      }
      public void describeTo(org.hamcrest.Description description) {
        description.appendText("GetRequest for row " + Arrays.toString(row));
      }
    });
  }

  private static PutRequest anyPut() {
    return any(PutRequest.class);
  }

  private static PutRequest putForRow(final byte[] row) {
    return argThat(new ArgumentMatcher<PutRequest>() {
      public boolean matches(Object put) {
        return Arrays.equals(extractKey((PutRequest) put), row);
      }
      public void describeTo(org.hamcrest.Description description) {
        description.appendText("PutRequest for row " + Arrays.toString(row));
      }
    });
  }

  private static RowLockRequest anyRowLockRequest() {
    return any(RowLockRequest.class);
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

  /** Temporary hack until we can do proper ICVs -- see HBASE 2292. */
  private void whenFakeIcvThenReturn(final long value) {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(MAXID, ID, kind_array, Bytes.fromLong(value)));
    Deferred<ArrayList<KeyValue>> maxid_result = Deferred.fromResult(kvs);
    when(client.get(getForRow(MAXID)))
      .thenReturn(maxid_result);
    when(client.put(anyPut()))
      .thenReturn(Deferred.fromResult(null));
  }

}
