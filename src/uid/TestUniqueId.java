// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.uid;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.util.Bytes;

import net.opentsdb.HBaseException;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.*;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public final class TestUniqueId {

  @Mock private HTableInterface table;
  private UniqueId uid;
  private static final String kind = "kind";
  private static final byte[] kind_array = { 'k', 'i', 'n', 'd' };

  @Test(expected=IllegalArgumentException.class)
  public void testCtorZeroWidth() {
    uid = new UniqueId(table, kind, 0);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCtorNegativeWidth() {
    uid = new UniqueId(table, kind, -1);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCtorEmptyKind() {
    uid = new UniqueId(table, "", 3);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCtorLargeWidth() {
    uid = new UniqueId(table, kind, 9);
  }

  @Test
  public void kindEqual() {
    uid = new UniqueId(table, kind, 3);
    assertEquals(kind, uid.kind());
  }

  @Test
  public void widthEqual() {
    uid = new UniqueId(table, kind, 3);
    assertEquals(3, uid.width());
  }

  @Test
  public void getNameSuccessfulHBaseLookup() throws IOException {
    uid = new UniqueId(table, kind, 3);
    final byte[] id = { 0, 'a', 0x42 };
    final byte[] byte_name = { 'f', 'o', 'o' };

    Result fake_result = mock(Result.class);
    when(table.get(anyGet()))
      .thenReturn(fake_result);
    when(fake_result.getValue(anyBytes(), anyBytes()))
      .thenReturn(byte_name);

    assertEquals("foo", uid.getName(id));
    // Should be a cache hit ...
    assertEquals("foo", uid.getName(id));

    assertEquals(1, uid.cacheHits());
    assertEquals(1, uid.cacheMisses());
    assertEquals(2, uid.cacheSize());

    // ... so verify there was only one HBase Get.
    verify(table).get(anyGet());
  }

  @Test
  public void getNameWithErrorDuringHBaseLookup() throws IOException {
    uid = new UniqueId(table, kind, 3);
    final byte[] id = { 0, 'a', 0x42 };
    final byte[] byte_name = { 'f', 'o', 'o' };

    IOException ioe = new IOException("Fake Get failed.");

    Result fake_result = mock(Result.class);
    when(table.get(anyGet()))
      .thenThrow(ioe)
      .thenReturn(fake_result);

    when(fake_result.getValue(anyBytes(), anyBytes()))
      .thenReturn(byte_name);

    // 1st calls fails.
    try {
      uid.getName(id);
      fail("HBaseException should have been thrown.");
    } catch (HBaseException e) {
      // OK.
      assertEquals(ioe, e.getCause());
    }

    // 2nd call succeeds.
    assertEquals("foo", uid.getName(id));

    assertEquals(0, uid.cacheHits());
    assertEquals(2, uid.cacheMisses());  // 1st (failed) attempt + 2nd.
    assertEquals(2, uid.cacheSize());

    verify(table, times(2)).get(anyGet());
  }

  @Test(expected=NoSuchUniqueId.class)
  public void getNameForNonexistentId() throws IOException {
    uid = new UniqueId(table, kind, 3);

    Result fake_result = mock(Result.class);
    when(table.get(anyGet()))
      .thenReturn(fake_result);

    uid.getName(new byte[] { 1, 2, 3 });
  }

  @Test(expected=IllegalArgumentException.class)
  public void getNameWithInvalidId() {
    uid = new UniqueId(table, kind, 3);

    uid.getName(new byte[] { 1 });
  }

  @Test
  public void getIdSuccessfulHBaseLookup() throws IOException {
    uid = new UniqueId(table, kind, 3);
    final byte[] id = { 0, 'a', 0x42 };

    Result fake_result = mock(Result.class);
    when(table.get(anyGet()))
      .thenReturn(fake_result);
    when(fake_result.getValue(anyBytes(), anyBytes()))
      .thenReturn(id);

    assertArrayEquals(id, uid.getId("foo"));
    // Should be a cache hit ...
    assertArrayEquals(id, uid.getId("foo"));
    // Should be a cache hit too ...
    assertArrayEquals(id, uid.getId("foo"));

    assertEquals(2, uid.cacheHits());
    assertEquals(1, uid.cacheMisses());
    assertEquals(2, uid.cacheSize());

    // ... so verify there was only one HBase Get.
    verify(table).get(anyGet());
  }

  // The table contains IDs encoded on 2 bytes but the instance wants 3.
  @Test(expected=IllegalStateException.class)
  public void getIdMisconfiguredWidth() throws IOException {
    uid = new UniqueId(table, kind, 3);
    final byte[] id = { 'a', 0x42 };

    Result fake_result = mock(Result.class);
    when(table.get(anyGet()))
      .thenReturn(fake_result);
    when(fake_result.getValue(anyBytes(), anyBytes()))
      .thenReturn(id);

    uid.getId("foo");
  }

  @Test(expected=NoSuchUniqueName.class)
  public void getIdForNonexistentName() throws IOException {
    uid = new UniqueId(table, kind, 3);

    Result fake_result = mock(Result.class);
    when(table.get(anyGet()))
      .thenReturn(fake_result);

    uid.getId("foo");
  }

  @Test
  public void getOrCreateIdWithExistingId() throws IOException {
    uid = new UniqueId(table, kind, 3);
    final byte[] id = { 0, 'a', 0x42 };

    Result fake_result = mock(Result.class);
    when(table.get(anyGet()))
      .thenReturn(fake_result);
    when(fake_result.getValue(anyBytes(), anyBytes()))
      .thenReturn(id);

    assertArrayEquals(id, uid.getOrCreateId("foo"));
    // Should be a cache hit ...
    assertArrayEquals(id, uid.getOrCreateId("foo"));
    assertEquals(1, uid.cacheHits());
    assertEquals(1, uid.cacheMisses());
    assertEquals(2, uid.cacheSize());

    // ... so verify there was only one HBase Get.
    verify(table).get(anyGet());
  }

  @Test  // Test the creation of an ID with no problem.
  public void getOrCreateIdAssignIdWithSuccess() throws IOException {
    uid = new UniqueId(table, kind, 3);
    final byte[] id = { 0, 0, 5 };

    RowLock fake_lock = mock(RowLock.class);
    when(table.lockRow(anyBytes()))
      .thenReturn(fake_lock);

    Result fake_result = mock(Result.class);
    when(table.get(anyGet()))
      .thenReturn(fake_result);
    when(fake_result.getValue(anyBytes(), anyBytes()))
      .thenReturn(null);  // This ID doens't exist.

    // Uncomment after HBASE-2292:
    //when(table.incrementColumnValue(anyBytes(), anyBytes(),
    //                                eq(kind_array), eq(1L)))
    //  .thenReturn(5L);
    // HACK HACK HACK
    whenFakeIcvThenReturn(4L);
    // end HACK HACK HACK

    assertArrayEquals(id, uid.getOrCreateId("foo"));
    // Should be a cache hit since we created that entry.
    assertArrayEquals(id, uid.getOrCreateId("foo"));
    // Should be a cache hit too for the same reason.
    assertEquals("foo", uid.getName(id));

    // The +1's below are due to the whenFakeIcvThenReturn() hack.
    verify(table, times(2+1)).get(anyGet());// Initial Get + double check.
    verify(table).lockRow(anyBytes());      // The .maxid row.
    verify(table, times(2+1)).put(anyPut());// reverse + forward mappings.
    verify(table).unlockRow(fake_lock);     // The .maxid row.
  }

  @PrepareForTest(UniqueId.class)
  @Test  // Test the creation of an ID when unable to acquire the row lock.
  public void getOrCreateIdUnableToAcquireRowLock() throws Exception {
    PowerMockito.mockStatic(Thread.class);

    uid = new UniqueId(table, kind, 3);

    Result fake_result = mock(Result.class);
    when(table.get(anyGet()))
      .thenReturn(fake_result);
    when(fake_result.getValue(anyBytes(), anyBytes()))
      .thenReturn(null);  // This ID doens't exist.

    IOException ioe = new IOException("Fake error while acquiring the RowLock");
    when(table.lockRow(anyBytes()))
      .thenThrow(ioe);
    PowerMockito.doNothing().when(Thread.class); Thread.sleep(anyInt());

    try {
      uid.getOrCreateId("foo");
      fail("HBaseException should have been thrown!");
    } catch (HBaseException e) {
      assertSame(ioe, e.getCause());
    }
  }

  @Test  // Test the creation of an ID with a race condition.
  public void getOrCreateIdAssignIdWithRaceCondition() throws IOException {
    // Simulate a race between client A and client B.
    // A does a Get and sees that there's no ID for this name.
    // B does a Get and sees that there's no ID too, and B actually goes
    // through the entire process to create the ID.
    // Then A attempts to go through the process and should discover that the
    // ID has already been assigned.

    uid = new UniqueId(table, kind, 3);  // Used by client A.
    HTableInterface table_b = mock(HTableInterface.class);
    final UniqueId uid_b = new UniqueId(table_b, kind, 3);  // for client B.

    final byte[] id = { 0, 0, 5 };

    Result fake_result_a = mock(Result.class);
    when(table.get(anyGet()))
      .thenReturn(fake_result_a);

    final Answer the_race = new Answer<byte[]>() {
      public byte[] answer(final InvocationOnMock unused_invocation) {
        // While answering A's first Get, B doest a full getOrCreateId.
        assertArrayEquals(id, uid_b.getOrCreateId("foo"));
        return null;
      }
    };

    when(fake_result_a.getValue(anyBytes(), anyBytes()))
      .thenAnswer(the_race)  // Start the race when answering A's first Get.
      .thenReturn(id);       // The 2nd Get succeeds because B created the ID.

    RowLock fake_lock_a = mock(RowLock.class);
    when(table.lockRow(anyBytes()))
      .thenReturn(fake_lock_a);

    Result fake_result_b = mock(Result.class);
    when(table_b.get(anyGet()))
      .thenReturn(fake_result_b);

    when(fake_result_b.getValue(anyBytes(), anyBytes()))
      .thenReturn(null);

    RowLock fake_lock_b = mock(RowLock.class);
    when(table_b.lockRow(anyBytes()))
      .thenReturn(fake_lock_b);

    // Uncomment after HBASE-2292:
    //when(table_b.incrementColumnValue(anyBytes(), anyBytes(),
    //                                  eq(kind_array), eq(1L)))
    //  .thenReturn(5L);
    // HACK HACK HACK
    Result maxid_result = mock(Result.class);
    when(table_b.get(getForRow(MAXID)))
      .thenReturn(maxid_result);
    when(maxid_result.getValue(anyBytes(), anyBytes()))
      .thenReturn(Bytes.toBytes(4L));
    // end HACK HACK HACK

    // Start the execution.
    assertArrayEquals(id, uid.getOrCreateId("foo"));

    // The +1's below are due to the whenFakeIcvThenReturn() hack.
    // Verify the order of execution too.
    final InOrder order = inOrder(table, table_b);
    order.verify(table).get(anyGet());             // 1st Get for A.
    order.verify(table_b).get(anyGet());           // 1st Get for B.
    order.verify(table_b).lockRow(anyBytes());     // B starts the process...
    order.verify(table_b, times(1+1)).get(anyGet()); // double check for B.
    order.verify(table_b, times(2+1)).put(anyPut()); // both mappings.
    order.verify(table_b).unlockRow(fake_lock_b);  // ... B finishes.
    order.verify(table).lockRow(anyBytes());       // A starts the process...
    order.verify(table).get(anyGet());             // Finds the ID added by B
    order.verify(table).unlockRow(fake_lock_a);    // ... and stops here.
    // Things A shouldn't do because B did them already:
    verify(table, never()).incrementColumnValue(anyBytes(), anyBytes(),
                                                anyBytes(), anyLong());
    verify(table, never()).put(anyPut());
  }

  @Test
  // Test the creation of an ID when all possible IDs are already in use
  public void getOrCreateIdWithOverflow() throws IOException {
    uid = new UniqueId(table, kind, 1);  // IDs are only on 1 byte.

    RowLock fake_lock = mock(RowLock.class);
    when(table.lockRow(anyBytes()))
      .thenReturn(fake_lock);

    Result fake_result = mock(Result.class);
    when(table.get(anyGet()))
      .thenReturn(fake_result);
    when(fake_result.getValue(anyBytes(), anyBytes()))
      .thenReturn(null);  // This ID doens't exist.

    // Uncomment after HBASE-2292:
    //when(table.incrementColumnValue(anyBytes(), anyBytes(),
    //                                eq(kind_array), eq(1L)))
    //  // Return an ID that's too long to fit in 1 byte.
    //  .thenReturn(Byte.MAX_VALUE - Byte.MIN_VALUE + 1L);
    // HACK HACK HACK
    whenFakeIcvThenReturn(Byte.MAX_VALUE - Byte.MIN_VALUE);
    // end HACK HACK HACK

    try {
      final byte[] id = uid.getOrCreateId("foo");
      fail("IllegalArgumentException should have been thrown but instead "
           + " this was returned id=" + Arrays.toString(id));
    } catch (IllegalStateException e) {
      // OK.
    }

    // The +1 below is due to the whenFakeIcvThenReturn() hack.
    verify(table, times(2+1)).get(anyGet());// Initial Get + double check.
    verify(table).lockRow(anyBytes());      // The .maxid row.
    verify(table).unlockRow(fake_lock);     // The .maxid row.
  }

  @Test  // ICV throws an exception, we can't get an ID.
  public void getOrCreateIdWithICVFailure() throws IOException {
    uid = new UniqueId(table, kind, 3);

    RowLock fake_lock = mock(RowLock.class);
    when(table.lockRow(anyBytes()))
      .thenReturn(fake_lock);

    Result fake_result = mock(Result.class);
    when(table.get(anyGet()))
      .thenReturn(fake_result);
    when(fake_result.getValue(anyBytes(), anyBytes()))
      .thenReturn(null);  // This ID doens't exist.

    // Uncomment after HBASE-2292:
    //when(table.incrementColumnValue(anyBytes(), anyBytes(),
    //                                eq(kind_array), eq(1L)))
    //  .thenThrow(new IOException("Fake ICV failed"))
    //  .thenReturn(5L);
    // HACK HACK HACK
    Result maxid_result = mock(Result.class);
    when(table.get(getForRow(MAXID)))
      .thenThrow(new IOException("Fake ICV failed"))
      .thenReturn(maxid_result);
    when(maxid_result.getValue(anyBytes(), anyBytes()))
      .thenReturn(Bytes.toBytes(4L));
    // end HACK HACK HACK

    final byte[] id = { 0, 0, 5 };
    assertArrayEquals(id, uid.getOrCreateId("foo"));
    // The +2/+1 below are due to the whenFakeIcvThenReturn() hack.
    verify(table, times(4+2)).get(anyGet());  // Initial Get + double check x2.
    verify(table, times(2)).lockRow(anyBytes());   // The .maxid row x2.
    verify(table, times(2+1)).put(anyPut());         // Both mappings.
    verify(table, times(2)).unlockRow(fake_lock);  // The .maxid row x2.
  }

  @Test  // Test that the reverse mapping is created before the forward one.
  public void getOrCreateIdPutsReverseMappingFirst() throws IOException {
    uid = new UniqueId(table, kind, 3);

    RowLock fake_lock = mock(RowLock.class);
    when(table.lockRow(anyBytes()))
      .thenReturn(fake_lock);

    Result fake_result = mock(Result.class);
    when(table.get(anyGet()))
      .thenReturn(fake_result);
    when(fake_result.getValue(anyBytes(), anyBytes()))
      .thenReturn(null);

    // Uncomment after HBASE-2292:
    //when(table.incrementColumnValue(anyBytes(), anyBytes(),
    //                                eq(kind_array), eq(1L)))
    //  .thenReturn(6L);
    // HACK HACK HACK
    whenFakeIcvThenReturn(5L);
    // end HACK HACK HACK

    final byte[] id = { 0, 0, 6 };
    final byte[] row = { 'f', 'o', 'o' };
    assertArrayEquals(id, uid.getOrCreateId("foo"));

    final InOrder order = inOrder(table);
    order.verify(table).get(anyGet());            // Initial Get.
    order.verify(table).lockRow(anyBytes());      // The .maxid row.
    // Uncomment after HBASE-2292:
    //order.verify(table).get(anyGet());            // Double check.
    //order.verify(table).incrementColumnValue(anyBytes(), anyBytes(),
    //                                         anyBytes(), anyLong());
    // HACK HACK HACK
    order.verify(table).get(getForRow(new byte[] { 'f', 'o', 'o' }));
    order.verify(table).get(getForRow(MAXID));    // "ICV".
    order.verify(table).put(putForRow(MAXID));    // "ICV".
    // end HACK HACK HACK
    order.verify(table).put(putForRow(id));
    order.verify(table).put(putForRow(row));
    order.verify(table).unlockRow(fake_lock);     // The .maxid row.
  }

  private static Get anyGet() {
    return any(Get.class);
  }

  private static Get getForRow(final byte[] row) {
    return argThat(new ArgumentMatcher<Get>() {
      public boolean matches(Object get) {
        return Arrays.equals(((Get) get).getRow(), row);
      }
      public void describeTo(org.hamcrest.Description description) {
        description.appendText("Get for row " + Arrays.toString(row));
      }
    });
  }

  private static Put anyPut() {
    return any(Put.class);
  }

  private static Put putForRow(final byte[] row) {
    return argThat(new ArgumentMatcher<Put>() {
      public boolean matches(Object put) {
        return Arrays.equals(((Put) put).getRow(), row);
      }
      public void describeTo(org.hamcrest.Description description) {
        description.appendText("Put for row " + Arrays.toString(row));
      }
    });
  }

  private static byte[] anyBytes() {
    return any(byte[].class);
  }

  private static final byte[] MAXID = { 0 };

  /** Temporary hack until we can do proper ICVs -- see HBASE 2292. */
  private void whenFakeIcvThenReturn(final long value) throws IOException {
    Result maxid_result = mock(Result.class);
    when(table.get(getForRow(MAXID)))
      .thenReturn(maxid_result);
    when(maxid_result.getValue(anyBytes(), anyBytes()))
      .thenReturn(Bytes.toBytes(value));
  }

}
