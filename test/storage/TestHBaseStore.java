package net.opentsdb.storage;

import com.stumbleupon.async.Deferred;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import org.hbase.async.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;


@RunWith(PowerMockRunner.class)
@PrepareForTest(HBaseClient.class)
public class TestHBaseStore extends TestTsdbStore {
  private static final byte[] MAX_UID = {0};

  private HBaseClient client;
  private byte[] foo_array;

  @Before
  public void setUp() throws IOException {
    client = PowerMockito.mock(HBaseClient.class);
    tsdb_store = new HBaseStore(client, new Config(false));
    foo_array = new byte[]{'f', 'o', 'o'};
  }

  @Test
  // Test the creation of an ID when all possible IDs are already in use
  public void allocateUIDWithNegativeUID() throws Exception {
    //If the atomicIncrement returns a negative id it is not valid should get
    // exception
    long id = -1;
    //mock negative answer from atomic increment
    when(client.atomicIncrement(incrementForRow(MAX_UID))).thenReturn(Deferred.
            fromResult(id));
    try {
      Deferred<byte[]> uid = tsdb_store.allocateUID(foo_array,
              UniqueId.UniqueIdType.METRIC,
              UniqueId.UniqueIdType.METRIC.width);

      fail("IllegalArgumentException should have been thrown but instead "
              + " this was returned id=" + uid.joinUninterruptibly());
    } catch (IllegalStateException e) {
      assertEquals("Got a negative ID from HBase: " + id, e.getMessage());
    }
  }

  @Test
  public void allocateUIDWithTooNarrowWidth() throws Exception {

    final short id_width = 9;
    long id = 16777216L;
    //mock overflow answer from atomic Increment since width is checked first
    when(client.atomicIncrement(incrementForRow(MAX_UID))).
            thenReturn(Deferred.fromResult(16777216L));
    final byte[] row = Bytes.fromLong(id);
    try {
      Deferred<byte[]> uid = tsdb_store.allocateUID(foo_array,
              UniqueId.UniqueIdType.METRIC, id_width);

      fail("IllegalArgumentException should have been thrown but instead "
              + " this was returned id=" + uid.joinUninterruptibly());
    } catch (IllegalStateException e) {
      String expected_msg = "row.length = " + row.length
              + " which is less than " + id_width
              + " for id=" + id
              + " row=" + Arrays.toString(row);
      //Check if we got the right exception
      assertEquals(expected_msg, e.getMessage());

    }
  }

  @Test
  // Test the creation of an ID when all possible IDs are already in use
  public void allocateUIDWithOverflow() throws Exception {
    //mock overflow answer from atomic Increment
    when(client.atomicIncrement(incrementForRow(MAX_UID))).thenReturn(Deferred.
            fromResult(16777216L));
    try {
      Deferred<byte[]> uid = tsdb_store.allocateUID(foo_array,
              UniqueId.UniqueIdType.METRIC,
              UniqueId.UniqueIdType.METRIC.width);

      fail("IllegalArgumentException should have been thrown but instead "
              + " this was returned id=" + uid.joinUninterruptibly());
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().startsWith("All Unique IDs for "));
    }
  }

  @Test  // Test that the reverse mapping is created before the forward one.
  public void allocateUIDPutsReverseMappingFirst() throws Exception{
    //mock valid
    when(client.atomicIncrement(incrementForRow(MAX_UID))).thenReturn(Deferred.
            fromResult(6L));
    // next is compare and set make it return false and catch the exception
    when(client.compareAndSet(any(PutRequest.class), eq(HBaseClient.EMPTY_ARRAY))).
            thenReturn(Deferred.fromResult(false));

    try {
      Deferred<byte[]> uid = tsdb_store.allocateUID(foo_array,
              UniqueId.UniqueIdType.METRIC,
              UniqueId.UniqueIdType.METRIC.width);
      fail("IllegalArgumentException should have been thrown but instead "
              + " this was returned id=" + uid.joinUninterruptibly());

    } catch(IllegalStateException e) {
      //validate we got the right exception
      //this together with verifying that compareAndSet has been run exactly
      //once will guarantee that nothing else was called.
      //Also make sure that the get was called or put.
      //These should fail due to the fact that they have not been mocked,
      //however we want to make sure in case someone decides to mock them and
      //change the implementation.
      assertTrue(e.getMessage().startsWith("CAS to create mapping when " +
              "allocating UID with request ") && e.getMessage().endsWith(" " +
              "failed. You should probably run a FSCK against the UID table."));

    }
    final InOrder order = inOrder(client);
    order.verify(client).atomicIncrement(incrementForRow(MAX_UID));
    order.verify(client).compareAndSet(any(PutRequest.class),
            eq(HBaseClient.EMPTY_ARRAY));
    verify(client, times(1)).compareAndSet(any(PutRequest.class),
            eq(HBaseClient.EMPTY_ARRAY));
    verify(client, times(1)).atomicIncrement(incrementForRow(MAX_UID));
    verify(client, never()).get(any(GetRequest.class));
    verify(client, never()).put(any(PutRequest.class));
  }


  @Test  // Test the creation of an ID when unable to increment MAXID
  public void allocateUIDUnableToIncrementMaxId() throws Exception {
    PowerMockito.mockStatic(Thread.class);

    uid = new UniqueId(client, table, UniqueIdType.METRIC);

    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    HBaseException hbe = fakeHBaseException();
    when(client.atomicIncrement(incrementForRow(MAX_UID)))
      .thenThrow(hbe);
    PowerMockito.doNothing().when(Thread.class); Thread.sleep(anyInt());

    try {
      uid.getOrCreateId("foo");
      fail("HBaseException should have been thrown!");
    } catch (HBaseException e) {
      assertSame(hbe, e);
    }
  }

  /*
  @Test  // Test the creation of an ID with a race condition.
  @PrepareForTest({HBaseStore.class, Deferred.class})
   public void getOrCreateIdAssignIdWithRaceCondition() {
    // Simulate a race between client A and client B.
    // A does a Get and sees that there's no ID for this name.
    // B does a Get and sees that there's no ID too, and B actually goes
    // through the entire process to create the ID.
    // Then A attempts to go through the process and should discover that the
    // ID has already been assigned.

    uid = new UniqueId(client, table, UniqueIdType.METRIC); // Used by client A.
    HBaseStore client_b = mock(HBaseStore.class); // For client B.
    final UniqueId uid_b = new UniqueId(client_b, table, UniqueIdType.METRIC);

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


  @Test  // ICV throws an exception, we can't get an ID.
  public void getOrCreateIdWithICVFailure() {
    uid = new UniqueId(client, table, UniqueIdType.METRIC);
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

  */
  private static AtomicIncrementRequest incrementForRow(final byte[] p_row) {
    return argThat(new ArgumentMatcher<AtomicIncrementRequest>() {
      public boolean matches(Object incr) {
        return Arrays.equals(((AtomicIncrementRequest) incr).key(), p_row);
      }

      public void describeTo(org.hamcrest.Description description) {
        description.appendText("AtomicIncrementRequest for row "
                + Arrays.toString(p_row));
      }
    });
  }
  private static HBaseException fakeHBaseException() {
    final HBaseException hbe = mock(HBaseException.class);
    when(hbe.getStackTrace()).thenReturn(Arrays.copyOf(
            new RuntimeException().getStackTrace(), 3));
    when(hbe.getMessage())
            .thenReturn("fake exception");
    return hbe;
  }
}
