package net.opentsdb.storage;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;

@Ignore
public class TestHBaseStore extends TestTsdbStore {
  private static final byte[] MAX_UID = { 0 };

  private HBaseClient client;

  @Before
  public void setUp() throws IOException {
    // why.... why..... why final!?
    client = mock(HBaseClient.class);
    tsdb_store = new HBaseStore(client, new Config(false));
  }

  @Test
  // Test the creation of an ID when all possible IDs are already in use
  public void getOrCreateIdWithOverflow() throws Exception {
    // Update once HBASE-2292 is fixed:
    when(client.atomicIncrement(incrementForRow(MAX_UID)))
            .thenReturn(Deferred.fromResult(256L));

    try {
      Deferred<byte[]> uid = tsdb_store.allocateUID(new byte[]{'f', 'o', 'o'},
              UniqueId.UniqueIdType.METRIC,
              UniqueId.UniqueIdType.METRIC.width);

      fail("IllegalArgumentException should have been thrown but instead "
              + " this was returned id=" + uid.joinUninterruptibly());
    } catch(IllegalStateException e) {
      assertTrue(e.getMessage().startsWith("All Unique IDs for "));
    }
  }

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

  /*
  @Test  // Test that the reverse mapping is created before the forward one.
  public void getOrCreateIdPutsReverseMappingFirst() {
    uid = new UniqueId(client, table, UniqueId.UniqueIdType.METRIC);
    final Config config = mock(Config.class);
    when(config.enable_realtime_uid()).thenReturn(false);
    final TSDB tsdb = mock(TSDB.class);
    when(tsdb.getConfig()).thenReturn(config);
    uid.setTSDB(tsdb);

    when(client.get(anyGet()))      // null  =>  ID doesn't exist.
            .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(null));
    // Watch this! ______,^   I'm writing C++ in Java!

    when(client.atomicIncrement(incrementForRow(MAX_UID)))
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

  @PrepareForTest({HBaseStore.class, UniqueId.class})
  @Test  // Test the creation of an ID when unable to increment MAXID
  public void getOrCreateIdUnableToIncrementMaxId() throws Exception {
    PowerMockito.mockStatic(Thread.class);

    uid = new UniqueId(client, table, UniqueIdType.METRIC);

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
}
