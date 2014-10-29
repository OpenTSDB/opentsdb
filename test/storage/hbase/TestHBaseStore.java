package net.opentsdb.storage.hbase;

import com.stumbleupon.async.Deferred;

import net.opentsdb.storage.TestTsdbStore;
import net.opentsdb.uid.UniqueIdType;
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
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;


@RunWith(PowerMockRunner.class)
@PrepareForTest(HBaseClient.class)
public class TestHBaseStore extends TestTsdbStore {
  private static final byte[] MAX_UID = {0};

  private HBaseClient client;
  private String foo_name;

  @Before
  public void setUp() throws IOException {
    client = PowerMockito.mock(HBaseClient.class);
    tsdb_store = new HBaseStore(client, new Config(false));
    foo_name = "foo";
  }

  @Test(expected = NullPointerException.class)
  // Test what happens when config is Null
  public void constructorWithNullConfig() {
    new HBaseStore(client, null);
  }

  @Test(expected = NullPointerException.class)
  // Test what happens when config is Null
  public void constructorWithNullClient() throws IOException{
    new HBaseStore(null, new Config(false));
  }

  @PrepareForTest({Config.class, HBaseClient.class})
  @Test
  public void constructorWithValidConfig() throws IOException{

    short flush_interval = 50;
    when(client.getFlushInterval()).thenReturn(flush_interval);


    HBaseStore temp_store = new HBaseStore(client, new Config(false));

    assertEquals(flush_interval, temp_store.getFlushInterval());
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
      Deferred<byte[]> uid = tsdb_store.allocateUID(foo_name,
              UniqueIdType.METRIC
      );

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
      Deferred<byte[]> uid = tsdb_store.allocateUID(foo_name,
              UniqueIdType.METRIC);

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
      Deferred<byte[]> uid = tsdb_store.allocateUID(foo_name,
              UniqueIdType.METRIC
      );

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
    when(client.compareAndSet(anyPut(), emptyArray())).
            thenReturn(Deferred.fromResult(false));

    try {
      Deferred<byte[]> uid = tsdb_store.allocateUID(foo_name,
              UniqueIdType.METRIC
      );
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
    order.verify(client).compareAndSet(anyPut(),
            emptyArray());
    verify(client, times(1)).compareAndSet(anyPut(),
            emptyArray());
    verify(client, times(1)).atomicIncrement(incrementForRow(MAX_UID));
    verify(client, never()).get(any(GetRequest.class));
    verify(client, never()).put(anyPut());
  }


  @Test  // Test the creation of an ID when unable to increment MAXID
  public void allocateUIDUnableToIncrementMaxId() throws Exception {
    HBaseException hbe = fakeHBaseException();

    when(client.atomicIncrement(incrementForRow(MAX_UID)))
      .thenReturn(Deferred.<Long>fromError(hbe));
    // Watch this! ______,^   I'm writing C++ in Java!

    try {
      //need to join to get the exception to be thrown
      byte[] uid = tsdb_store.allocateUID(foo_name,
              UniqueIdType.METRIC
      ).joinUninterruptibly();
      fail("HBaseException should have been thrown!");
    } catch (HBaseException e) {
      assertSame(hbe,e);
    }
  }


  @Test  // Test the creation of an ID with a race condition.
  public void getOrCreateIdAssignIdWithRaceCondition() throws Exception{
  // Simulate a race between client A and client B.
  // A does a Get and sees that there's no ID for this name.
  // B does a Get and sees that there's no ID too, and B actually goes
  // through the entire process to create the ID.
  // Then A attempts to go through the process and should discover that the
  // ID has already been assigned.

  //make sur ethe atomicIncrement pass with valid long
  when(client.atomicIncrement(incrementForRow(MAX_UID))).thenReturn(Deferred.
    fromResult(6L));
  // First call to compareAndSet should return true, basically we would create
  // the reverse mapping.
  // However the second call to compare and set should return false, we fail to
  // create the forward mapping similar this is what should happen is another
  // client created the before the first instance was finished.
  // A warning should then be logged and a client.get request should happen.
  when(client.compareAndSet(anyPut(),emptyArray()))
          .thenReturn(Deferred.fromResult(true))
          .thenReturn(Deferred.fromResult(false));
  // For simplicity return an empty ArrayList, then the call back should return
  // null
  final byte[] value = {'1','2','3'};
  ArrayList<KeyValue> al = new ArrayList<KeyValue>();
  al.add(new KeyValue(new byte[] {'h', 'o', 's', 't'}, new byte[] {},
          new byte[] {}, value));
  when(client.get(any(GetRequest.class)))
          .thenReturn(Deferred.fromResult(al));


  byte[] uid = tsdb_store.allocateUID(foo_name, UniqueIdType.METRIC
  ).joinUninterruptibly();
  assertEquals(value,uid);

  // Verify the order of execution too.
  final InOrder order = inOrder(client);
  order.verify(client, times(1)).atomicIncrement(incrementForRow(MAX_UID));
  order.verify(client, times(2)).compareAndSet(anyPut(),
          emptyArray());
  order.verify(client, times(1)).get(any(GetRequest.class));

  }

  @Test  // ICV throws an exception, we can't get an ID.
  public void getOrCreateIdWithICVFailure() throws Exception{
  // Watch this! ______,^   I'm writing C++ in Java!

  when(client.atomicIncrement(incrementForRow(MAX_UID)))
    .thenReturn(Deferred.fromResult(5L));

  when(client.compareAndSet(anyPut(), emptyArray()))
    .thenReturn(Deferred.fromResult(true))
    .thenReturn(Deferred.fromResult(true));

    final byte[] id = { 0, 0, 5 };
    assertArrayEquals(id, tsdb_store.allocateUID(foo_name,
      UniqueIdType.METRIC)
      .joinUninterruptibly());
    verify(client, times(1)).atomicIncrement(incrementForRow(MAX_UID));
    // Reverse + forward mappings.
    verify(client, times(2)).compareAndSet(anyPut(),
            emptyArray());
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
  private static HBaseException fakeHBaseException() {
    final HBaseException hbe = mock(HBaseException.class);
    when(hbe.getStackTrace()).thenReturn(Arrays.copyOf(
            new RuntimeException().getStackTrace(), 3));
    when(hbe.getMessage())
            .thenReturn("fake exception");
    return hbe;
  }

  private byte[] emptyArray() {
    return eq(HBaseClient.EMPTY_ARRAY);
  }
  private PutRequest anyPut() {
    return any(PutRequest.class);
  }
}
