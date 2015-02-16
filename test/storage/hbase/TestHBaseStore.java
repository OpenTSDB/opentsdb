package net.opentsdb.storage.hbase;

import com.stumbleupon.async.Deferred;

import dagger.ObjectGraph;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TestTsdbStore;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.TestBranch;
import net.opentsdb.tree.TestTree;
import net.opentsdb.tree.Tree;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static net.opentsdb.core.StringCoder.toBytes;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;


@RunWith(PowerMockRunner.class)
@PrepareForTest({HBaseClient.class, Scanner.class})
public class TestHBaseStore extends TestTsdbStore {
  private static final byte[] MAX_UID = {0};

  private HBaseClient client;
  private String foo_name;
  private Scanner scanner;

  @Before
  public void setUp() throws IOException {
    client = PowerMockito.mock(HBaseClient.class);
    tsdb_store = new HBaseStore(client, new Config(false));
    foo_name = "foo";
    scanner = PowerMockito.mock(Scanner.class);
  }

  @Test
  public void testFetchBranchLoadingMetricsUID() throws Exception {
    setupBranchHBaseStore(STORE_DATA);
    when(client.newScanner(anyBytes())).thenReturn(scanner);

    ArrayList<ArrayList<KeyValue>> valid_return = getValidReturn();
    when(scanner.nextRows()).thenReturn(
            Deferred.fromResult(valid_return))
            .thenReturn(
                    Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));

    ArrayList<KeyValue> ans1 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans2 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans3 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans4 = new ArrayList<KeyValue>();

    KeyValue kv = new KeyValue(new byte[0], new byte[0], new byte[0],
            "user".getBytes());
    ans1.add(kv);
    kv = new KeyValue(new byte[0], new byte[0], new byte[0], "tagk".getBytes());
    ans2.add(kv);
    kv = new KeyValue(new byte[0], new byte[0], new byte[0], "tagv".getBytes());
    ans3.add(kv);
    kv = new KeyValue(new byte[0], new byte[0], new byte[0], "nice".getBytes());
    ans4.add(kv);

    when(client.get(anyGet())).thenReturn(Deferred.fromResult(ans1))
            .thenReturn(Deferred.fromResult(ans2))
            .thenReturn(Deferred.fromResult(ans3))
            .thenReturn(Deferred.fromResult(ans4))
            .thenReturn(Deferred.fromResult(ans2))
            .thenReturn(Deferred.fromResult(ans3));

    super.testFetchBranchLoadingMetricsUID();
  }

  @Test
  public void testFetchBranch() throws Exception {
    setupBranchHBaseStore(STORE_DATA);
    when(client.newScanner(anyBytes())).thenReturn(scanner);

    ArrayList<ArrayList<KeyValue>> valid_return = getValidReturn();
    when(scanner.nextRows()).thenReturn(
            Deferred.fromResult(valid_return))
            .thenReturn(
                    Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
    super.testFetchBranch();
  }
  @Test
  public void testFetchBranchNotFound() throws Exception {
    setupBranchHBaseStore(STORE_DATA);
    when(client.newScanner(anyBytes())).thenReturn(scanner);
    when(scanner.nextRows()).thenReturn(
            Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
    //Could I have some C in^ that Java?
    super.testFetchBranchNotFound();
  }

  @Test
  public void testFetchBranchOnly() throws Exception {
    setupBranchHBaseStore(STORE_DATA);

    ArrayList<ArrayList<KeyValue>> valid_return = getValidReturn();

    when(client.get(anyGet())).thenReturn(
            Deferred.fromResult(valid_return.get(0)));
    super.testFetchBranchOnly();
  }

  @Test
  public void testFetchBranchOnlyNotFound() throws Exception {
    setupBranchHBaseStore(STORE_DATA);
    when(client.get(anyGet())).thenReturn(
            Deferred.<ArrayList<KeyValue>>fromResult(null));
    //Could I have sôme C in that Java?
    super.testFetchBranchOnlyNotFound();
  }
  @Test
  public void testStoreBranch() throws Exception {
    setupBranchHBaseStore(!STORE_DATA);

    //answer is mocked so this compareAndSet does not do anything really
    when(client.compareAndSet(anyPut(),emptyArray())).
            thenReturn(Deferred.fromResult(true));

    when(client.newScanner(anyBytes())).thenReturn(scanner);

    ArrayList<ArrayList<KeyValue>> valid_return =
            new ArrayList<ArrayList<KeyValue>>();

    ArrayList<KeyValue> ans = new ArrayList<KeyValue>();

    //answer
    KeyValue kv = new KeyValue(
            Branch.stringToId("0001"), new byte[0],
            toBytes("branch"),
            jsonMapper.writeValueAsBytes(TestBranch.buildTestBranch(tree)));
    ans.add(kv);

    valid_return.add(ans);

    when(scanner.nextRows()).thenReturn(Deferred.fromResult(valid_return))
            .thenReturn(
                    Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
    super.testStoreBranch();
  }

  @Test
  public void testStoreBranchExistingLeaf() throws Exception {
    setupBranchHBaseStore(!STORE_DATA);//test for HBaseStore

    /*Setup data*/
    when(client.compareAndSet(anyPut(), emptyArray()))
            .thenReturn(Deferred.fromResult(true));
    final Branch branch = TestBranch.buildTestBranch(tree);
    tsdb_store.storeBranch(tree, branch, true);

    when(client.compareAndSet(anyPut(), emptyArray()))
            .thenReturn(Deferred.fromResult(false))
            .thenReturn(Deferred.fromResult(true));

    ArrayList<KeyValue> ans = new ArrayList<KeyValue>();
    //branches
    KeyValue kv = new KeyValue(
            branch.compileBranchId(), new byte[0],
            toBytes("branch"), jsonMapper.writeValueAsBytes(branch));
    ans.add(kv);

    when(client.get(anyGet())).thenReturn(
            Deferred.fromResult(ans));

    super.testStoreBranchExistingLeaf();
  }
  @Test
  public void testStoreBranchCollision() throws Exception {

    setupBranchHBaseStore(!STORE_DATA);

    /* Collision object */
    final Branch root = getLeafCollision(NOT_SAME_TSUID);
    final Branch branch = TestBranch.buildTestBranch(tree);

    //mock answers that should be generated
    KeyValue kv = new KeyValue(root.compileBranchId(), new byte[0],
            Leaf.LEAF_PREFIX(), jsonMapper.writeValueAsBytes(branch.getLeaves().first()));
    ArrayList<KeyValue> ans = new ArrayList<KeyValue>();
    ans.add(kv);

    /*Setup answer */
    when(client.compareAndSet(anyPut(), emptyArray()))
            .thenReturn(Deferred.fromResult(true))
            .thenReturn(Deferred.fromResult(true))
            .thenReturn(Deferred.fromResult(true))
            .thenReturn(Deferred.fromResult(false));

    when(client.get(anyGet())).thenReturn(Deferred.fromResult(ans));

    super.testStoreBranchCollision();
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

  @PrepareForTest({Config.class, HBaseClient.class, Scanner.class})
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
              + " this was returned id=" +
              Arrays.toString(uid.joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)));
    } catch (IllegalStateException e) {
      assertEquals("Got a negative ID from HBase: " + id, e.getMessage());
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
              + " this was returned id=" +
              Arrays.toString(uid.joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)));
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
              + " this was returned id=" +
              Arrays.toString(uid.joinUninterruptibly(MockBase.DEFAULT_TIMEOUT)));

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
      tsdb_store.allocateUID(foo_name,
              UniqueIdType.METRIC
      ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
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
  ).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
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
      .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    verify(client, times(1)).atomicIncrement(incrementForRow(MAX_UID));
    // Reverse + forward mappings.
    verify(client, times(2)).compareAndSet(anyPut(),
            emptyArray());
  }

  @Test
  public void testStoreLeaf() throws Exception {

    final Tree tree = TestTree.buildTestTree();
    final Branch branch = TestBranch.buildTestBranch(tree);
    final Leaf leaf = new Leaf("Leaf", "000002000002000002");

    when(client.compareAndSet(anyPut(), eq(new byte[0])))
            .thenReturn(Deferred.fromResult(true));

    assertTrue(tsdb_store.storeLeaf(leaf, branch, tree)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void testStoreLeafDataCorruptEmptyArray() throws Exception {

    final Tree tree = TestTree.buildTestTree();
    final Branch branch = TestBranch.buildTestBranch(tree);
    final Leaf leaf = new Leaf("Leaf", "000002000002000002");

    when(client.compareAndSet(anyPut(), eq(new byte[0])))
            .thenReturn(Deferred.fromResult(false));

    when(client.get(anyGet())).thenReturn(
            Deferred.fromResult(new ArrayList<KeyValue>()));

    assertFalse(tsdb_store.storeLeaf(leaf, branch, tree)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    Map<String, String> collisions = tree.getCollisions();
    assertNull(collisions);
  }

  @Test
  public void testStoreLeafDataCorruptNullAnswer() throws Exception {

    final Tree tree = TestTree.buildTestTree();
    final Branch branch = TestBranch.buildTestBranch(tree);
    final Leaf leaf = new Leaf("Leaf", "000002000002000002");

    when(client.compareAndSet(anyPut(), eq(new byte[0])))
            .thenReturn(Deferred.fromResult(false));

    when(client.get(anyGet())).thenAnswer(newDeferred(null));

    assertFalse(tsdb_store.storeLeaf(leaf, branch, tree)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    Map<String, String> collisions = tree.getCollisions();
    assertNull(collisions);
  }
  @Test
  public void testStoreLeafLeafAlreadyExists() throws Exception {

    final Tree tree = TestTree.buildTestTree();
    final Branch branch = TestBranch.buildTestBranch(tree);
    final Leaf leaf = new Leaf("Leaf", "000002000002000002");

    when(client.compareAndSet(anyPut(), eq(new byte[0])))
            .thenReturn(Deferred.fromResult(false));

    ArrayList<KeyValue> answer = new ArrayList<KeyValue>(1);

    answer.add(new KeyValue(
            new byte[0], new byte[0], new byte[0], jsonMapper.writeValueAsBytes(leaf)));

    when(client.get(anyGet())).thenReturn(Deferred.fromResult(answer));

    assertTrue(tsdb_store.storeLeaf(leaf, branch, tree)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void testStoreLeafCollision() throws Exception {

    final Tree tree = TestTree.buildTestTree();
    final Branch branch = TestBranch.buildTestBranch(tree);
    final Leaf leaf = new Leaf("Leaf", "000002000002000002");

    final Leaf existing_leaf = new Leaf("Leaf", "000002000002000001");

    when(client.compareAndSet(anyPut(), eq(new byte[0])))
            .thenReturn(Deferred.fromResult(false));

    ArrayList<KeyValue> answer = new ArrayList<KeyValue>(1);

    answer.add(
            new KeyValue(
            new byte[0], new byte[0], new byte[0],
                    jsonMapper.writeValueAsBytes(existing_leaf)));

    when(client.get(anyGet())).thenReturn(Deferred.fromResult(answer));

    assertFalse(tsdb_store.storeLeaf(leaf, branch, tree)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    /*Check that collision was properly set*/
    Map<String, String> collisions = tree.getCollisions();
    assertEquals(1, collisions.size());
    assertEquals("000002000002000001",collisions.get("000002000002000002"));
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


  /**
   * The tests named parseFromStorage tests the private function getLeaf()
   * Previously this was a public method on the leaf that was called
   * parseFromStorage(), this the name convention is left.
   *
   * @throws Exception
   */
  @Test
  public void testParseFromStorage() throws Exception {
    setupBranchHBaseStore(STORE_DATA);
    when(client.newScanner(anyBytes())).thenReturn(scanner);

    ArrayList<ArrayList<KeyValue>> valid_return = getValidReturn();
    when(scanner.nextRows()).thenReturn(
            Deferred.fromResult(valid_return))
            .thenReturn(
                    Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));

    ArrayList<KeyValue> ans1 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans2 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans3 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans4 = new ArrayList<KeyValue>();

    KeyValue kv;
    kv = new KeyValue(new byte[0], new byte[0], new byte[0],
            "sys.cpu.0".getBytes());
    ans1.add(kv);
    kv = new KeyValue(new byte[0], new byte[0], new byte[0], "host".getBytes());
    ans2.add(kv);
    kv = new KeyValue(new byte[0], new byte[0], new byte[0],
            "web01".getBytes());
    ans3.add(kv);
    kv = new KeyValue(new byte[0], new byte[0], new byte[0], "nice".getBytes());
    ans4.add(kv);

    when(client.get(anyGet())).thenReturn(Deferred.fromResult(ans1))
            .thenReturn(Deferred.fromResult(ans2))
            .thenReturn(Deferred.fromResult(ans3))
            .thenReturn(Deferred.fromResult(ans4))
            .thenReturn(Deferred.fromResult(ans2))
            .thenReturn(Deferred.fromResult(ans3));
    super.testParseFromStorage();
  }

  @Test
  public void testParseFromStorageNSUMetric() throws Throwable {
    setupBranchHBaseStore(STORE_DATA);
    when(client.newScanner(anyBytes())).thenReturn(scanner);

    ArrayList<ArrayList<KeyValue>> valid_return = getValidReturn();
    when(scanner.nextRows()).thenReturn(
            Deferred.fromResult(valid_return))
            .thenReturn(
                    Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));

    ArrayList<KeyValue> ans1 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans2 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans3 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans4 = new ArrayList<KeyValue>();

    KeyValue kv;
    kv = new KeyValue(new byte[0], new byte[0], new byte[0], "host".getBytes());
    ans2.add(kv);
    kv = new KeyValue(new byte[0], new byte[0], new byte[0],
            "web01".getBytes());
    ans3.add(kv);
    kv = new KeyValue(new byte[0], new byte[0], new byte[0], "nice".getBytes());
    ans4.add(kv);

    when(client.get(anyGet())).thenReturn(Deferred.fromResult(ans1))
            .thenReturn(Deferred.fromResult(ans2))
            .thenReturn(Deferred.fromResult(ans3))
            .thenReturn(Deferred.fromResult(ans4))
            .thenReturn(Deferred.fromResult(ans2))
            .thenReturn(Deferred.fromResult(ans3));

    super.testParseFromStorageNSUMetric();
  }

  @Test
  public void testParseFromStorageNSUTagk() throws Throwable {
    setupBranchHBaseStore(STORE_DATA);
    when(client.newScanner(anyBytes())).thenReturn(scanner);

    ArrayList<ArrayList<KeyValue>> valid_return = getValidReturn();
    when(scanner.nextRows()).thenReturn(
            Deferred.fromResult(valid_return))
            .thenReturn(
                    Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));

    ArrayList<KeyValue> ans1 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans2 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans3 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans4 = new ArrayList<KeyValue>();

    KeyValue kv;
    kv = new KeyValue(new byte[0], new byte[0], new byte[0],
            "sys.cpu.0".getBytes());
    ans1.add(kv);
    kv = new KeyValue(new byte[0], new byte[0], new byte[0],
            "web01".getBytes());
    ans3.add(kv);
    kv = new KeyValue(new byte[0], new byte[0], new byte[0], "nice".getBytes());
    ans4.add(kv);

    when(client.get(anyGet())).thenReturn(Deferred.fromResult(ans1))
            .thenReturn(Deferred.fromResult(ans2))
            .thenReturn(Deferred.fromResult(ans3))
            .thenReturn(Deferred.fromResult(ans4))
            .thenReturn(Deferred.fromResult(ans2))
            .thenReturn(Deferred.fromResult(ans3));
    super.testParseFromStorageNSUTagk();
  }

  @Test
  public void testParseFromStorageNSUTagV() throws Throwable {
    setupBranchHBaseStore(STORE_DATA);
    when(client.newScanner(anyBytes())).thenReturn(scanner);

    ArrayList<ArrayList<KeyValue>> valid_return = getValidReturn();
    when(scanner.nextRows()).thenReturn(
            Deferred.fromResult(valid_return))
            .thenReturn(
                    Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));

    ArrayList<KeyValue> ans1 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans2 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans3 = new ArrayList<KeyValue>();
    ArrayList<KeyValue> ans4 = new ArrayList<KeyValue>();

    KeyValue kv;
    kv = new KeyValue(new byte[0], new byte[0], new byte[0],
            "sys.cpu.0".getBytes());
    ans1.add(kv);
    kv = new KeyValue(new byte[0], new byte[0], new byte[0], "host".getBytes());
    ans2.add(kv);
    kv = new KeyValue(new byte[0], new byte[0], new byte[0], "nice".getBytes());
    ans4.add(kv);

    when(client.get(anyGet())).thenReturn(Deferred.fromResult(ans1))
            .thenReturn(Deferred.fromResult(ans2))
            .thenReturn(Deferred.fromResult(ans3))
            .thenReturn(Deferred.fromResult(ans4))
            .thenReturn(Deferred.fromResult(ans2))
            .thenReturn(Deferred.fromResult(ans3));

    super.testParseFromStorageNSUTagV();
  }
  @Test
  public void TestFetchBranchNSU() throws Exception {
    setupBranchHBaseStore(STORE_DATA);

    when(client.newScanner(anyBytes())).thenReturn(scanner);

    ArrayList<ArrayList<KeyValue>> valid_return = getValidReturn();

    // Modify the valid_return var as to un-match the Leaf!
    valid_return.get(0).set(2, new KeyValue(
                    Branch.stringToId("00010001BECD000181A8BF992A98"),
                    new byte[0],
                    toBytes("branch"),
                    jsonMapper.writeValueAsBytes(child_branch)));
    when(scanner.nextRows()).thenReturn(
            Deferred.fromResult(valid_return))
            .thenReturn(
                    Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));

    super.TestFetchBranchNSU();

    /*
    *This test was supposed to test the branch structure if it was not linked by
    * UID anymore. Thus a leaf should not be connected to the branch because
    * of lacking UID match. In the Memory store we keep these things separate.
    * No point in testing for MemoryStore.
    */

  }


  /** Creates a new Deferred that's already called back.  */
  private static <T> Answer<Deferred<T>> newDeferred(final T result) {
    return new Answer<Deferred<T>>() {
      public Deferred<T> answer(final InvocationOnMock invocation) {
        return Deferred.fromResult(result);
      }
    };
  }

  /**
   * Mocks HBase Branch stuff
   */
  private void setupBranchHBaseStore(final boolean store) throws Exception {
    client = PowerMockito.mock(HBaseClient.class);
    ObjectGraph objectGraph = ObjectGraph.create(new HBaseTestModule(client));
    tsdb_store = objectGraph.get(TsdbStore.class);
    tsdb = objectGraph.get(TSDB.class);

    setUpBranchesAndLeafs();
    if (!store)
      return;

    //since the answer is mocked this probably does not matter much
    when(client.compareAndSet(anyPut(), emptyArray()))
            .thenReturn(Deferred.fromResult(true));
    tsdb_store.storeBranch(tree, root_branch, false);
    tsdb_store.storeLeaf(root_leaf_one, root_branch, tree);
    tsdb_store.storeLeaf(root_leaf_two, root_branch, tree);
    tsdb_store.storeBranch(tree, child_branch, false);
    tsdb_store.storeLeaf(child_leaf_one, child_branch, tree);
  }
}
