package net.opentsdb.storage;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.storage.hbase.HBaseStore;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.TestBranch;
import net.opentsdb.tree.TestTree;
import net.opentsdb.tree.Tree;
import net.opentsdb.uid.UniqueIdType;

import net.opentsdb.utils.Config;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

import static net.opentsdb.core.StringCoder.toBytes;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HBaseClient.class, Scanner.class})
public abstract class TestTsdbStore {
  private static final boolean SAME_TSUID = true;
  protected TSDB tsdb;
  private HBaseClient client;
  protected TsdbStore tsdb_store;
  protected UIDMeta meta;
  protected Config config;

  /*Branch test*/
  protected Tree tree;
  protected Branch root_branch;
  protected Branch child_branch;
  protected Leaf root_leaf_one;
  protected Leaf root_leaf_two;
  protected Leaf child_leaf_one;

  protected static boolean STORE_DATA = true;


  /*BRANCH DATABASE STUFF*/
  /**
   * Sets up the branches
   */
  protected void setUpBranchesAndLeafs() {
    tree = TestTree.buildTestTree();
    root_branch = new Branch(1);
    TreeMap<Integer, String> path = new TreeMap<Integer, String>();
    path.put(0, "ROOT");
    path.put(1, "sys");
    path.put(2, "cpu");
    root_branch.prependParentPath(path);
    root_branch.setDisplayName("cpu");

    root_leaf_one = new Leaf("user", "000001000001000001");
    root_leaf_two = new Leaf("nice", "000002000002000002");

    child_branch  = new Branch(1);
    path.put(3, "mboard");
    child_branch.prependParentPath(path);
    child_branch.setDisplayName("mboard");
    child_leaf_one = new Leaf("Asus", "000003000003000003");
  }

  /**
   * Mocks classes for testing the storage calls
   */
  private void setupBranchMemoryStore(final boolean store) throws Exception {

    config = new Config(false);
    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, config);

    setUpBranchesAndLeafs();
    if (!store)
      return;

    tsdb_store.storeBranch(tree, root_branch, false);
    tsdb_store.storeLeaf(root_leaf_one, root_branch, tree);
    tsdb_store.storeLeaf(root_leaf_two, root_branch, tree);
    tsdb_store.storeBranch(tree, child_branch, false);
    tsdb_store.storeLeaf(child_leaf_one, child_branch, tree);
  }

  /**
   * Mocks HBase Branch stuff
   */
  private void setupBranchHBaseStore(final boolean store) throws Exception{

    config = new Config(false);
    client = PowerMockito.mock(HBaseClient.class);
    tsdb_store = new HBaseStore(client, config);
    tsdb = new TSDB(tsdb_store, config);

    setUpBranchesAndLeafs();
    if (!store)
      return;

    //since the answer is mocket this probably does not matter much
    when(client.compareAndSet(anyPut(), emptyArray()))
            .thenReturn(Deferred.fromResult(true));
    tsdb_store.storeBranch(tree, root_branch, false);
    tsdb_store.storeLeaf(root_leaf_one, root_branch, tree);
    tsdb_store.storeLeaf(root_leaf_two, root_branch, tree);
    tsdb_store.storeBranch(tree, child_branch, false);
    tsdb_store.storeLeaf(child_leaf_one, child_branch, tree);
  }

  /**
   * Use this method to get a Deferred with valid answers for the branch query.
   *
   * @return A valid return that the HBase database would return for the objects
   * specified by the @see {@link TestTsdbStore#setUpBranchesAndLeafs()}
   */
  protected ArrayList<ArrayList<KeyValue>> getValidReturn() {

    ArrayList<ArrayList<KeyValue>> valid_return =
            new ArrayList<ArrayList<KeyValue>>();

    ArrayList<KeyValue> ans = new ArrayList<KeyValue>();
    //branches
    KeyValue kv = new KeyValue(
            Branch.stringToId("00010001BECD000181A8"), new byte[0],
            toBytes("branch"), root_branch.toStorageJson());
    ans.add(kv);
    kv = new KeyValue(
            Branch.stringToId("00010001BECD000181A8BF992A99"), new byte[0],
            toBytes("branch"), child_branch.toStorageJson());
    ans.add(kv);
    //leaves
    kv = new KeyValue( Branch.stringToId("00010001BECD000181A8"), new byte[0],
            Leaf.LEAF_PREFIX(), root_leaf_one.getStorageJSON());
    ans.add(kv);
    kv = new KeyValue( Branch.stringToId("00010001BECD000181A8"), new byte[0],
            Leaf.LEAF_PREFIX(), root_leaf_two.getStorageJSON());
    ans.add(kv);
    kv = new KeyValue(
            Branch.stringToId("00010001BECD000181A8BF992A99"), new byte[0],
            Leaf.LEAF_PREFIX(), child_leaf_one.getStorageJSON());
    ans.add(kv);

    valid_return.add(ans);
    return valid_return;
  }

  @Test
  public void testFetchBranchLoadingMetrics() throws Exception {
  fail();

  /*
   * This test should test fetching a branch and loading the metrics.
   * The testFetchBranch does not load because the leafs currently does not have
   * a metric set. This must be looked at carefully...
   */

  }

  @Test
  public void testFetchBranch() throws Exception {
    final Branch branch = tsdb_store.fetchBranch(
            Branch.stringToId("00010001BECD000181A8"),
            false, tsdb).joinUninterruptibly();
    assertNotNull(branch);
    assertEquals(1, branch.getTreeId());
    assertEquals("cpu", branch.getDisplayName());
    assertEquals("00010001BECD000181A8", branch.getBranchId());
    assertEquals(1, branch.getBranches().size());
    assertEquals(2, branch.getLeaves().size());
  }

  @Test
  public void testFetchBranchNotFound() throws Exception {
    Branch branch = tsdb_store.fetchBranch(
            Branch.stringToId("00010001BECD000181A0"),
            false, tsdb).joinUninterruptibly();
    assertNull(branch);
  }

  @Test
  public void testFetchBranchOnly() throws Exception {
    final Branch branch = tsdb_store.fetchBranchOnly(
            Branch.stringToId("00010001BECD000181A8")).joinUninterruptibly();
    assertNotNull(branch);
    assertEquals("cpu", branch.getDisplayName());
    assertNull(branch.getLeaves());
    assertNull(branch.getBranches());

    setupBranchHBaseStore(STORE_DATA);
  }

  @Test
  public void testFetchBranchOnlyNotFound() throws Exception {
    final Branch branch = tsdb_store.fetchBranchOnly(
            Branch.stringToId("00010001BECD000181A0")).joinUninterruptibly();
    assertNull(branch);
  }

  @Test
  public void testStoreBranch() throws Exception {
    final Branch branch = TestBranch.buildTestBranch(tree);
    tsdb_store.storeBranch(tree, branch, true);
    final Branch parsed =
            tsdb_store.fetchBranch(branch.compileBranchId(), true, tsdb)
            .joinUninterruptibly();
    assertEquals("ROOT", parsed.getDisplayName());
    assertEquals(1, parsed.getTreeId());
    assertNotNull(parsed.getPath());
  }

  @Test
  public void testStoreBranchExistingLeaf() throws Exception {

    final Branch root = getLeafCollision(SAME_TSUID);

    ArrayList<Boolean> results =
            tsdb_store.storeBranch(tree, root, true).joinUninterruptibly();

    assertEquals(2, results.size());
    assertFalse(results.get(0));
    assertTrue(results.get(1));
    assertNull(tree.getCollisions());


    final Branch parsed = tsdb_store
            .fetchBranchOnly(
            TestBranch.buildTestBranch(tree)
            .compileBranchId()).joinUninterruptibly();
    parsed.setTreeId(1);
    assertEquals("ROOT", parsed.getDisplayName());
  }

  @Test
  public void testStoreBranchCollision() throws Exception {

    setupBranchMemoryStore(!STORE_DATA);//test for memory store

    /*Collision object*/
    final TreeMap<Integer, String> root_path = new TreeMap<Integer, String>();
    final Branch root = new Branch(tree.getTreeId());
    root.setDisplayName("ROOT");
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    Leaf leaf = new Leaf("Alarms", "0101");//collision leaf not same tsuid
    root.addLeaf(leaf, tree);


    /*Setup data*/
    final Branch branch = TestBranch.buildTestBranch(tree);
    tsdb_store.storeBranch(tree, branch, true);

    final ArrayList<Boolean> results =
            tsdb_store.storeBranch(tree, root, true).joinUninterruptibly();

    assertEquals(2, results.size());
    assertFalse(results.get(0));
    assertFalse(results.get(1));
    assertEquals(1, tree.getCollisions().size());

    //HBaseStore test part
    setupBranchHBaseStore(!STORE_DATA);

    /*Setup data*/
    when(client.compareAndSet(anyPut(), emptyArray()))
            .thenReturn(Deferred.fromResult(true));
    tsdb_store.storeBranch(tree, branch, true);


    //mock answers that should be generated
    KeyValue kv = new KeyValue(root.compileBranchId(), new byte[0],
            Leaf.LEAF_PREFIX(), branch.getLeaves().first().getStorageJSON());
    ArrayList<KeyValue> ans = new ArrayList<KeyValue>();
    ans.add(kv);
    when(client.get(anyGet())).thenReturn(Deferred.fromResult(ans));
    when(client.compareAndSet(anyPut(), emptyArray()))
            .thenReturn(Deferred.fromResult(false));

    final ArrayList<Boolean> HBase_results =
            tsdb_store.storeBranch(tree, root, true).joinUninterruptibly();

    assertEquals(2, HBase_results.size());
    assertFalse(HBase_results.get(0));
    assertFalse(HBase_results.get(1));
    assertEquals(1, tree.getCollisions().size());
  }

  /*META TESTS*/

  @Test
  public void testGetMetaNullCell() throws IOException {
    tsdb_store.getMeta(new byte[]{0, 0, 1}, "derp", UniqueIdType.TAGK);
  }

  protected byte[] emptyArray() {
    return eq(HBaseClient.EMPTY_ARRAY);
  }
  protected PutRequest anyPut() {
    return any(PutRequest.class);
  }
  protected GetRequest anyGet() {
    return any(GetRequest.class);
  }
  protected byte[] anyBytes() { return any(byte[].class); }

  protected Branch getLeafCollision(boolean sameTsuid) {
    /*Collision object*/
    final TreeMap<Integer, String> root_path = new TreeMap<Integer, String>();
    final Branch root = new Branch(tree.getTreeId());
    root.setDisplayName("ROOT");
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    Leaf leaf = new Leaf("Alarms", "ABCD");//collision leaf but with same tsuid
    if (!sameTsuid)
      leaf.setTsuid("0101");
    root.addLeaf(leaf, tree);

    return root;
  }
}
