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
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HBaseClient.class, Scanner.class})
public abstract class TestTsdbStore {
  private TSDB tsdb;
  private HBaseClient client;
  protected TsdbStore tsdb_store;
  protected UIDMeta meta;
  private Config config;

  /*Branch test*/
  private Tree tree = TestTree.buildTestTree();
  private Branch root_branch;
  private Branch child_branch;
  private Leaf root_leaf_one;
  private Leaf root_leaf_two;
  private Leaf child_leaf_one;



  @Test
  public void testGetMetaNullCell() throws IOException {
    tsdb_store.getMeta(new byte[]{0, 0, 1}, "derp", UniqueIdType.TAGK);
  }


  /*BRANCH DATABASE STUFF*/
  /**
   * Sets up the branches
   */
  private void setUpBranchesAndLeafs() {
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
  private void setupBranchMemoryStore() throws Exception {

    config = new Config(false);
    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, config);

    setUpBranchesAndLeafs();

    tsdb_store.storeBranch(tree, root_branch, false);
    tsdb_store.storeLeaf(root_leaf_one, root_branch, tree);
    tsdb_store.storeLeaf(root_leaf_two, root_branch, tree);
    tsdb_store.storeBranch(tree, child_branch, false);
    tsdb_store.storeLeaf(child_leaf_one, child_branch, tree);
  }

  /**
   * Mocks HBase Branch stuff
   */
  private void setupBranchHBaseStore() throws Exception{

    config = new Config(false);
    client = PowerMockito.mock(HBaseClient.class);
    tsdb_store = new HBaseStore(client, config);
    tsdb = new TSDB(tsdb_store, config);

    setUpBranchesAndLeafs();

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
  private ArrayList<ArrayList<KeyValue>> get_valid_return() {

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
  public void testFetchBranch() throws Exception {

    setupBranchMemoryStore();

    final Branch branch = tsdb.fetchBranch(
            Branch.stringToId("00010001BECD000181A8"),
            true).joinUninterruptibly();
    assertNotNull(branch);
    assertEquals(1, branch.getTreeId());
    assertEquals("cpu", branch.getDisplayName());
    assertEquals("00010001BECD000181A8", branch.getBranchId());
    assertEquals(1, branch.getBranches().size());
    assertEquals(2, branch.getLeaves().size());

    setupBranchHBaseStore();
    Scanner scanner = PowerMockito.mock(Scanner.class);
    when(client.newScanner(anyBytes())).thenReturn(scanner);

    ArrayList<ArrayList<KeyValue>> valid_return = get_valid_return();

    when(scanner.nextRows()).thenReturn(
            Deferred.fromResult(valid_return))
            .thenReturn(
            Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));


    final Branch hBaseBranch = tsdb.fetchBranch(
            Branch.stringToId("00010001BECD000181A8"),
            true).joinUninterruptibly();
    assertNotNull(hBaseBranch);
    assertEquals(1, hBaseBranch.getTreeId());
    assertEquals("cpu", hBaseBranch.getDisplayName());
    assertEquals("00010001BECD000181A8", hBaseBranch.getBranchId());
    assertEquals(1, hBaseBranch.getBranches().size());
    assertEquals(2, hBaseBranch.getLeaves().size());

    assertEquals(hBaseBranch, branch);
  }


  @Test
  public void fetchBranchNotFound() throws Exception {
    setupBranchMemoryStore();
    fail();
    final Branch branch = tsdb.fetchBranch(
            Branch.stringToId("00010001BECD000181A0"),
            false).joinUninterruptibly();
    assertNull(branch);
  }

  @Test
  public void fetchBranchOnly() throws Exception {
    setupBranchMemoryStore();
    fail();
    final Branch branch = tsdb.fetchBranchOnly(
            Branch.stringToId("00010001BECD000181A8")).joinUninterruptibly();
    assertNotNull(branch);
    assertEquals("cpu", branch.getDisplayName());
    assertNull(branch.getLeaves());
    assertNull(branch.getBranches());
  }

  @Test
  public void fetchBranchOnlyNotFound() throws Exception {
    setupBranchMemoryStore();
    fail();
    final Branch branch = tsdb.fetchBranchOnly(
            Branch.stringToId("00010001BECD000181A0")).joinUninterruptibly();
    assertNull(branch);
  }
  @Test
  public void fetchBranchNSU() throws Exception {
    setupBranchMemoryStore();
    fail();
    /*
    *This test was supposed to test the branch structure if it was not linked by
    * UID anymore. Thus a leaf should not be connected to the branch because
    * of lacking UID match. In the Memory store we keep these things separate.
    *
    * As this basically was mocked before I would argue this test was more or
    * less useless here. All the fetch methods should be moved into
    * TestHBaseStore.
    */

    /*tsdb_store.allocateUID("sys.cpu.0", new byte[]{0, 0, 1}, METRIC);
    tsdb_store.allocateUID("host", new byte[]{0, 0, 1}, TAGK);
    tsdb_store.allocateUID("web01", new byte[]{0, 0, 1}, TAGV);

    final Branch branch = tsdb.fetchBranch(
    Branch.stringToId("00010001BECD000181A8"),
    true).joinUninterruptibly();
    assertNotNull(branch);
    assertEquals(1, branch.getTreeId());
    assertEquals("cpu", branch.getDisplayName());
    assertEquals("00010001BECD000181A8", branch.getBranchId());
    assertEquals(1, branch.getBranches().size());
    assertEquals(1, branch.getLeaves().size());*/
  }

  @Test
  public void testStoreBranch() throws Exception {
    setupBranchMemoryStore();
    final Branch branch = TestBranch.buildTestBranch(tree);
    tsdb.storeBranch(tree, branch, true);
    //assertEquals(3, tsdb_store.numRows());
    //assertEquals(3, tsdb_store.numColumns(new byte[]{0, 1}));
    final Branch parsed =
            tsdb_store.fetchBranch(branch.compileBranchId(), true, tsdb)
            .joinUninterruptibly();
    parsed.setTreeId(1);
    assertEquals("ROOT", parsed.getDisplayName());

    setupBranchHBaseStore();
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeBranchMissingTreeID() throws Exception {
    setupBranchMemoryStore();
    fail();
    final Branch branch = new Branch();
    tsdb.storeBranch(tree, branch, false);
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeBranchTreeID0() throws Exception {
    setupBranchMemoryStore();
    fail();
    final Branch branch = TestBranch.buildTestBranch(tree);
    branch.setTreeId(0);
    tsdb.storeBranch(tree, branch, false);
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeBranchTreeID65536() throws Exception {
    setupBranchMemoryStore();
    fail();
    final Branch branch = TestBranch.buildTestBranch(tree);
    branch.setTreeId(65536);
    tsdb.storeBranch(tree ,branch, false);
  }

  @Test
  public void storeBranchExistingLeaf() throws Exception {
    setupBranchMemoryStore();
    final Branch branch = TestBranch.buildTestBranch(tree);
    Leaf leaf = new Leaf("Alarms", "ABCD");
    byte[] qualifier = leaf.columnQualifier();
    //tsdb_store.addColumn(branch.compileBranchId(), Tree.TREE_FAMILY(),
    //        qualifier, leaf.toStorageJson());

    tsdb.storeBranch(tree, branch, true);
    fail();//TODO
    //assertEquals(3, tsdb_store.numRows());
    //assertEquals(3, tsdb_store.numColumns(new byte[]{0, 1}));
    assertNull(tree.getCollisions());
    //final Branch parsed = JSON.parseToObject(tsdb_store.getColumn(
    //                new byte[]{0, 1}, "branch".getBytes(Const.CHARSET_ASCII)),
    //        Branch.class);
    //parsed.setTreeId(1);
    //assertEquals("ROOT", parsed.getDisplayName());
  }

  @Test
  public void storeBranchCollision() throws Exception {
    setupBranchMemoryStore();
    final Branch branch = TestBranch.buildTestBranch(tree);
    Leaf leaf = new Leaf("Alarms", "0101");
    byte[] qualifier = leaf.columnQualifier();
    //tsdb_store.addColumn(branch.compileBranchId(), Tree.TREE_FAMILY(),
    //        qualifier, leaf.toStorageJson());

    tsdb.storeBranch(tree, branch, true);
    fail();//TODO
    //assertEquals(3, tsdb_store.numRows());
    //assertEquals(3, tsdb_store.numColumns(new byte[]{0, 1}));
    assertEquals(1, tree.getCollisions().size());
    //final Branch parsed = JSON.parseToObject(tsdb_store.getColumn(
    //                new byte[] { 0, 1 }, "branch".getBytes(Const.CHARSET_ASCII)),
    //        Branch.class);
    //parsed.setTreeId(1);
    //assertEquals("ROOT", parsed.getDisplayName());
  }

  private byte[] emptyArray() {
    return eq(HBaseClient.EMPTY_ARRAY);
  }
  private PutRequest anyPut() {
    return any(PutRequest.class);
  }
  private byte[] anyBytes() { return any(byte[].class); }
}
