package net.opentsdb.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stumbleupon.async.DeferredGroupException;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.storage.json.StorageModule;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.TestBranch;
import net.opentsdb.tree.TestTree;
import net.opentsdb.tree.Tree;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueIdType;

import net.opentsdb.utils.Config;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;


import static net.opentsdb.core.StringCoder.toBytes;
import static net.opentsdb.uid.UniqueIdType.METRIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public abstract class TestTsdbStore {
  protected static final boolean SAME_TSUID = true;
  protected static final boolean NOT_SAME_TSUID = false;
  protected TSDB tsdb;
  protected TsdbStore tsdb_store;
  protected ObjectMapper jsonMapper;
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

  @Before
  public void setUpTsdbStore() throws Exception {
    jsonMapper = new ObjectMapper();
    jsonMapper.registerModule(new StorageModule());
  }

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
   * Use this method to get a Deferred with valid answers for the branch query.
   *
   * @return A valid return that the HBase database would return for the objects
   * specified by the @see {@link TestTsdbStore#setUpBranchesAndLeafs()}
   */
  protected ArrayList<ArrayList<KeyValue>> getValidReturn() throws JsonProcessingException {

    ArrayList<ArrayList<KeyValue>> valid_return =
            new ArrayList<ArrayList<KeyValue>>();

    ArrayList<KeyValue> ans = new ArrayList<KeyValue>();
    //branches
    KeyValue kv = new KeyValue(
            Branch.stringToId("00010001BECD000181A8"), new byte[0],
            toBytes("branch"), jsonMapper.writeValueAsBytes(root_branch));
    ans.add(kv);
    kv = new KeyValue(
            Branch.stringToId("00010001BECD000181A8BF992A99"), new byte[0],
            toBytes("branch"), jsonMapper.writeValueAsBytes(root_branch));
    ans.add(kv);
    //leaves
    kv = new KeyValue( Branch.stringToId("00010001BECD000181A8"), new byte[0],
            Leaf.LEAF_PREFIX(), jsonMapper.writeValueAsBytes(root_leaf_one));
    ans.add(kv);
    kv = new KeyValue( Branch.stringToId("00010001BECD000181A8"), new byte[0],
            Leaf.LEAF_PREFIX(), jsonMapper.writeValueAsBytes(root_leaf_two));
    ans.add(kv);
    kv = new KeyValue(
            Branch.stringToId("00010001BECD000181A8BF992A99"), new byte[0],
            Leaf.LEAF_PREFIX(), jsonMapper.writeValueAsBytes(child_leaf_one));
    ans.add(kv);

    valid_return.add(ans);
    return valid_return;
  }

  @Test
  public void testFetchBranchLoadingMetricsUID() throws Exception {
  /*
   * This test should test fetching a branch and loading the uid metrics on the
   * leafs.
   */
  final Branch branch = tsdb_store.fetchBranch(
    Branch.stringToId("00010001BECD000181A8"), true, tsdb)
          .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(branch);
    assertEquals(1, branch.getTreeId());
    assertEquals("cpu", branch.getDisplayName());
    assertEquals("00010001BECD000181A8", branch.getBranchId());
    assertEquals(1, branch.getBranches().size());
    assertEquals(2, branch.getLeaves().size());
  }

  @Test
  public void testFetchBranch() throws Exception {
    final Branch branch = tsdb_store.fetchBranch(
            Branch.stringToId("00010001BECD000181A8"),
            false, tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
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
            false, tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNull(branch);
  }

  @Test
  public void testFetchBranchOnly() throws Exception {
    final Branch branch = tsdb_store.fetchBranchOnly(
            Branch.stringToId("00010001BECD000181A8")).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(branch);
    assertEquals("cpu", branch.getDisplayName());
    assertNull(branch.getLeaves());
    assertNull(branch.getBranches());
  }

  @Test
  public void testFetchBranchOnlyNotFound() throws Exception {
    final Branch branch = tsdb_store.fetchBranchOnly(
            Branch.stringToId("00010001BECD000181A0")).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNull(branch);
  }

  @Test
  public void testStoreBranch() throws Exception {
    final Branch branch = TestBranch.buildTestBranch(tree);
    tsdb_store.storeBranch(tree, branch, true);
    final Branch parsed =
            tsdb_store.fetchBranch(branch.compileBranchId(), true, tsdb)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals("ROOT", parsed.getDisplayName());
    assertEquals(1, parsed.getTreeId());
    assertNotNull(parsed.getPath());
  }

  @Test
  public void testStoreBranchExistingLeaf() throws Exception {

    final Branch root = getLeafCollision(SAME_TSUID);

    ArrayList<Boolean> results =
            tsdb_store.storeBranch(tree, root, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertEquals(2, results.size());
    assertFalse(results.get(0));
    assertTrue(results.get(1));
    assertNull(tree.getCollisions());

    final Branch parsed = tsdb_store
            .fetchBranchOnly(
            TestBranch.buildTestBranch(tree)
            .compileBranchId()).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    parsed.setTreeId(1);
    assertEquals("ROOT", parsed.getDisplayName());
  }

  @Test
  public void testStoreBranchCollision() throws Exception {

    /* Collision object */
    final Branch root = getLeafCollision(NOT_SAME_TSUID);

    /* Setup data */
    final Branch branch = TestBranch.buildTestBranch(tree);
    tsdb_store.storeBranch(tree, branch, true);
    /* Get results*/
    final ArrayList<Boolean> results =
            tsdb_store.storeBranch(tree, root, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    /* Check results*/
    assertEquals(2, results.size());
    assertFalse(results.get(0));
    assertFalse(results.get(1));
    assertEquals(1, tree.getCollisions().size());
  }

  @Test
  public void testParseFromStorage() throws Exception {
    final Branch branch = tsdb_store.fetchBranch(
            Branch.stringToId("00010001BECD000181A8"), true, tsdb)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    Leaf leaf = null;
    for (Leaf temp : branch.getLeaves()) {
      if (temp.getTsuid().equals("000001000001000001")) {
        leaf = temp;
        break;
      }
    }

    assertNotNull(leaf);
    assertEquals("user", leaf.getDisplayName());
    assertEquals("000001000001000001", leaf.getTsuid());
    assertEquals("sys.cpu.0", leaf.getMetric());
    assertEquals(1, leaf.getTags().size());
    assertEquals("web01", leaf.getTags().get("host"));
  }

  /* TREE */
  @Test
  public void storeTree() throws Exception {
    fail();
    /*setupStorage();
    final Tree tree = buildTestTree();
    tree.setName("New Name");
    Assert.assertNotNull(tsdb_store.storeTree(tree, false)
            .joinUninterruptibly());*/
  }


  @Test
  public void flushCollisions() throws Exception {
    fail();
    /*setupStorage();
    final Tree tree = buildTestTree();
    tree.setStoreFailures(true);
    tree.addCollision("010203", "AABBCCDD");
    Assert.assertNotNull(tsdb.flushTreeCollisions(tree)
            .joinUninterruptibly());
    assertEquals(4, tsdb_store.numRows());
    assertEquals(3, tsdb_store.numColumns(new byte[]{0, 1, 1}));*/
  }


  @Test
  public void flushNotMatched() throws Exception {
    fail();
    /*setupStorage();
    final Tree tree = buildTestTree();
    tree.setStoreFailures(true);
    tree.addNotMatched("010203", "Failed rule 2:2");
    Assert.assertNotNull(tsdb.flushTreeNotMatched(tree)
            .joinUninterruptibly());
    assertEquals(4, tsdb_store.numRows());
    assertEquals(3, tsdb_store.numColumns(new byte[]{0, 1, 2}));*/
  }
  @Test
  public void flushNotMatchedDisabled() throws Exception {
    fail();
    /*setupStorage();
    final Tree tree = buildTestTree();
    tree.addNotMatched("010203", "Failed rule 2:2");
    Assert.assertTrue(tsdb.flushTreeNotMatched(tree)
            .joinUninterruptibly());

    ArrayList<String> list = new ArrayList<String>(tree.getNotMatched().size());
    list.addAll(tree.getNotMatched().keySet());
    Map<String, String> fetched = tsdb.fetchNotMatched(tree.getTreeId(), list)
            .joinUninterruptibly();*/


  }

  @Test
  public void flushNotMatchedWNotMatchedExisting() throws Exception {
    fail();
    /*setupStorage();
    final Tree tree = buildTestTree();
    tree.addNotMatched("010101", "Failed rule 4:4");
    Assert.assertNotNull(tsdb.flushTreeNotMatched(tree)
            .joinUninterruptibly());
    assertEquals(4, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(new byte[]{0, 1, 2}));*/
  }

  @Test
  public void createNewTree() throws Exception {
    fail();
    /*setupStorage();
    final Tree tree = new Tree();
    tree.setName("New Tree");
    final int tree_id = tsdb.createNewTree(tree)
            .joinUninterruptibly();
    assertEquals(3, tree_id);
    assertEquals(5, tsdb_store.numRows());
    assertEquals(1, tsdb_store.numColumns(new byte[]{0, 3}));*/
  }

  @Test
  public void createNewFirstTree() throws Exception {
    fail();
    /*setupStorage();
    tsdb_store.flushStorage();
    final Tree tree = new Tree();
    tree.setName("New Tree");
    final int tree_id = tsdb.createNewTree(tree)
            .joinUninterruptibly();
    assertEquals(1, tree_id);
    assertEquals(1, tsdb_store.numRows());
    assertEquals(1, tsdb_store.numColumns(new byte[]{0, 1}));*/
  }

  @Test (expected = IllegalArgumentException.class)
  public void createNewTreeNoChanges() throws Exception {
    fail();
    /*
    setupStorage();
    final Tree tree = new Tree();
    tsdb.createNewTree(tree).joinUninterruptibly();
    */
  }

  @Test (expected = IllegalArgumentException.class)
  public void createNewTreeOutOfIDs() throws Exception {
    fail();/*
    setupStorage();

    final Tree max_tree = new Tree(65535);
    max_tree.setName("max");
    tsdb_store.addColumn(new byte[]{(byte) 0xFF, (byte) 0xFF},
            "tree".getBytes(Const.CHARSET_ASCII), JSON.serializeToBytes(max_tree));

    final Tree tree = new Tree();
    tsdb.createNewTree(tree).joinUninterruptibly();*/
  }
  @Test
  public void fetchTree() throws Exception {
    fail();/*
    setupStorage();
    final Tree tree = tsdb.fetchTree(1)
            .joinUninterruptibly();
    Assert.assertNotNull(tree);
    assertEquals("Test Tree", tree.getName());
    assertEquals(2, tree.getRules().size());
    Assert.assertTrue(tree.getEnabled());*/
  }

  @Test
  public void fetchTreeDoesNotExist() throws Exception {
    fail();/*
    setupStorage();
    Assert.assertNull(tsdb.fetchTree(3).joinUninterruptibly());*/
  }

  @Test
  public void fetchAllTrees() throws Exception {
    fail(); /*
    setupStorage();
    final List<Tree> trees = tsdb_store.fetchAllTrees()
            .joinUninterruptibly();
    Assert.assertNotNull(trees);
    assertEquals(2, trees.size());*/
  }

  @Test
  public void fetchAllTreesNone() throws Exception {
    fail();/*
    setupStorage();
    tsdb_store.flushStorage();
    final List<Tree> trees = tsdb_store.fetchAllTrees()
            .joinUninterruptibly();
    Assert.assertNotNull(trees);
    assertEquals(0, trees.size());*/
  }

  @Test
  public void fetchAllCollisions() throws Exception {
    fail();/*
    setupStorage();
    Map<String, String> collisions =
            tsdb.fetchCollisions(1, null).joinUninterruptibly();
    Assert.assertNotNull(collisions);
    assertEquals(2, collisions.size());
    Assert.assertTrue(collisions.containsKey("010101"));
    Assert.assertTrue(collisions.containsKey("020202"));*/
  }

  @Test
  public void fetchAllCollisionsNone() throws Exception {
    fail();/*
    setupStorage();
    tsdb_store.flushRow(new byte[]{0, 1, 1});
    Map<String, String> collisions =
            tsdb.fetchCollisions(1, null).joinUninterruptibly();
    Assert.assertNotNull(collisions);
    assertEquals(0, collisions.size());*/
  }

  @Test
  public void fetchCollisionsSingle() throws Exception {
    fail();/*
    setupStorage();
    final ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("020202");
    Map<String, String> collisions =
            tsdb.fetchCollisions(1, tsuids).joinUninterruptibly();
    Assert.assertNotNull(collisions);
    assertEquals(1, collisions.size());
    Assert.assertTrue(collisions.containsKey("020202"));*/
  }

  @Test
  public void fetchCollisionsSingleNotFound() throws Exception {
    fail();/*
    setupStorage();
    final ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("030303");
    Map<String, String> collisions =
            tsdb.fetchCollisions(1, tsuids).joinUninterruptibly();
    Assert.assertNotNull(collisions);
    assertEquals(0, collisions.size());*/
  }

  @Test (expected = IllegalArgumentException.class)
  public void fetchCollisionsID0() throws Exception {
    fail();/*
    setupStorage();
    tsdb.fetchCollisions(0, null);*/
  }

  @Test (expected = IllegalArgumentException.class)
  public void fetchCollisionsID655536() throws Exception {
    fail();/*
    setupStorage();
    tsdb.fetchCollisions(655536, null);*/
  }

  @Test
  public void fetchAllNotMatched() throws Exception {
    fail();/*
    setupStorage();
    Map<String, String> not_matched =
            tsdb.fetchNotMatched(1, null).joinUninterruptibly();
    Assert.assertNotNull(not_matched);
    assertEquals(2, not_matched.size());
    Assert.assertTrue(not_matched.containsKey("010101"));
    assertEquals("Failed rule 0:0", not_matched.get("010101"));
    Assert.assertTrue(not_matched.containsKey("020202"));
    assertEquals("Failed rule 1:1", not_matched.get("020202"));*/
  }

  @Test
  public void fetchAllNotMatchedNone() throws Exception {
    fail();/*
    setupStorage();
    tsdb_store.flushRow(new byte[]{0, 1, 2});
    Map<String, String> not_matched =
            tsdb.fetchNotMatched(1, null).joinUninterruptibly();
    Assert.assertNotNull(not_matched);
    assertEquals(0, not_matched.size());*/
  }

  @Test
  public void fetchNotMatchedSingle() throws Exception {
    fail();/*
    setupStorage();
    final ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("020202");
    Map<String, String> not_matched =
            tsdb.fetchNotMatched(1, tsuids).joinUninterruptibly();
    Assert.assertNotNull(not_matched);
    assertEquals(1, not_matched.size());
    Assert.assertTrue(not_matched.containsKey("020202"));
    assertEquals("Failed rule 1:1", not_matched.get("020202"));*/
  }

  @Test
  public void fetchNotMatchedSingleNotFound() throws Exception {
    fail();/*
    setupStorage();
    final ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("030303");
    Map<String, String> not_matched =
            tsdb.fetchNotMatched(1, tsuids).joinUninterruptibly();
    Assert.assertNotNull(not_matched);
    assertEquals(0, not_matched.size());*/
  }

  @Test (expected = IllegalArgumentException.class)
  public void fetchNotMatchedID0() throws Exception {
    fail();/*
    setupStorage();
    tsdb.fetchNotMatched(0, null);*/
  }

  @Test (expected = IllegalArgumentException.class)
  public void fetchNotMatchedID655536() throws Exception {
    fail();/*
    setupStorage();
    tsdb.fetchNotMatched(655536, null);*/
  }

  @Test
  public void deleteTree() throws Exception {
    fail();/*
    setupStorage();
    Assert.assertNotNull(tsdb.deleteTree(1, true)
            .joinUninterruptibly());
    assertEquals(0, tsdb_store.numRows());*/
  }



  @Test (expected = NoSuchUniqueId.class)
  public void testParseFromStorageNSUMetric() throws Throwable {
    try {
      tsdb_store.fetchBranch(
              Branch.stringToId("00010001BECD000181A8"), true, tsdb)
              .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }

  @Test (expected = NoSuchUniqueId.class)
  public void testParseFromStorageNSUTagk() throws Throwable {
    try {
      tsdb_store.fetchBranch(
              Branch.stringToId("00010001BECD000181A8"), true, tsdb)
              .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }

  @Test (expected = NoSuchUniqueId.class)
  public void testParseFromStorageNSUTagV() throws Throwable{
    try {
      tsdb_store.fetchBranch(
              Branch.stringToId("00010001BECD000181A8"), true, tsdb)
              .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }
  public void TestFetchBranchNSU() throws Exception {

    final Branch branch = tsdb.getTreeClient().fetchBranch(
            Branch.stringToId("00010001BECD000181A8"),
            false, tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertNotNull(branch);
    assertEquals(1, branch.getTreeId());
    assertEquals("cpu", branch.getDisplayName());
    assertEquals("00010001BECD000181A8", branch.getBranchId());
    assertEquals(1, branch.getBranches().size());
    assertEquals(1, branch.getLeaves().size());
  }

  /*META TESTS*/

  @Test
  public void testGetMetaNullCell() throws IOException {
    tsdb_store.getMeta(new byte[]{0, 0, 1}, "derp", UniqueIdType.TAGK);
  }

  /* COUNTER TESTS */

  @Test
  public void deleteTimeseriesCounter() {
    //tsdb_store.deleteTimeseriesCounter(TSMeta ts);
    fail();
  }

  @Test
  public void setTSMetaCounter() {
    //tsdb_store.setTSMetaCounter(byte[] tsuid, long number);
    fail();
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

  protected byte[] anyBytes() {
    return any(byte[].class);
  }

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

  @Test
  public void storeNew() throws Exception {
    meta = new UIDMeta(METRIC, new byte[] { 0, 0, 1 }, "sys.cpu.1");
    meta.setDisplayName("System CPU");
    tsdb_store.add(meta).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    meta = tsdb_store.getMeta(new byte[] { 0, 0, 1 },meta.getName() ,METRIC)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    assertEquals("System CPU", meta.getDisplayName());
  }
}
