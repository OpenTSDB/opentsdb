package net.opentsdb.storage;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.TestBranch;
import net.opentsdb.tree.TestTree;
import net.opentsdb.tree.Tree;
import net.opentsdb.uid.UniqueIdType;

import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.TreeMap;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@Ignore
public abstract class TestTsdbStore {
  private TSDB tsdb;
  protected TsdbStore tsdb_store;
  protected UIDMeta meta;

  private Tree tree = TestTree.buildTestTree();

  @Test
  public void testGetMetaNullCell() throws IOException {
    tsdb_store.getMeta(new byte[]{0, 0, 1}, "derp", UniqueIdType.TAGK);
  }


  /*BRANCH DATABASE STUFF*/

  /**
   * Mocks classes for testing the storage calls
   */
  private void setupMemoryStore() throws Exception {
    final Config config = new Config(false);

    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, config);

    Branch branch = new Branch(1);
    TreeMap<Integer, String> path = new TreeMap<Integer, String>();
    path.put(0, "ROOT");
    path.put(1, "sys");
    path.put(2, "cpu");
    branch.prependParentPath(path);
    branch.setDisplayName("cpu");

    tsdb_store.storeBranch(tree, branch, false);
    Leaf leaf = new Leaf("user", "000001000001000001");
    tsdb_store.storeLeaf(leaf, branch, tree);
    leaf = new Leaf("nice", "000002000002000002");
    tsdb_store.storeLeaf(leaf, branch, tree);

    branch = new Branch(1);
    path.put(3, "mboard");
    branch.prependParentPath(path);
    branch.setDisplayName("mboard");
    tsdb_store.storeBranch(tree, branch, false);
    leaf = new Leaf("Asus", "000003000003000003");
    tsdb_store.storeLeaf(leaf, branch, tree);
  }

  @Test
  public void fetchBranch() throws Exception {
    setupMemoryStore();
    fail();

    final Branch branch = tsdb.fetchBranch(
            Branch.stringToId("00010001BECD000181A8"),
            true).joinUninterruptibly();
    assertNotNull(branch);
    assertEquals(1, branch.getTreeId());
    assertEquals("cpu", branch.getDisplayName());
    assertEquals("00010001BECD000181A8", branch.getBranchId());
    assertEquals(1, branch.getBranches().size());
    assertEquals(2, branch.getLeaves().size());
  }

  @Test
  public void fetchBranchNotFound() throws Exception {
    setupMemoryStore();
    fail();
    final Branch branch = tsdb.fetchBranch(
            Branch.stringToId("00010001BECD000181A0"),
            false).joinUninterruptibly();
    assertNull(branch);
  }

  @Test
  public void fetchBranchOnly() throws Exception {
    setupMemoryStore();
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
    setupMemoryStore();
    fail();
    final Branch branch = tsdb.fetchBranchOnly(
            Branch.stringToId("00010001BECD000181A0")).joinUninterruptibly();
    assertNull(branch);
  }
  @Test
  public void fetchBranchNSU() throws Exception {
    setupMemoryStore();
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
  public void storeBranch() throws Exception {
    setupMemoryStore();
    final Branch branch = TestBranch.buildTestBranch(tree);
    tsdb.storeBranch(tree, branch, true);
    fail();//TODO need to fetch from the new structure.
    //assertEquals(3, tsdb_store.numRows());
    //assertEquals(3, tsdb_store.numColumns(new byte[]{0, 1}));
    //final Branch parsed = JSON.parseToObject(tsdb_store.getColumn(
    //    new byte[] { 0, 1 }, "branch".getBytes(Const.CHARSET_ASCII)),
    //    Branch.class);
    //parsed.setTreeId(1);
    //assertEquals("ROOT", parsed.getDisplayName());
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeBranchMissingTreeID() throws Exception {
    setupMemoryStore();
    fail();
    final Branch branch = new Branch();
    tsdb.storeBranch(tree, branch, false);
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeBranchTreeID0() throws Exception {
    setupMemoryStore();
    fail();
    final Branch branch = TestBranch.buildTestBranch(tree);
    branch.setTreeId(0);
    tsdb.storeBranch(tree, branch, false);
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeBranchTreeID65536() throws Exception {
    setupMemoryStore();
    fail();
    final Branch branch = TestBranch.buildTestBranch(tree);
    branch.setTreeId(65536);
    tsdb.storeBranch(tree ,branch, false);
  }

  @Test
  public void storeBranchExistingLeaf() throws Exception {
    setupMemoryStore();
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
    setupMemoryStore();
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
}
