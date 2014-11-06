package net.opentsdb.storage;

import net.opentsdb.core.TSDB;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.TestBranch;
import net.opentsdb.utils.Config;
import org.junit.Test;

import java.util.TreeMap;

/**
 * Use this to make sure the MemoryStore returns the same as the other
 * TsdbStores.
 */
public class TestMemoryStore extends TestTsdbStore {
  @Test
  public void testFetchBranch() throws Exception {
    setupBranchMemoryStore(STORE_DATA);
    super.testFetchBranch();
  }

  @Test
  public void testFetchBranchNotFound() throws Exception {
    setupBranchMemoryStore(STORE_DATA);
    super.testFetchBranchNotFound();
  }

  @Test
  public void testFetchBranchOnly() throws Exception {
    setupBranchMemoryStore(STORE_DATA);
    super.testFetchBranchOnly();
  }

  @Test
  public void testFetchBranchOnlyNotFound() throws Exception {
    setupBranchMemoryStore(STORE_DATA);
    super.testFetchBranchOnlyNotFound();
  }
  @Test
  public void testStoreBranch() throws Exception {
    setupBranchMemoryStore(!STORE_DATA);
    super.testStoreBranch();
  }
  @Test
  public void testStoreBranchExistingLeaf() throws Exception {
    setupBranchMemoryStore(!STORE_DATA);//test for memory store

    final Branch branch = TestBranch.buildTestBranch(tree);
    tsdb.storeBranch(tree, branch, true);
    super.testStoreBranchExistingLeaf();
  }

    /**
     *  Classes for testing the storage calls for branch tests
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
}
