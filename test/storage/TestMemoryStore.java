package net.opentsdb.storage;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TsdbBuilder;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.TestBranch;
import net.opentsdb.utils.Config;
import org.junit.Test;


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
  @Test
  public void testStoreBranchCollision() throws Exception {
    setupBranchMemoryStore(!STORE_DATA);//test for memory store
    super.testStoreBranchCollision();
  }

  /**
   * The tests testFetchBranchLoadingMetricsUID and testParseFromStorage are
   * not tested in the MemoryStore. testFetchBranchLoadingMetricsUID is
   * unnecessary to test since it stores the whole leaf.
   * testParseFromStorage - These errors or correct answers are there to test
   * the parsing from the KeyValue from the database. No such parsing is made
   * in the MemoryStore since the raw objects is stored.
   */
  @Test
  public void testFetchBranchLoadingMetricsUID() { }

  @Test
  public void testParseFromStorage() throws Exception { }

  @Test
  public void testParseFromStorageNSUMetric() throws Throwable { }

  @Test
  public void testParseFromStorageNSUTagk() throws Throwable { }

  @Test
  public void testParseFromStorageNSUTagV() throws Throwable { }

    /**
     *  Classes for testing the storage calls for branch tests
     */
  private void setupBranchMemoryStore(final boolean store) throws Exception {

    config = new Config(false);
    tsdb_store = new MemoryStore();
    tsdb = TsdbBuilder.createFromConfig(config)
            .withStore(tsdb_store)
            .build();

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
