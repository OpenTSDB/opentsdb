package net.opentsdb.storage;

import dagger.ObjectGraph;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.core.TSDB;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.TestBranch;
import org.junit.Test;


/**
 * Use this to make sure the MemoryStore returns the same as the other
 * TsdbStores.
 */
public class TestMemoryStore extends TestTsdbStore {
  @Override
  @Test
  public void testFetchBranch() throws Exception {
    setupBranchMemoryStore(STORE_DATA);
    super.testFetchBranch();
  }

  @Override
  @Test
  public void testFetchBranchNotFound() throws Exception {
    setupBranchMemoryStore(STORE_DATA);
    super.testFetchBranchNotFound();
  }

  @Override
  @Test
  public void testFetchBranchOnly() throws Exception {
    setupBranchMemoryStore(STORE_DATA);
    super.testFetchBranchOnly();
  }

  @Override
  @Test
  public void testFetchBranchOnlyNotFound() throws Exception {
    setupBranchMemoryStore(STORE_DATA);
    super.testFetchBranchOnlyNotFound();
  }
  @Override
  @Test
  public void testStoreBranch() throws Exception {
    setupBranchMemoryStore(!STORE_DATA);
    super.testStoreBranch();
  }
  @Override
  @Test
  public void testStoreBranchExistingLeaf() throws Exception {
    setupBranchMemoryStore(!STORE_DATA);//test for memory store

    final Branch branch = TestBranch.buildTestBranch(tree);
    tsdb.getTreeClient().storeBranch(tree, branch, true);
    super.testStoreBranchExistingLeaf();
  }
  @Override
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
  @Override
  @Test
  public void testFetchBranchLoadingMetricsUID() { }

  @Override
  @Test
  public void testParseFromStorage() throws Exception { }

  @Override
  @Test
  public void testParseFromStorageNSUMetric() throws Throwable { }

  @Override
  @Test
  public void testParseFromStorageNSUTagk() throws Throwable { }

  @Override
  @Test
  public void testParseFromStorageNSUTagV() throws Throwable { }

    /**
     *  Classes for testing the storage calls for branch tests
     */
  private void setupBranchMemoryStore(final boolean store) {
    ObjectGraph objectGraph = ObjectGraph.create(new TestModuleMemoryStore());
    tsdb_store = objectGraph.get(TsdbStore.class);
    tsdb = objectGraph.get(TSDB.class);

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
