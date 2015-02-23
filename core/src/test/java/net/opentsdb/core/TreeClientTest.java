package net.opentsdb.core;

import dagger.ObjectGraph;
import net.opentsdb.TestModule;
import net.opentsdb.storage.MockBase;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;

import static org.junit.Assert.*;

public class TreeClientTest {
  @Inject TreeClient treeClient;

  @Before
  public void before() throws Exception {
    ObjectGraph.create(new TestModule()).inject(this);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStoreTreeTooLowID() {
    Tree tree = new Tree();
    tree.setTreeId(Const.MIN_TREE_ID_INCLUSIVE - 1);

    treeClient.storeTree(tree, true);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testStoreTreeTooHighID() {
    Tree tree = new Tree();
    tree.setTreeId(Const.MAX_TREE_ID_INCLUSIVE + 1);

    treeClient.storeTree(tree, true);
  }

  @Test (expected = IllegalStateException.class)
  public void testStoreTreeNotChanged() {
    Tree tree = new Tree();
    tree.setTreeId(Const.MAX_TREE_ID_INCLUSIVE);

    treeClient.storeTree(tree, true);
  }
  @Test
  public void testStoreTreeValidID() throws Exception {
    Tree tree = new Tree();
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {

      tree.setTreeId(id);
      // sets the optional Note field do tha this
      // tree will have the status changed
      tree.setNotes("Note");

      assertTrue(treeClient.storeTree(tree, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    }
  }

  @Test
  public void testFetchTree() throws Exception {
    Tree tree = new Tree();
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {

      tree.setTreeId(id);
      // sets the optional Note field do tha this
      // tree will have the status changed
      tree.setNotes("Note");

      treeClient.storeTree(tree, true);//maybe alternate true and false?
      assertEquals(tree, treeClient.fetchTree(id).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    }
  }


  @Test (expected = IllegalArgumentException.class)
  public void testFetchTreeTooLowID() {
    treeClient.fetchTree(Const.MIN_TREE_ID_INCLUSIVE - 1);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFetchTreeTooHighID() {
    treeClient.fetchTree(Const.MAX_TREE_ID_INCLUSIVE + 1);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testCreateNewTreeIDAlreadySet() {
    Tree tree = new Tree();
    tree.setTreeId(1);

    treeClient.createNewTree(tree);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testCreateNewTreeNoName() {
    Tree tree = new Tree();

    treeClient.createNewTree(tree);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testCreateNewTreeEmptyName() {
    Tree tree = new Tree();
    tree.setName("");

    treeClient.createNewTree(tree);
  }

  @Test
  public void testCreateNewTree() throws Exception {
    Tree tree = new Tree();
    tree.setName("Valid1");

    assertEquals( new Integer(1), treeClient.createNewTree(tree).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    Tree tree2 = new Tree();
    tree2.setName("Valid2");
    assertEquals( new Integer(2), treeClient.createNewTree(tree2).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test (expected = IllegalArgumentException.class)
  public void testDeleteTreeTooLowID() {

    treeClient.deleteTree(Const.MIN_TREE_ID_INCLUSIVE - 1, true);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testDeleteTreeTooHighID() {
    treeClient.deleteTree(Const.MAX_TREE_ID_INCLUSIVE + 1, true);
  }

  @Test
  public void testDeleteTree() throws Exception {
    testStoreTreeValidID();
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {

      assertTrue(treeClient.deleteTree(id, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    }
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFetchCollisionsTooLowID() {

    treeClient.fetchCollisions(Const.MIN_TREE_ID_INCLUSIVE - 1, null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFetchCollisionsTooHighID() {
    treeClient.fetchCollisions(Const.MAX_TREE_ID_INCLUSIVE + 1, null);
  }

  @Test
  public void testFetchCollisions() throws Exception {
    testStoreTreeValidID();
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {

      treeClient.fetchCollisions(id, null).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFetchNotMatchedTooLowID() {
    treeClient.fetchCollisions(Const.MIN_TREE_ID_INCLUSIVE - 1, null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFetchNotMatchedTooHighID() {
    treeClient.fetchCollisions(Const.MAX_TREE_ID_INCLUSIVE + 1, null);
  }

  @Test
  public void testFetchNotMatched() throws Exception {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {

      treeClient.fetchNotMatched(id, null).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }
  }

  @Test
  public void testFlushTreeCollisionsNoStoredFailures() throws Exception {
    final Tree tree = new Tree();
    tree.setStoreFailures(false);
    tree.addCollision("4711", "JustANumber");
    assertTrue(tree.getCollisions().containsKey("4711"));
    assertTrue(tree.getCollisions().containsValue("JustANumber"));
    assertEquals(1, tree.getCollisions().size());

    assertTrue(treeClient.flushTreeCollisions(tree).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    assertEquals(0, tree.getCollisions().size());
  }

  @Test
  public void testFlushTreeCollisionsFailures() throws Exception {
    final Tree tree = new Tree();
    tree.setStoreFailures(true);
    tree.addCollision("4711", "JustANumber");
    assertTrue(tree.getCollisions().containsKey("4711"));
    assertTrue(tree.getCollisions().containsValue("JustANumber"));
    assertEquals(1, tree.getCollisions().size());

    assertTrue(treeClient.flushTreeCollisions(tree).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    assertEquals(1, tree.getCollisions().size());
  }

  @Test
  public void testFlushCollisionsDisabled() throws Exception {
    final Tree tree = new Tree();
    tree.setStoreFailures(false);
    tree.addCollision("4711", "JustANumber");
    assertTrue(tree.getCollisions().containsKey("4711"));
    assertTrue(tree.getCollisions().containsValue("JustANumber"));
    assertEquals(1, tree.getCollisions().size());

    tree.addCollision("010203", "AABBCCDD");
    assertTrue(treeClient.flushTreeCollisions(tree)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    assertEquals(0, tree.getCollisions().size());
  }

  @Test
  public void testFlushTreeNotMatchedStoreFailures() throws Exception {
    final Tree tree = new Tree();
    tree.setStoreFailures(false);
    tree.addNotMatched("4711", "JustANumber");
    assertTrue(tree.getNotMatched().containsKey("4711"));
    assertTrue(tree.getNotMatched().containsValue("JustANumber"));
    assertEquals(1, tree.getNotMatched().size());

    assertTrue(treeClient.flushTreeNotMatched(tree).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    assertEquals(0, tree.getNotMatched().size());
  }
  @Test
  public void testFlushTreeNotMatchedNoStoreFailures() throws Exception {
    final Tree tree = new Tree();
    tree.setStoreFailures(true);
    tree.addNotMatched("4711", "JustANumber");
    assertTrue(tree.getNotMatched().containsKey("4711"));
    assertTrue(tree.getNotMatched().containsValue("JustANumber"));
    assertEquals(1, tree.getNotMatched().size());

    assertTrue(treeClient.flushTreeNotMatched(tree).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    assertEquals(1, tree.getNotMatched().size());
  }

  @Test
  public void testStoreLeaf() {
    /*
     * Placeholder test. The method does nothing but forwards the call to the
     * tsdb_store.
     */
  }

  @Test (expected = IllegalArgumentException.class)
  public void testStoreBranchTooLowID() {
    Branch branch = new Branch();
    branch.setTreeId(Const.MIN_TREE_ID_INCLUSIVE - 1);
    treeClient.storeBranch(null, branch, true);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testStoreBranchTooHighID() {
    Branch branch = new Branch();
    branch.setTreeId(Const.MAX_TREE_ID_INCLUSIVE + 1);
    treeClient.storeBranch(null, branch, true);
  }

  @Test
  public void testStoreBranch() throws Exception{
    Branch branch = new Branch();
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {

      branch.setTreeId(id);

      treeClient.storeBranch(null, branch, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }
  }
  @Test (expected = IllegalArgumentException.class)
  public void testStoreBranchMissingTreeID() throws Exception {
    final Branch branch = new Branch();
    treeClient.storeBranch(null, branch, false);
  }

  @Test
  public void testFetchBranchOnly() {
    /*
     * Placeholder test. The method does nothing but forwards the call to the
     * tsdb_store.
     */
  }

  @Test
  public void testFetchBranch() {
    /*
     * Placeholder test. The method does nothing but forwards the call to the
     * tsdb_store.
     */
  }

  @Test (expected = IllegalArgumentException.class)
  public void testSyncTreeRuleToStorageTooLowID() {
    TreeRule rule = new TreeRule(Const.MIN_TREE_ID_INCLUSIVE - 1);
    treeClient.syncTreeRuleToStorage(rule, true);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testSyncTreeRuleToStorageTooHighID() {
    TreeRule rule = new TreeRule(Const.MAX_TREE_ID_INCLUSIVE + 1);
    treeClient.syncTreeRuleToStorage(rule, true);
  }

  @Test
  public void testFestSyncTreeRuleToStorage() {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      TreeRule rule = new TreeRule(id);
      treeClient.syncTreeRuleToStorage(rule, true);
    }

  }

  @Test
  public void testFetchTreeRuleTooLowID() {
    try {
      treeClient.fetchTreeRule(Const.MIN_TREE_ID_INCLUSIVE - 1, 0, 0);
    } catch (IllegalArgumentException e){
      assertEquals("Invalid Tree ID", e.getMessage());
    }
  }

  @Test
  public void testFetchTreeRuleTooHighID() {
    try {
      treeClient.fetchTreeRule(Const.MAX_TREE_ID_INCLUSIVE + 1, 0, 0);
    } catch (IllegalArgumentException e){
      assertEquals("Invalid Tree ID", e.getMessage());
    }
  }

  @Test
  public void testFetchTreeRuleInvalidLevel() {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      try {
        treeClient.fetchTreeRule(id, -1, 0);
      } catch (IllegalArgumentException e){
        assertEquals("Invalid rule level" ,e.getMessage());
      }
    }
  }

  @Test
  public void testFetchTreeRuleInvalidOrder() {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      try {
        treeClient.fetchTreeRule(id, 0, -1);
      } catch (IllegalArgumentException e){
        assertEquals("Invalid rule order" , e.getMessage());
      }
    }
  }

  @Test
  public void testFetchTreeRule() throws Exception {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      assertNull(treeClient.fetchTreeRule(id, 0, 0).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    }
  }

  @Test
  public void testDeleteTreeRuleTooLowID() {
    try {
      treeClient.deleteTreeRule(Const.MIN_TREE_ID_INCLUSIVE - 1, -1, -1);
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid Tree ID" , e.getMessage());
    }
  }

  @Test
  public void testDeleteTreeRuleTooHighID() {
    try {
      treeClient.deleteTreeRule(Const.MAX_TREE_ID_INCLUSIVE + 1, -1, -1);
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid Tree ID" , e.getMessage());
    }
  }

  @Test
  public void testDeleteTreeRuleInvalidLevel() {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      try {
        treeClient.deleteTreeRule(id, -1, 0);
      } catch (IllegalArgumentException e) {
        assertEquals("Invalid rule level" , e.getMessage());
      }
    }
  }

  @Test
  public void testDeleteTreeRuleInvalidOrder() {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      try {
        treeClient.deleteTreeRule(id, 0, -1);
      } catch (IllegalArgumentException e) {
        assertEquals("Invalid rule order" , e.getMessage());
      }
    }
  }

  @Test
  public void testDeleteTreeRule() {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      treeClient.deleteTreeRule(id, 0, 0);
    }
  }

  @Test (expected = IllegalArgumentException.class)
  public void testDeleteAllTreeRulesTooLowID() {
    treeClient.deleteAllTreeRules(Const.MIN_TREE_ID_INCLUSIVE - 1);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testDeleteAllTreeRulesTooHighID() {
    treeClient.deleteAllTreeRules(Const.MAX_TREE_ID_INCLUSIVE + 1);
  }

  @Test
  public void testDeleteAllTreeRules() {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      treeClient.deleteAllTreeRules(id);
    }
  }
}