// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tree;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule.TreeRuleType;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, HBaseClient.class, GetRequest.class,
  PutRequest.class, KeyValue.class, Scanner.class, DeleteRequest.class})
public final class TestTree {
  private MockBase storage;
  private TSDB tsdb;
  private HBaseClient client = mock(HBaseClient.class);
  
  final static private Method TreetoStorageJson;
  static {
    try {
      TreetoStorageJson = Tree.class.getDeclaredMethod("toStorageJson");
      TreetoStorageJson.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  @Before
  public void before() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.storage.enable_compaction", "false");
    PowerMockito.whenNew(HBaseClient.class)
      .withArguments(anyString(), anyString()).thenReturn(client);
    tsdb = new TSDB(client, config);
  }
  
  @Test
  public void copyConstructor() {
    final Tree tree = buildTestTree();
    tree.setStrictMatch(true);
    final Tree copy = new Tree(tree);
    
    assertEquals(1, copy.getTreeId());
    assertEquals(1356998400L, copy.getCreated());
    assertEquals("My Description", copy.getDescription());
    assertEquals("Test Tree", copy.getName());
    assertEquals("Details", copy.getNotes());
    assertTrue(copy.getStrictMatch());
    assertTrue(copy.getEnabled());
    assertNull(copy.getCollisions());
    assertNull(copy.getNotMatched());
    assertNotNull(copy.getRules());
    assertTrue(copy.getRules() != tree.getRules());
  }
  
  @Test
  public void copyChanges() throws Exception {
    final Tree tree = buildTestTree();
    final Tree tree2 = buildTestTree();
    tree2.setName("Different Tree");
    assertTrue(tree.copyChanges(tree2, false));
    assertEquals("Different Tree", tree.getName());
  }
  
  @Test
  public void copyChangesNone() throws Exception {
    final Tree tree = buildTestTree();
    final Tree tree2 = buildTestTree();
    assertFalse(tree.copyChanges(tree2, false));
  }
  
  @Test
  public void copyChangesOverride() throws Exception {
    final Tree tree = buildTestTree();
    final Tree tree2 = new Tree(1);
    assertTrue(tree.copyChanges(tree2, true));
    assertTrue(tree.getName().isEmpty());
    assertTrue(tree.getDescription().isEmpty());
    assertTrue(tree.getNotes().isEmpty());
  }

  @Test
  public void serialize() throws Exception {
    final String json = JSON.serializeToString(buildTestTree());
    assertNotNull(json);
    assertTrue(json.contains("\"created\":1356998400"));
    assertTrue(json.contains("\"name\":\"Test Tree\""));
    assertTrue(json.contains("\"description\":\"My Description\""));
    assertTrue(json.contains("\"enabled\":true"));
  }

  @Test
  public void deserialize() throws Exception {
    Tree t = JSON.parseToObject((byte[])TreetoStorageJson.invoke(
        buildTestTree()), Tree.class);
    assertTrue(t.getEnabled());
  }
  
  @Test
  public void addRule() throws Exception {
    final Tree tree = new Tree();
    tree.addRule(new TreeRule());
    assertNotNull(tree.getRules());
    assertEquals(1, tree.getRules().size());
  }
  
  @Test
  public void addRuleLevel() throws Exception {
    final Tree tree = new Tree();
    TreeRule rule = new TreeRule(1);
    rule.setDescription("MyRule");
    rule.setLevel(1);
    rule.setOrder(1);
    tree.addRule(rule);
    assertNotNull(tree.getRules());
    assertEquals(1, tree.getRules().size());
    assertEquals("MyRule", tree.getRules().get(1).get(1).getDescription());
    
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRuleNull() throws Exception {
    final Tree tree = new Tree();
    tree.addRule(null);
  }

  @Test
  public void addCollision() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getCollisions());
    tree.addCollision("010203", "AABBCCDD");
    assertEquals(1, tree.getCollisions().size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addCollisionNull() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getCollisions());
    tree.addCollision(null, "AABBCCDD");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addCollisionEmpty() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getCollisions());
    tree.addCollision("", "AABBCCDD");
  }
  
  @Test
  public void addNoMatch() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getNotMatched());
    tree.addNotMatched("010203", "Bummer");
    assertEquals(1, tree.getNotMatched().size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addNoMatchNull() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getNotMatched());
    tree.addNotMatched(null, "Bummer");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addNoMatchEmpty() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getNotMatched());
    tree.addNotMatched("", "Bummer");
  }
  
  @Test
  public void storeTree() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.setName("New Name");
    assertNotNull(tree.storeTree(storage.getTSDB(), false)
        .joinUninterruptibly());
  }
  
  @Test (expected = IllegalStateException.class)
  public void storeTreeNoChanges() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.storeTree(storage.getTSDB(), false);
    tree.storeTree(storage.getTSDB(), false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeTreeTreeID0() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.setTreeId(0);
    tree.storeTree(storage.getTSDB(), false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeTreeTreeID655536() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.setTreeId(655536);
    tree.storeTree(storage.getTSDB(), false);
  }
  
  @Test
  public void flushCollisions() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.setStoreFailures(true);
    tree.addCollision("010203", "AABBCCDD");
    assertNotNull(tree.flushCollisions(storage.getTSDB())
        .joinUninterruptibly());
    assertEquals(4, storage.numRows());
    assertEquals(3, storage.numColumns(new byte[] { 0, 1, 1 }));
  }
  
  @Test
  public void flushCollisionsDisabled() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.addCollision("010203", "AABBCCDD");
    assertNotNull(tree.flushCollisions(storage.getTSDB())
        .joinUninterruptibly());
    assertEquals(4, storage.numRows());
    assertEquals(2, storage.numColumns(new byte[] { 0, 1, 1 }));
  }
  
  @Test
  public void flushCollisionsWCollisionExisting() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.addCollision("010101", "AAAAAA");
    assertNotNull(tree.flushCollisions(storage.getTSDB())
        .joinUninterruptibly());
    assertEquals(4, storage.numRows());
    assertEquals(2, storage.numColumns(new byte[] { 0, 1, 1 }));
  }
  
  @Test
  public void flushNotMatched() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.setStoreFailures(true);
    tree.addNotMatched("010203", "Failed rule 2:2");
    assertNotNull(tree.flushNotMatched(storage.getTSDB())
        .joinUninterruptibly());
    assertEquals(4, storage.numRows());
    assertEquals(3, storage.numColumns(new byte[] { 0, 1, 2 }));
  }
  
  @Test
  public void flushNotMatchedDisabled() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.addNotMatched("010203", "Failed rule 2:2");
    assertNotNull(tree.flushNotMatched(storage.getTSDB())
        .joinUninterruptibly());
    assertEquals(4, storage.numRows());
    assertEquals(2, storage.numColumns(new byte[] { 0, 1, 2 }));
  }
  
  @Test
  public void flushNotMatchedWNotMatchedExisting() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.addNotMatched("010101", "Failed rule 4:4");
    assertNotNull(tree.flushNotMatched(storage.getTSDB())
        .joinUninterruptibly());
    assertEquals(4, storage.numRows());
    assertEquals(2, storage.numColumns(new byte[] { 0, 1, 2 }));
  }
  
  @Test
  public void getRule() throws Exception {
    final TreeRule rule = buildTestTree().getRule(3, 0);
    assertNotNull(rule);
    assertEquals(TreeRuleType.METRIC, rule.getType());
  }
  
  @Test
  public void getRuleNullSet() throws Exception {
    final Tree tree = buildTestTree();
    Field rules = Tree.class.getDeclaredField("rules");
    rules.setAccessible(true);
    rules.set(tree, null);
    rules.setAccessible(false);
    assertNull(tree.getRule(3, 0));
  }
  
  @Test
  public void getRuleNoLevel() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getRule(42, 0));
  }
  
  @Test
  public void getRuleNoOrder() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getRule(3, 42));
  }

  @Test
  public void createNewTree() throws Exception {
    setupStorage(true, true);
    final Tree tree = new Tree();
    tree.setName("New Tree");
    final int tree_id = tree.createNewTree(storage.getTSDB())
    .joinUninterruptibly();
    assertEquals(3, tree_id);
    assertEquals(5, storage.numRows());
    assertEquals(1, storage.numColumns(new byte[] { 0, 3 }));
  }

  @Test
  public void createNewFirstTree() throws Exception {
    setupStorage(true, true);
    storage.flushStorage();
    final Tree tree = new Tree();
    tree.setName("New Tree");
    final int tree_id = tree.createNewTree(storage.getTSDB())
    .joinUninterruptibly();
    assertEquals(1, tree_id);
    assertEquals(1, storage.numRows());
    assertEquals(1, storage.numColumns(new byte[] { 0, 1 }));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void createNewTreeNoChanges() throws Exception {
    setupStorage(true, true);
    final Tree tree = new Tree();
    tree.createNewTree(storage.getTSDB()).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void createNewTreeOutOfIDs() throws Exception {
    setupStorage(true, true);

    final Tree max_tree = new Tree(65535);
    max_tree.setName("max");
    storage.addColumn(new byte[] { (byte) 0xFF, (byte) 0xFF }, 
        "tree".getBytes(MockBase.ASCII()), JSON.serializeToBytes(max_tree));
    
    final Tree tree = new Tree();
    tree.createNewTree(storage.getTSDB()).joinUninterruptibly();
  }

  @Test
  public void fetchTree() throws Exception {
    setupStorage(true, true);
    final Tree tree = Tree.fetchTree(storage.getTSDB(), 1)
    .joinUninterruptibly();
    assertNotNull(tree);
    assertEquals("Test Tree", tree.getName());
    assertEquals(2, tree.getRules().size());
    assertTrue(tree.getEnabled());
  }
  
  @Test
  public void fetchTreeDoesNotExist() throws Exception {
    setupStorage(true, true);
    assertNull(Tree.fetchTree(storage.getTSDB(), 3).joinUninterruptibly());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchTreeID0() throws Exception {
    setupStorage(true, true);
    Tree.fetchTree(storage.getTSDB(), 0);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchTreeID65536() throws Exception {
    setupStorage(true, true);
    Tree.fetchTree(storage.getTSDB(), 65536);
  }
  
  @Test
  public void fetchAllTrees() throws Exception {
    setupStorage(true, true);
    final List<Tree> trees = Tree.fetchAllTrees(storage.getTSDB())
    .joinUninterruptibly();
    assertNotNull(trees);
    assertEquals(2, trees.size());
  }
  
  @Test
  public void fetchAllTreesNone() throws Exception {
    setupStorage(true, true);
    storage.flushStorage();
    final List<Tree> trees = Tree.fetchAllTrees(storage.getTSDB())
    .joinUninterruptibly();
    assertNotNull(trees);
    assertEquals(0, trees.size());
  }

  @Test
  public void fetchAllCollisions() throws Exception {
    setupStorage(true, true);
    Map<String, String> collisions = 
      Tree.fetchCollisions(storage.getTSDB(), 1, null).joinUninterruptibly();
    assertNotNull(collisions);
    assertEquals(2, collisions.size());
    assertTrue(collisions.containsKey("010101"));
    assertTrue(collisions.containsKey("020202"));
  }
  
  @Test
  public void fetchAllCollisionsNone() throws Exception {
    setupStorage(true, true);
    storage.flushRow(new byte[] { 0, 1, 1 });
    Map<String, String> collisions = 
      Tree.fetchCollisions(storage.getTSDB(), 1, null).joinUninterruptibly();
    assertNotNull(collisions);
    assertEquals(0, collisions.size());
  }
  
  @Test
  public void fetchCollisionsSingle() throws Exception {
    setupStorage(true, true);
    final ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("020202");
    Map<String, String> collisions = 
      Tree.fetchCollisions(storage.getTSDB(), 1, tsuids).joinUninterruptibly();
    assertNotNull(collisions);
    assertEquals(1, collisions.size());
    assertTrue(collisions.containsKey("020202"));
  }
  
  @Test
  public void fetchCollisionsSingleNotFound() throws Exception {
    setupStorage(true, true);
    final ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("030303");
    Map<String, String> collisions = 
      Tree.fetchCollisions(storage.getTSDB(), 1, tsuids).joinUninterruptibly();
    assertNotNull(collisions);
    assertEquals(0, collisions.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchCollisionsID0() throws Exception {
    setupStorage(true, true);
    Tree.fetchCollisions(storage.getTSDB(), 0, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchCollisionsID655536() throws Exception {
    setupStorage(true, true);
    Tree.fetchCollisions(storage.getTSDB(), 655536, null);
  }
  
  @Test
  public void fetchAllNotMatched() throws Exception {
    setupStorage(true, true);
    Map<String, String> not_matched = 
      Tree.fetchNotMatched(storage.getTSDB(), 1, null).joinUninterruptibly();
    assertNotNull(not_matched);
    assertEquals(2, not_matched.size());
    assertTrue(not_matched.containsKey("010101"));
    assertEquals("Failed rule 0:0", not_matched.get("010101"));
    assertTrue(not_matched.containsKey("020202"));
    assertEquals("Failed rule 1:1", not_matched.get("020202"));
  }
  
  @Test
  public void fetchAllNotMatchedNone() throws Exception {
    setupStorage(true, true);
    storage.flushRow(new byte[] { 0, 1, 2 });
    Map<String, String> not_matched = 
      Tree.fetchNotMatched(storage.getTSDB(), 1, null).joinUninterruptibly();
    assertNotNull(not_matched);
    assertEquals(0, not_matched.size());
  }
  
  @Test
  public void fetchNotMatchedSingle() throws Exception {
    setupStorage(true, true);
    final ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("020202");
    Map<String, String> not_matched = 
      Tree.fetchNotMatched(storage.getTSDB(), 1, tsuids).joinUninterruptibly();
    assertNotNull(not_matched);
    assertEquals(1, not_matched.size());
    assertTrue(not_matched.containsKey("020202"));
    assertEquals("Failed rule 1:1", not_matched.get("020202"));
  }
  
  @Test
  public void fetchNotMatchedSingleNotFound() throws Exception {
    setupStorage(true, true);
    final ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("030303");
    Map<String, String> not_matched = 
      Tree.fetchNotMatched(storage.getTSDB(), 1, tsuids).joinUninterruptibly();
    assertNotNull(not_matched);
    assertEquals(0, not_matched.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchNotMatchedID0() throws Exception {
    setupStorage(true, true);
    Tree.fetchNotMatched(storage.getTSDB(), 0, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchNotMatchedID655536() throws Exception {
    setupStorage(true, true);
    Tree.fetchNotMatched(storage.getTSDB(), 655536, null);
  }

  @Test
  public void deleteTree() throws Exception {
    setupStorage(true, true);

    assertEquals(4, storage.numRows());
    assertNotNull(Tree.deleteTree(storage.getTSDB(), 1, true)
        .joinUninterruptibly());

    byte[] remainingKey = new byte[] {0, 2};
    assertEquals(1, storage.numRows());
    assertNotNull(storage.getColumn(
        remainingKey, "tree".getBytes(MockBase.ASCII())));
  }

  @Test
  public void idToBytes() throws Exception {
    assertArrayEquals(new byte[]{ 0, 1 }, Tree.idToBytes(1));
  }
  
  @Test
  public void idToBytesMax() throws Exception {
    assertArrayEquals(new byte[]{ (byte) 0xFF, (byte) 0xFF }, 
        Tree.idToBytes(65535));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void idToBytesBadID0() throws Exception {
    Tree.idToBytes(0);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void idToBytesBadID655536() throws Exception {
    Tree.idToBytes(655536);
  }
  
  @Test
  public void bytesToId() throws Exception {
    assertEquals(1, Tree.bytesToId(new byte[] { 0, 1 }));
  }
  
  @Test
  public void bytesToIdMetaRow() throws Exception {
    assertEquals(1, Tree.bytesToId(new byte[] { 0, 1, 1 }));
  }

  @Test
  public void bytesToIdBranchRow() throws Exception {
    assertEquals(1, Tree.bytesToId(new byte[] { 0, 1, 4, 2, 1, 0 }));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void bytesToIdBadRow() throws Exception {
    Tree.bytesToId(new byte[] { 1 });
  }
  
  /**
   * Returns a 5 level rule set that parses a data center, a service, the 
   * hostname, metric and some tags from meta data.
   * @param tree The tree to add the rules to
   */
  public static void buildTestRuleSet(final Tree tree) {

    // level 0
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setRegex("^.*\\.([a-zA-Z]{3,4})[0-9]{0,1}\\..*\\..*$");
    rule.setField("fqdn");
    rule.setDescription("Datacenter");
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setRegex("^.*\\.([a-zA-Z]{3,4})[0-9]{0,1}\\..*\\..*$");
    rule.setField("host");
    rule.setDescription("Datacenter");
    rule.setOrder(1);
    tree.addRule(rule);
    
    // level 1
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setRegex("^([a-zA-Z]+)(\\-|[0-9])*.*\\..*$");
    rule.setField("fqdn");
    rule.setDescription("Service");
    rule.setLevel(1);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setRegex("^([a-zA-Z]+)(\\-|[0-9])*.*\\..*$");
    rule.setField("host");
    rule.setDescription("Service");
    rule.setLevel(1);
    rule.setOrder(1);
    tree.addRule(rule);
    
    // level 2
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setField("fqdn");
    rule.setDescription("Hostname");
    rule.setLevel(2);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setField("host");
    rule.setDescription("Hostname");
    rule.setLevel(2);
    rule.setOrder(1);
    tree.addRule(rule);
    
    // level 3
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.METRIC);
    rule.setDescription("Metric split");
    rule.setSeparator("\\.");
    rule.setLevel(3);
    tree.addRule(rule);
    
    // level 4
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setField("type");
    rule.setDescription("Type Tag");
    rule.setLevel(4);
    rule.setOrder(0);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setField("method");
    rule.setDescription("Method Tag");
    rule.setLevel(4);
    rule.setOrder(1);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setField("port");
    rule.setDescription("Port Tag");
    rule.setDisplayFormat("Port: {value}");
    rule.setLevel(4);
    rule.setOrder(2);
    tree.addRule(rule);
  }
  
  /**
   * Returns a configured tree with rules and values for testing purposes
   * @return A tree to test with
   */
  public static Tree buildTestTree() {
    final Tree tree = new Tree();
    tree.setTreeId(1);
    tree.setCreated(1356998400L);
    tree.setDescription("My Description");
    tree.setName("Test Tree");
    tree.setNotes("Details");
    tree.setEnabled(true);
    buildTestRuleSet(tree);
    
    // reset the changed field via reflection
    Method reset;
    try {
      reset = Tree.class.getDeclaredMethod("initializeChangedMap");
      reset.setAccessible(true);
      reset.invoke(tree);
      reset.setAccessible(false);
    // Since some other tests are calling this as a constructor, we can't throw
    // exceptions. So just print them.
    } catch (SecurityException e) {
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    }
    return tree;
  }

  /**
   * Mocks classes for testing the storage calls
   */
  private void setupStorage(final boolean default_get, 
      final boolean default_put) throws Exception {
    storage = new MockBase(tsdb, client, default_get, default_put, true, true);
    
    byte[] key = new byte[] { 0, 1 };
    // set pre-test values
    storage.addColumn(key, "tree".getBytes(MockBase.ASCII()), 
        (byte[])TreetoStorageJson.invoke(buildTestTree()));

    TreeRule rule = new TreeRule(1);
    rule.setField("host");
    rule.setType(TreeRuleType.TAGK);
    storage.addColumn(key, "tree_rule:0:0".getBytes(MockBase.ASCII()), 
        JSON.serializeToBytes(rule));

    rule = new TreeRule(1);
    rule.setField("");
    rule.setLevel(1);
    rule.setType(TreeRuleType.METRIC);
    storage.addColumn(key, "tree_rule:1:0".getBytes(MockBase.ASCII()), 
        JSON.serializeToBytes(rule));
    
    Branch root = new Branch(1);
    root.setDisplayName("ROOT");
    TreeMap<Integer, String> root_path = new TreeMap<Integer, String>();
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    // TODO - static
    Method branch_json = Branch.class.getDeclaredMethod("toStorageJson");
    branch_json.setAccessible(true);
    storage.addColumn(key, "branch".getBytes(MockBase.ASCII()), 
        (byte[])branch_json.invoke(root));
    
    // tree 2
    key = new byte[] { 0, 2 };

    Tree tree2 = new Tree();
    tree2.setTreeId(2);
    tree2.setName("2nd Tree");
    tree2.setDescription("Other Tree");
    storage.addColumn(key, "tree".getBytes(MockBase.ASCII()), 
        (byte[])TreetoStorageJson.invoke(tree2));
    
    rule = new TreeRule(2);
    rule.setField("host");
    rule.setType(TreeRuleType.TAGK);
    storage.addColumn(key, "tree_rule:0:0".getBytes(MockBase.ASCII()), 
        JSON.serializeToBytes(rule));
    
    rule = new TreeRule(2);
    rule.setField("");
    rule.setLevel(1);
    rule.setType(TreeRuleType.METRIC);
    storage.addColumn(key, "tree_rule:1:0".getBytes(MockBase.ASCII()), 
        JSON.serializeToBytes(rule));
    
    root = new Branch(2);
    root.setDisplayName("ROOT");
    root_path = new TreeMap<Integer, String>();
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    storage.addColumn(key, "branch".getBytes(MockBase.ASCII()), 
        (byte[])branch_json.invoke(root));
    
    // sprinkle in some collisions and no matches for fun
    // collisions
    key = new byte[] { 0, 1, 1 };
    String tsuid = "010101";
    byte[] qualifier = new byte[Tree.COLLISION_PREFIX().length + 
                                      (tsuid.length() / 2)];
    System.arraycopy(Tree.COLLISION_PREFIX(), 0, qualifier, 0, 
        Tree.COLLISION_PREFIX().length);
    byte[] tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.COLLISION_PREFIX().length, 
        tsuid_bytes.length);
    storage.addColumn(key, qualifier, "AAAAAA".getBytes(MockBase.ASCII()));
    
    tsuid = "020202";
    qualifier = new byte[Tree.COLLISION_PREFIX().length + 
                                      (tsuid.length() / 2)];
    System.arraycopy(Tree.COLLISION_PREFIX(), 0, qualifier, 0, 
        Tree.COLLISION_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.COLLISION_PREFIX().length, 
        tsuid_bytes.length);
    storage.addColumn(key, qualifier, "BBBBBB".getBytes(MockBase.ASCII()));
    
    // not matched
    key = new byte[] { 0, 1, 2 };
    tsuid = "010101";
    qualifier = new byte[Tree.NOT_MATCHED_PREFIX().length + 
                             (tsuid.length() / 2)];
    System.arraycopy(Tree.NOT_MATCHED_PREFIX(), 0, qualifier, 0, 
        Tree.NOT_MATCHED_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.NOT_MATCHED_PREFIX().length, 
    tsuid_bytes.length);
    storage.addColumn(key, qualifier, "Failed rule 0:0"
        .getBytes(MockBase.ASCII()));
    
    tsuid = "020202";
    qualifier = new byte[Tree.NOT_MATCHED_PREFIX().length + 
                             (tsuid.length() / 2)];
    System.arraycopy(Tree.NOT_MATCHED_PREFIX(), 0, qualifier, 0, 
        Tree.NOT_MATCHED_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.NOT_MATCHED_PREFIX().length, 
    tsuid_bytes.length);
    storage.addColumn(key, qualifier, "Failed rule 1:1"
        .getBytes(MockBase.ASCII()));
    
  }
}
