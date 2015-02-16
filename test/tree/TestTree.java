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


import net.opentsdb.tree.TreeRule.TreeRuleType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertArrayEquals;

public final class TestTree {

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
  public void getRule() throws Exception {
    final TreeRule rule = buildTestTree().getRule(3, 0);
    assertNotNull(rule);
    assertEquals(TreeRuleType.METRIC, rule.getType());
  }
  
  @Test
  public void getRuleNullSet() throws Exception {
    final Tree tree = new Tree(1);
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

    tree.initializeChangedMap();

    return tree;
  }


}
