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


import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

import static org.junit.Assert.*;


public final class TestBranch {
  private Tree tree = TestTree.buildTestTree();

  @Test
  public void copyConstructor() {
    final Branch branch = buildTestBranch(tree);
    final Branch copy = new Branch(branch);
    assertEquals(1, copy.getTreeId());
    assertEquals("ROOT", copy.getDisplayName());
    assertNotNull(copy.getBranches());
    assertTrue(copy.getBranches() != branch.getBranches());
    assertNotNull(copy.getLeaves());
    assertTrue(copy.getLeaves() != branch.getLeaves());
    assertNotNull(copy.getPath());
    assertTrue(copy.getPath() != branch.getPath());
  }
  
  @Test
  public void testHashCode() {
    final Branch branch = buildTestBranch(tree);
    assertEquals(2521314, branch.hashCode());
  }
  
  @Test
  public void testEquals() {
    final Branch branch = buildTestBranch(tree);
    final Branch branch2 = buildTestBranch(tree);
    assertTrue(branch.equals(branch2));
  }
  
  @Test
  public void equalsSameAddress() {
    final Branch branch = buildTestBranch(tree);
    assertTrue(branch.equals(branch));
  }
  
  @Test
  public void equalsNull() {
    final Branch branch = buildTestBranch(tree);
    assertFalse(branch == null);
  }
  
  @Test
  public void equalsWrongClass() {
    final Branch branch = buildTestBranch(tree);
    assertFalse(branch.equals(new Object()));
  }
  
  @Test
  public void compareTo() {
    final Branch branch = buildTestBranch(tree);
    final Branch branch2 = buildTestBranch(tree);
    assertEquals(0, branch.compareTo(branch2));
  }
  
  @Test
  public void compareToLess() {
    final Branch branch = buildTestBranch(tree);
    final Branch branch2 = buildTestBranch(tree);
    branch2.setDisplayName("Ardvark");
    assertTrue(branch.compareTo(branch2) > 0);
  }
  
  @Test
  public void compareToGreater() {
    final Branch branch = buildTestBranch(tree);
    final Branch branch2 = buildTestBranch(tree);
    branch2.setDisplayName("Zelda");
    assertTrue(branch.compareTo(branch2) < 0);
  }
  
  @Test
  public void getBranchIdRoot() {
    final Branch branch = buildTestBranch(tree);
    assertEquals("0001", branch.getBranchId());
  }
  
  @Test
  public void getBranchIdChild() {
    final Branch branch = buildTestBranch(tree);
    assertEquals("0001D119F20E", branch.getBranches().first().getBranchId());
  }
  
  @Test
  public void addChild() throws Exception {
    final Branch branch = buildTestBranch(tree);
    final Branch child = new Branch(tree.getTreeId());
    assertTrue(branch.addChild(child));
    assertEquals(3, branch.getBranches().size());
    assertEquals(2, branch.getLeaves().size());
  }
  
  @Test
  public void addChildNoLocalBranches() throws Exception {
    final TreeMap<Integer, String> root_path = new TreeMap<Integer, String>();
    final Branch branch = new Branch(tree.getTreeId());
    branch.setDisplayName("ROOT");
    root_path.put(0, "ROOT");
    branch.prependParentPath(root_path);

    Leaf leaf = new Leaf("Alarms", "ABCD");
    branch.addLeaf(leaf, tree);

    leaf = new Leaf("Employees in Office", "EF00");
    branch.addLeaf(leaf, tree);

    final Branch child = new Branch(tree.getTreeId());
    assertTrue(branch.addChild(child));
    assertEquals(1, branch.getBranches().size());
    assertEquals(2, branch.getLeaves().size());
  }
  
  @Test
  public void addChildNoChanges() throws Exception {
    final Branch branch = buildTestBranch(tree);
    final Branch child = new Branch(tree.getTreeId());
    assertTrue(branch.addChild(child));
    assertFalse(branch.addChild(child));
    assertEquals(3, branch.getBranches().size());
    assertEquals(2, branch.getLeaves().size());
  }
  
  @Test
  public void addLeafExists() throws Exception {
    final Tree tree = TestTree.buildTestTree();
    final Branch branch = buildTestBranch(tree);

    Leaf leaf = new Leaf();
    leaf.setDisplayName("Alarms");
    leaf.setTsuid("ABCD");

    assertFalse(branch.addLeaf(leaf, tree));
    assertEquals(2, branch.getBranches().size());
    assertEquals(2, branch.getLeaves().size());
    assertNull(tree.getCollisions());
  }
  
  @Test
  public void addLeafCollision() throws Exception {
    final Tree tree = TestTree.buildTestTree();
    final Branch branch = buildTestBranch(tree);

    Leaf leaf = new Leaf();
    leaf.setDisplayName("Alarms");
    leaf.setTsuid("0001");

    assertFalse(branch.addLeaf(leaf, tree));
    assertEquals(2, branch.getBranches().size());
    assertEquals(2, branch.getLeaves().size());
    assertEquals(1, tree.getCollisions().size());
  }

  @Test (expected = IllegalArgumentException.class)
  public void addChildNull() throws Exception {
    final Branch branch = buildTestBranch(tree);
    branch.addChild(null);
  }

  @Test
  public void addLeaf() throws Exception {
    final Branch branch = buildTestBranch(tree);
    
    Leaf leaf = new Leaf();
    leaf.setDisplayName("Application Servers");
    leaf.setTsuid("0004");
    
    assertTrue(branch.addLeaf(leaf, null));
  }

  @Test (expected = IllegalArgumentException.class)
  public void addLeafNull() throws Exception {
    final Branch branch = buildTestBranch(tree);
    branch.addLeaf(null, null);
  }
  
  @Test
  public void compileBranchId() {
    final Branch branch = buildTestBranch(tree);
    assertArrayEquals(new byte[] { 0, 1 }, branch.compileBranchId());
  }
  
  @Test
  public void compileBranchIdChild() {
    final Branch branch = buildTestBranch(tree);
    assertArrayEquals(new byte[] { 0, 1 , (byte) 0xD1, 0x19, (byte) 0xF2, 0x0E }, 
        branch.getBranches().first().compileBranchId());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void compileBranchIdEmptyDisplayName() {
    final Branch branch = new Branch(1);
    branch.compileBranchId();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void compileBranchIdInvalidId() {
    final Branch branch = new Branch(0);
    branch.compileBranchId();
  }
  
  @Test
  public void idToString() throws Exception {
    assertEquals("0EA8", Branch.idToString(new byte[] { 0x0E, (byte) 0xA8 }));
  }
  
  @Test
  public void idToStringZeroes() throws Exception {
    assertEquals("0000", Branch.idToString(new byte[] { 0, 0 }));
  }
  
  @Test (expected = NullPointerException.class)
  public void idToStringNull() throws Exception {
    Branch.idToString(null);
  }
  
  @Test
  public void stringToId() throws Exception {
    assertArrayEquals(new byte[] { 0x0E, (byte) 0xA8 }, 
        Branch.stringToId("0EA8"));
  }
  
  @Test
  public void stringToIdZeros() throws Exception {
    assertArrayEquals(new byte[] { 0, 0 }, Branch.stringToId("0000"));
  }
  
  @Test
  public void stringToIdZerosPadding() throws Exception {
    assertArrayEquals(new byte[] { 0, 0, 0 }, Branch.stringToId("00000"));
  }
  
  @Test
  public void stringToIdCase() throws Exception {
    assertArrayEquals(new byte[] { 0x0E, (byte) 0xA8 }, 
        Branch.stringToId("0ea8"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToIdNull() throws Exception {
    Branch.stringToId(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToIdEmpty() throws Exception {
    Branch.stringToId("");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToIdTooShort() throws Exception {
    Branch.stringToId("01");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToIdNotHex() throws Exception {
    Branch.stringToId("HelloWorld!");
  }
  
  @Test
  public void prependParentPath() throws Exception {
    Branch branch = new Branch(1);
    branch.setDisplayName("cpu");
    final TreeMap<Integer, String> path = new TreeMap<Integer, String>();
    path.put(0, "ROOT");
    path.put(1, "sys");
    branch.prependParentPath(path);
       
    final Map<Integer, String> compiled_path = branch.getPath();
    assertNotNull(compiled_path);
    assertEquals(3, compiled_path.size());
  }
  
  @Test
  public void prependParentPathEmpty() throws Exception {
    Branch branch = new Branch(1);
    branch.setDisplayName("cpu");
    final TreeMap<Integer, String> path = new TreeMap<Integer, String>();
    branch.prependParentPath(path);
       
    final Map<Integer, String> compiled_path = branch.getPath();
    assertNotNull(compiled_path);
    assertEquals(1, compiled_path.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void prependParentPathNull() throws Exception {
    new Branch().prependParentPath(null);
  }

  @Test
  public void testGetTreeId() {
    Branch branch = buildTestBranch(tree);
    assertEquals(1, branch.getTreeId());
  }

  @Test
  public void testGetDisplayName() {
    Branch branch = buildTestBranch(tree);
    assertEquals("ROOT", branch.getDisplayName());
  }

  @Test
  public void testGetLeaves() {
    Branch branch = buildTestBranch(tree);
    assertEquals(2, branch.getLeaves().size());
  }

  @Test
  public void testGetBranches() {
    Branch branch = buildTestBranch(tree);
    assertEquals(2, branch.getBranches().size());
  }

  @Test
  public void testSetTreeId() {
    Branch branch = buildTestBranch(tree);

    assertEquals(1, branch.getTreeId());
    branch.setTreeId(2);
    assertEquals(2, branch.getTreeId());
  }

  @Test
  public void testSetDisplayName() {
    Branch branch = buildTestBranch(tree);
    assertEquals("ROOT", branch.getDisplayName());
    branch.setDisplayName("ROOT2");
    assertEquals("ROOT2", branch.getDisplayName());
  }

  @Test
  public void testSetPath() {
    Branch branch = buildTestBranch(tree);
    assertEquals(1, branch.getPath().size());
    assertEquals("ROOT", branch.getPath().get(0));
  }

  @Test
  public void testAddLeaf() {
    final TreeMap<Integer, String> root_path = new TreeMap<Integer, String>();
    final Branch root = new Branch(tree.getTreeId());
    root.setDisplayName("ROOT");
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);

    assertNull(root.getLeaves());

    Leaf leaf = new Leaf("Alarms", "ABCD");
    root.addLeaf(leaf, tree);

    assertNotNull(root.getLeaves());
    assertEquals(1, root.getLeaves().size());
  }

  /**
   * Helper to build a default branch for testing
   * @return A branch with some child branches and leaves
   */
  public static Branch buildTestBranch(final Tree tree) {
    final TreeMap<Integer, String> root_path = new TreeMap<Integer, String>();
    final Branch root = new Branch(tree.getTreeId());
    root.setDisplayName("ROOT");
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);

    Branch child = new Branch(1);
    child.prependParentPath(root_path);
    child.setDisplayName("System");
    root.addChild(child);

    child = new Branch(tree.getTreeId());
    child.prependParentPath(root_path);
    child.setDisplayName("Network");
    root.addChild(child);

    Leaf leaf = new Leaf("Alarms", "ABCD");
    root.addLeaf(leaf, tree);

    leaf = new Leaf("Employees in Office", "EF00");
    root.addLeaf(leaf, tree);

    return root;
  }
}
