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
import java.lang.reflect.Method;
import java.util.Map;
import java.util.TreeMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
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
@PrepareForTest({ TSDB.class, HBaseClient.class, GetRequest.class,
  PutRequest.class, KeyValue.class, Scanner.class, DeleteRequest.class })
public final class TestBranch {
  private MockBase storage;
  private Tree tree = TestTree.buildTestTree();
  final static private Method toStorageJson;
  static {
    try {
      toStorageJson = Branch.class.getDeclaredMethod("toStorageJson");
      toStorageJson.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  final static private Method LeaftoStorageJson;
  static {
    try {
      LeaftoStorageJson = Leaf.class.getDeclaredMethod("toStorageJson");
      LeaftoStorageJson.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  @Test
  public void testHashCode() {
    final Branch branch = buildTestBranch(tree);
    assertEquals(2521314, branch.hashCode());
  }
  
  @Test
  public void testEquals() {
    final Branch branch = buildTestBranch(tree);;
    final Branch branch2 = buildTestBranch(tree);;
    assertTrue(branch.equals(branch2));
  }
  
  @Test
  public void equalsSameAddress() {
    final Branch branch = buildTestBranch(tree);;
    assertTrue(branch.equals(branch));
  }
  
  @Test
  public void equalsNull() {
    final Branch branch = buildTestBranch(tree);;
    assertFalse(branch.equals(null));
  }
  
  @Test
  public void equalsWrongClass() {
    final Branch branch = buildTestBranch(tree);;
    assertFalse(branch.equals(new Object()));
  }
  
  @Test
  public void compareTo() {
    final Branch branch = buildTestBranch(tree);;
    final Branch branch2 = buildTestBranch(tree);;
    assertEquals(0, branch.compareTo(branch2));
  }
  
  @Test
  public void compareToLess() {
    final Branch branch = buildTestBranch(tree);;
    final Branch branch2 = buildTestBranch(tree);;
    branch2.setDisplayName("Ardvark");
    assertTrue(branch.compareTo(branch2) > 0);
  }
  
  @Test
  public void compareToGreater() {
    final Branch branch = buildTestBranch(tree);;
    final Branch branch2 = buildTestBranch(tree);;
    branch2.setDisplayName("Zelda");
    assertTrue(branch.compareTo(branch2) < 0);
  }
  
  @Test
  public void getBranchIdRoot() {
    final Branch branch = buildTestBranch(tree);;
    assertEquals("0001", branch.getBranchId());
  }
  
  @Test
  public void getBranchIdChild() {
    final Branch branch = buildTestBranch(tree);;
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
    final Branch branch = buildTestBranch(tree);;
    final Branch child = new Branch(tree.getTreeId());
    Field branches = Branch.class.getDeclaredField("branches");
    branches.setAccessible(true);
    branches.set(branch, null);
    branches.setAccessible(false);
    assertTrue(branch.addChild(child));
    assertEquals(1, branch.getBranches().size());
    assertEquals(2, branch.getLeaves().size());
  }
  
  @Test
  public void addChildNoChanges() throws Exception {
    final Branch branch = buildTestBranch(tree);;
    final Branch child = new Branch(tree.getTreeId());
    assertTrue(branch.addChild(child));
    assertFalse(branch.addChild(child));
    assertEquals(3, branch.getBranches().size());
    assertEquals(2, branch.getLeaves().size());
  }
  
  @Test
  public void addLeafExists() throws Exception {
    final Tree tree = TestTree.buildTestTree();
    final Branch branch = buildTestBranch(tree);;

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
    final Branch branch = buildTestBranch(tree);;

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
    final Branch branch = buildTestBranch(tree);;
    branch.addChild(null);
  }

  @Test
  public void addLeaf() throws Exception {
    final Branch branch = buildTestBranch(tree);;
    
    Leaf leaf = new Leaf();
    leaf.setDisplayName("Application Servers");
    leaf.setTsuid("0004");
    
    assertTrue(branch.addLeaf(leaf, null));
  }

  @Test (expected = IllegalArgumentException.class)
  public void addLeafNull() throws Exception {
    final Branch branch = buildTestBranch(tree);;
    branch.addLeaf(null, null);
  }
  
  @Test
  public void compileBranchId() {
    final Branch branch = buildTestBranch(tree);;
    assertArrayEquals(new byte[] { 0, 1 }, branch.compileBranchId());
  }
  
  @Test
  public void compileBranchIdChild() {
    final Branch branch = buildTestBranch(tree);;
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
  public void fetchBranch() throws Exception {
    setupStorage();
    
    storage.addColumn(new byte[] { 0, 0, 1 }, 
        "metrics".getBytes(MockBase.ASCII()),
        "sys.cpu.0".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1 }, 
        "tagk".getBytes(MockBase.ASCII()),
        "host".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1 }, 
        "tagv".getBytes(MockBase.ASCII()),
        "web01".getBytes(MockBase.ASCII()));
    
    storage.addColumn(new byte[] { 0, 0, 2 }, 
        "metrics".getBytes(MockBase.ASCII()),
        "sys.cpu.1".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 2 }, 
        "tagk".getBytes(MockBase.ASCII()),
        "owner".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 2 }, 
        "tagv".getBytes(MockBase.ASCII()),
        "ops".getBytes(MockBase.ASCII()));
    
    final Branch branch = Branch.fetchBranch(storage.getTSDB(), 
        Branch.stringToId("00010001BECD000181A8"), true).joinUninterruptibly();
    assertNotNull(branch);
    assertEquals(1, branch.getTreeId());
    assertEquals("cpu", branch.getDisplayName());
    assertEquals("00010001BECD000181A8", branch.getBranchId());
    assertEquals(1, branch.getBranches().size());
    assertEquals(2, branch.getLeaves().size());
  }
  
  @Test
  public void fetchBranchNSU() throws Exception {
    setupStorage();
    
    storage.addColumn(new byte[] { 0, 0, 1 }, 
        "metrics".getBytes(MockBase.ASCII()),
        "sys.cpu.0".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1 }, 
        "tagk".getBytes(MockBase.ASCII()),
        "host".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1 }, 
        "tagv".getBytes(MockBase.ASCII()),
        "web01".getBytes(MockBase.ASCII()));
    
    final Branch branch = Branch.fetchBranch(storage.getTSDB(), 
        Branch.stringToId("00010001BECD000181A8"), true).joinUninterruptibly();
    assertNotNull(branch);
    assertEquals(1, branch.getTreeId());
    assertEquals("cpu", branch.getDisplayName());
    assertEquals("00010001BECD000181A8", branch.getBranchId());
    assertEquals(1, branch.getBranches().size());
    assertEquals(1, branch.getLeaves().size());
  }
  
  @Test
  public void fetchBranchNotFound() throws Exception {
    setupStorage();
    final Branch branch = Branch.fetchBranch(storage.getTSDB(), 
        Branch.stringToId("00010001BECD000181A0"), false).joinUninterruptibly();
    assertNull(branch);
  }
  
  @Test
  public void fetchBranchOnly() throws Exception {
    setupStorage();
    final Branch branch = Branch.fetchBranchOnly(storage.getTSDB(), 
        Branch.stringToId("00010001BECD000181A8")).joinUninterruptibly();
    assertNotNull(branch);
    assertEquals("cpu", branch.getDisplayName());
    assertNull(branch.getLeaves());
    assertNull(branch.getBranches());
  }

  @Test
  public void fetchBranchOnlyNotFound() throws Exception {
    setupStorage();
    final Branch branch = Branch.fetchBranchOnly(storage.getTSDB(), 
        Branch.stringToId("00010001BECD000181A0")).joinUninterruptibly();
    assertNull(branch);
  }
  
  @Test
  public void storeBranch() throws Exception {
    setupStorage();
    final Branch branch = buildTestBranch(tree);
    branch.storeBranch(storage.getTSDB(), tree, true);
    assertEquals(3, storage.numRows());
    assertEquals(3, storage.numColumns(new byte[] { 0, 1 }));
    final Branch parsed = JSON.parseToObject(storage.getColumn(
        new byte[] { 0, 1 }, "branch".getBytes(MockBase.ASCII())), 
        Branch.class);
    parsed.setTreeId(1);
    assertEquals("ROOT", parsed.getDisplayName());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeBranchMissingTreeID() throws Exception {
    setupStorage();
    final Branch branch = new Branch();
    branch.storeBranch(storage.getTSDB(), tree, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeBranchTreeID0() throws Exception {
    setupStorage();
    final Branch branch = buildTestBranch(tree);;
    branch.setTreeId(0);
    branch.storeBranch(storage.getTSDB(), tree, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeBranchTreeID65536() throws Exception {
    setupStorage();
    final Branch branch = buildTestBranch(tree);;
    branch.setTreeId(65536);
    branch.storeBranch(storage.getTSDB(), tree, false);
  }

  @Test
  public void storeBranchExistingLeaf() throws Exception {
    setupStorage();
    final Branch branch = buildTestBranch(tree);
    Leaf leaf = new Leaf("Alarms", "ABCD");
    byte[] qualifier = leaf.columnQualifier();
    storage.addColumn(branch.compileBranchId(), 
        qualifier, (byte[])LeaftoStorageJson.invoke(leaf));
    
    branch.storeBranch(storage.getTSDB(), tree, true);
    assertEquals(3, storage.numRows());
    assertEquals(3, storage.numColumns(new byte[] { 0, 1 }));
    assertNull(tree.getCollisions());
    final Branch parsed = JSON.parseToObject(storage.getColumn(
        new byte[] { 0, 1 }, "branch".getBytes(MockBase.ASCII())), 
        Branch.class);
    parsed.setTreeId(1);
    assertEquals("ROOT", parsed.getDisplayName());
  }
  
  @Test
  public void storeBranchCollision() throws Exception {
    setupStorage();
    final Branch branch = buildTestBranch(tree);
    Leaf leaf = new Leaf("Alarms", "0101");
    byte[] qualifier = leaf.columnQualifier();
    storage.addColumn(branch.compileBranchId(), 
        qualifier, (byte[])LeaftoStorageJson.invoke(leaf));
    
    branch.storeBranch(storage.getTSDB(), tree, true);
    assertEquals(3, storage.numRows());
    assertEquals(3, storage.numColumns(new byte[] { 0, 1 }));
    assertEquals(1, tree.getCollisions().size());
    final Branch parsed = JSON.parseToObject(storage.getColumn(
        new byte[] { 0, 1 }, "branch".getBytes(MockBase.ASCII())), 
        Branch.class);
    parsed.setTreeId(1);
    assertEquals("ROOT", parsed.getDisplayName());
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
  public void BRANCH_QUALIFIER() throws Exception {
    assertArrayEquals("branch".getBytes(MockBase.ASCII()), 
        Branch.BRANCH_QUALIFIER());
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
  
  /**
   * Mocks classes for testing the storage calls
   */
  private void setupStorage() throws Exception {
    final HBaseClient client = mock(HBaseClient.class);
    final Config config = new Config(false);
    PowerMockito.whenNew(HBaseClient.class)
      .withArguments(anyString(), anyString()).thenReturn(client);
    
    storage = new MockBase(new TSDB(config), client, true, true, true, true);
    
    Branch branch = new Branch(1);
    TreeMap<Integer, String> path = new TreeMap<Integer, String>();
    path.put(0, "ROOT");
    path.put(1, "sys");
    path.put(2, "cpu");
    branch.prependParentPath(path);
    branch.setDisplayName("cpu");
    storage.addColumn(branch.compileBranchId(), 
        "branch".getBytes(MockBase.ASCII()), 
        (byte[])toStorageJson.invoke(branch));
    
    Leaf leaf = new Leaf("user", "000001000001000001");
    byte[] qualifier = leaf.columnQualifier();
    storage.addColumn(branch.compileBranchId(), 
        qualifier, (byte[])LeaftoStorageJson.invoke(leaf));
    
    leaf = new Leaf("nice", "000002000002000002");
    qualifier = leaf.columnQualifier();
    storage.addColumn(branch.compileBranchId(), 
        qualifier, (byte[])LeaftoStorageJson.invoke(leaf));
    
    // child branch
    branch = new Branch(1);
    path.put(3, "mboard");
    branch.prependParentPath(path);
    branch.setDisplayName("mboard");
    storage.addColumn(branch.compileBranchId(), 
        "branch".getBytes(MockBase.ASCII()), 
        (byte[])toStorageJson.invoke(branch));
    
    leaf = new Leaf("Asus", "000003000003000003");
    qualifier = leaf.columnQualifier();
    storage.addColumn(branch.compileBranchId(), 
        qualifier, (byte[])LeaftoStorageJson.invoke(leaf));
  }
}
