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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.DeferredGroupException;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  GetRequest.class, PutRequest.class, DeleteRequest.class, KeyValue.class, 
  Scanner.class })
public final class TestLeaf {
  private static byte[] NAME_FAMILY = "name".getBytes(MockBase.ASCII());
  private TSDB tsdb;
  private HBaseClient client = mock(HBaseClient.class);
  private MockBase storage;
  
  @Before
  public void before() throws Exception {
    final Config config = new Config(false);
    tsdb = new TSDB(client, config);
    
    storage = new MockBase(tsdb, client, true, true, true, true);
    
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "metrics".getBytes(MockBase.ASCII()),
        "sys.cpu.0".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "tagk".getBytes(MockBase.ASCII()),
        "host".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "tagv".getBytes(MockBase.ASCII()),
        "web01".getBytes(MockBase.ASCII()));
    
    storage.addColumn(new byte[] { 0, 1 }, Tree.TREE_FAMILY(),
        new Leaf("0", "000001000001000001").columnQualifier(), 
        ("{\"displayName\":\"0\",\"tsuid\":\"000001000001000001\"}")
        .getBytes(MockBase.ASCII()));
  }
  
  @Test
  public void testEquals() {
    final Leaf leaf = new Leaf();
    leaf.setTsuid("ABCD");
    final Leaf leaf2 = new Leaf();
    leaf2.setTsuid("ABCD");
    assertTrue(leaf.equals(leaf2));
  }
  
  @Test
  public void equalsSameAddress() {
    final Leaf leaf = new Leaf();
    final Leaf leaf2 = leaf;
    assertTrue(leaf.equals(leaf2));
  }
  
  @Test
  public void equalsNull() {
    final Leaf leaf = new Leaf();
    assertFalse(leaf.equals(null));
  }
  
  @Test
  public void equalsWrongClass() {
    final Leaf leaf = new Leaf();
    assertFalse(leaf.equals(new Object()));
  }
  
  @Test
  public void compareTo() {
    final Leaf leaf = new Leaf();
    leaf.setDisplayName("Leaf");
    final Leaf leaf2 = new Leaf();
    leaf2.setDisplayName("Leaf");
    assertEquals(0, leaf.compareTo(leaf2));
  }
  
  @Test
  public void compareToLess() {
    final Leaf leaf = new Leaf();
    leaf.setDisplayName("Leaf");
    final Leaf leaf2 = new Leaf();
    leaf2.setDisplayName("Ardvark");
    assertTrue(leaf.compareTo(leaf2) > 0);
  }
  
  @Test
  public void compareToGreater() {
    final Leaf leaf = new Leaf();
    leaf.setDisplayName("Leaf");
    final Leaf leaf2 = new Leaf();
    leaf2.setDisplayName("Zelda");
    assertTrue(leaf.compareTo(leaf2) < 0);
  }

  @Test
  public void columnQualifier() throws Exception {
    final Leaf leaf = new Leaf("Leaf", "000001000001000001");
    assertEquals("6C6561663A0024137E", 
        Branch.idToString(leaf.columnQualifier()));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void columnQualifierNoDisplayName() throws Exception {
    final Leaf leaf = new Leaf("", "000001000001000001");
    leaf.columnQualifier();
  }
  
  @Test
  public void storeLeaf() throws Exception {
    final Leaf leaf = new Leaf("Leaf", "000002000002000002");
    final Tree tree = TestTree.buildTestTree();
    assertTrue(leaf.storeLeaf(tsdb, new byte[] { 0, 1 }, tree)
        .joinUninterruptibly());
    assertEquals(2, storage.numColumns(new byte[] { 0, 1 }));
  }
  
  @Test
  public void storeLeafExistingSame() throws Exception {
    final Leaf leaf = new Leaf("0", "000001000001000001");
    final Tree tree = TestTree.buildTestTree();
    assertTrue(leaf.storeLeaf(tsdb, new byte[] { 0, 1 }, tree)
        .joinUninterruptibly());
    assertEquals(1, storage.numColumns(new byte[] { 0, 1 }));
  }
  
  @Test
  public void storeLeafCollision() throws Exception {
    final Leaf leaf = new Leaf("0", "000002000001000001");
    final Tree tree = TestTree.buildTestTree();
    assertFalse(leaf.storeLeaf(tsdb, new byte[] { 0, 1 }, tree)
        .joinUninterruptibly());
    assertEquals(1, storage.numColumns(new byte[] { 0, 1 }));
    assertEquals(1, tree.getCollisions().size());
  }
  
  @Test
  public void parseFromStorage() throws Exception {
    final KeyValue column = mock(KeyValue.class);
    when(column.qualifier()).thenReturn(
        new Leaf("0", "000001000001000001").columnQualifier());
    when(column.value()).thenReturn(
        ("{\"displayName\":\"0\",\"tsuid\":\"000001000001000001\"}")
        .getBytes(MockBase.ASCII()));
    final Leaf leaf = Leaf.parseFromStorage(tsdb, column, true).joinUninterruptibly();
    assertNotNull(leaf);
    assertEquals("0", leaf.getDisplayName());
    assertEquals("000001000001000001", leaf.getTsuid());
    assertEquals("sys.cpu.0", leaf.getMetric());
    assertEquals(1, leaf.getTags().size());
    assertEquals("web01", leaf.getTags().get("host"));
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void parseFromStorageNSUMetric() throws Throwable {
    final KeyValue column = mock(KeyValue.class);
    when(column.qualifier()).thenReturn(
        new Leaf("0", "000002000001000001").columnQualifier());
    when(column.value()).thenReturn(
        ("{\"displayName\":\"0\",\"tsuid\":\"000002000001000001\"}")
        .getBytes(MockBase.ASCII()));
    try {
      Leaf.parseFromStorage(tsdb, column, true).joinUninterruptibly();
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void parseFromStorageNSUTagk() throws Throwable {
    final KeyValue column = mock(KeyValue.class);
    when(column.qualifier()).thenReturn(
        new Leaf("0", "000001000002000001").columnQualifier());
    when(column.value()).thenReturn(
        ("{\"displayName\":\"0\",\"tsuid\":\"000001000002000001\"}")
        .getBytes(MockBase.ASCII()));
    try {
      Leaf.parseFromStorage(tsdb, column, true).joinUninterruptibly();
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void parseFromStorageNSUTagV() throws Throwable {
    final KeyValue column = mock(KeyValue.class);
    when(column.qualifier()).thenReturn(
        new Leaf("0", "000001000001000002").columnQualifier());
    when(column.value()).thenReturn(
        ("{\"displayName\":\"0\",\"tsuid\":\"000001000001000002\"}")
        .getBytes(MockBase.ASCII()));
    try {
      Leaf.parseFromStorage(tsdb, column, true).joinUninterruptibly();
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }

  @Test
  public void LEAF_PREFIX() throws Exception {
    assertEquals("leaf:", new String(Leaf.LEAF_PREFIX(), MockBase.ASCII()));
  }
}
