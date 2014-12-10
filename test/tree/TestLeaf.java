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

import static net.opentsdb.uid.UniqueIdType.*;
import static org.junit.Assert.*;

import net.opentsdb.core.Const;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import org.hbase.async.KeyValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({KeyValue.class})
public final class TestLeaf {
  private TSDB tsdb;
  private MemoryStore tsdb_store;
  private Branch branch;
  
  @Before
  public void before() throws Exception {
    final Config config = new Config(false);
    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, config);
    branch = new Branch(1);

    tsdb_store.allocateUID("sys.cpu.0", new byte[]{0, 0, 1}, METRIC);
    tsdb_store.allocateUID("host", new byte[]{0, 0, 1}, TAGK);
    tsdb_store.allocateUID("web01", new byte[]{0, 0, 1}, TAGV);

    tsdb_store.addColumn(new byte[]{0, 1}, Tree.TREE_FAMILY(),
      new Leaf("0", "000001000001000001").columnQualifier(),
      ("{\"displayName\":\"0\",\"tsuid\":\"000001000001000001\"}")
        .getBytes(Const.CHARSET_ASCII));
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
  public void LEAF_PREFIX() throws Exception {
    assertEquals("leaf:", new String(Leaf.LEAF_PREFIX(), Const.CHARSET_ASCII));
  }
}
