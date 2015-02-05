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
package net.opentsdb.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.json.StorageModule;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.TestTree;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import org.junit.Before;
import org.junit.Test;

import java.util.TreeMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public final class TestTSDB {
  private Config config;
  private TSDB tsdb;
  private MemoryStore tsdb_store;
  private ObjectMapper jsonMapper;

  @Before
  public void before() throws Exception {
    config = new Config(false);
    config.setFixDuplicates(true); // TODO(jat): test both ways
    tsdb_store = new MemoryStore();
    tsdb = TsdbBuilder.createFromConfig(config)
            .withStore(tsdb_store)
            .build();
  }
  
  @Test
  public void getClient() {
    assertNotNull(tsdb.getTsdbStore());
  }
  
  @Test
  public void getConfig() {
    assertNotNull(tsdb.getConfig());
  }
  
  @Test
  public void uidTable() {
    assertNotNull(tsdb.uidTable());
    assertArrayEquals("tsdb-uid".getBytes(), tsdb.uidTable());
  }

  @Test
  public void getHBaseStore() {
    fail();
  }

  /**
   * Mocks classes for testing the storage calls
   */
  private void setupTreeStorage() throws Exception {
    tsdb_store = new MemoryStore();
    tsdb = TsdbBuilder.createFromConfig(new Config(false))
            .withStore(tsdb_store)
            .build();

    jsonMapper = new ObjectMapper();
    jsonMapper.registerModule(new StorageModule());

    byte[] key = new byte[] { 0, 1 };
    // set pre-test values
    tsdb_store.addColumn(key, "tree".getBytes(Const.CHARSET_ASCII),
        jsonMapper.writeValueAsBytes(TestTree.buildTestTree()));

    TreeRule rule = new TreeRule(1);
    rule.setField("host");
    rule.setType(TreeRule.TreeRuleType.TAGK);
    tsdb_store.addColumn(key, "tree_rule:0:0".getBytes(Const.CHARSET_ASCII),
            jsonMapper.writeValueAsBytes(rule));

    rule = new TreeRule(1);
    rule.setField("");
    rule.setLevel(1);
    rule.setType(TreeRule.TreeRuleType.METRIC);
    tsdb_store.addColumn(key, "tree_rule:1:0".getBytes(Const.CHARSET_ASCII),
            jsonMapper.writeValueAsBytes(rule));

    Branch root = new Branch(1);
    root.setDisplayName("ROOT");
    TreeMap<Integer, String> root_path = new TreeMap<Integer, String>();
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    tsdb_store.addColumn(key, "branch".getBytes(Const.CHARSET_ASCII),
            jsonMapper.writeValueAsBytes(root));

    // tree 2
    key = new byte[] { 0, 2 };

    Tree tree2 = new Tree();
    tree2.setTreeId(2);
    tree2.setName("2nd Tree");
    tree2.setDescription("Other Tree");
    tsdb_store.addColumn(key, "tree".getBytes(Const.CHARSET_ASCII),
        jsonMapper.writeValueAsBytes(tree2));

    rule = new TreeRule(2);
    rule.setField("host");
    rule.setType(TreeRule.TreeRuleType.TAGK);
    tsdb_store.addColumn(key, "tree_rule:0:0".getBytes(Const.CHARSET_ASCII),
            jsonMapper.writeValueAsBytes(rule));

    rule = new TreeRule(2);
    rule.setField("");
    rule.setLevel(1);
    rule.setType(TreeRule.TreeRuleType.METRIC);
    tsdb_store.addColumn(key, "tree_rule:1:0".getBytes(Const.CHARSET_ASCII),
            jsonMapper.writeValueAsBytes(rule));

    root = new Branch(2);
    root.setDisplayName("ROOT");
    root_path = new TreeMap<Integer, String>();
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    tsdb_store.addColumn(key, "branch".getBytes(Const.CHARSET_ASCII),
            jsonMapper.writeValueAsBytes(root));

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
    tsdb_store.addColumn(key, qualifier, "AAAAAA".getBytes(Const.CHARSET_ASCII));

    tsuid = "020202";
    qualifier = new byte[Tree.COLLISION_PREFIX().length +
            (tsuid.length() / 2)];
    System.arraycopy(Tree.COLLISION_PREFIX(), 0, qualifier, 0,
            Tree.COLLISION_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.COLLISION_PREFIX().length,
            tsuid_bytes.length);
    tsdb_store.addColumn(key, qualifier, "BBBBBB".getBytes(Const.CHARSET_ASCII));

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
    tsdb_store.addColumn(key, qualifier, "Failed rule 0:0"
            .getBytes(Const.CHARSET_ASCII));

    tsuid = "020202";
    qualifier = new byte[Tree.NOT_MATCHED_PREFIX().length +
            (tsuid.length() / 2)];
    System.arraycopy(Tree.NOT_MATCHED_PREFIX(), 0, qualifier, 0,
            Tree.NOT_MATCHED_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.NOT_MATCHED_PREFIX().length,
            tsuid_bytes.length);
    tsdb_store.addColumn(key, qualifier, "Failed rule 1:1"
            .getBytes(Const.CHARSET_ASCII));
  }
}
