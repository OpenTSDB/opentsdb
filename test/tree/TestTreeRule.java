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

import java.util.regex.PatternSyntaxException;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.json.StorageModule;
import net.opentsdb.tree.TreeRule.TreeRuleType;
import net.opentsdb.utils.Config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public final class TestTreeRule {
  private MemoryStore tsdb_store;
  private TSDB tsdb;
  private TreeRule rule;
  private ObjectMapper jsonMapper;

  @Before
  public void before() {
    rule = new TreeRule();

    jsonMapper = new ObjectMapper();
    jsonMapper.registerModule(new StorageModule());
  }
  
  @Test
  public void copyConstructor() {
    rule = new TreeRule(1);
    rule.setCustomField("Custom");    
    rule.setDescription("Hello World!");
    rule.setDisplayFormat("Display");
    rule.setField("Field");
    rule.setLevel(1);
    rule.setNotes("Notes");
    rule.setOrder(2);
    rule.setRegexGroupIdx(4);
    rule.setSeparator("\\.");
    
    final TreeRule copy = new TreeRule(rule);
    assertEquals(1, copy.getTreeId());
    assertEquals("Custom", copy.getCustomField());
    assertEquals("Hello World!", copy.getDescription());
    assertEquals("Display", copy.getDisplayFormat());
    assertEquals("Field", copy.getField());
    assertEquals(1, copy.getLevel());
    assertEquals("Notes", copy.getNotes());
    assertEquals(2, copy.getOrder());
    assertEquals(4, copy.getRegexGroupIdx());
    assertEquals("\\.", copy.getSeparator());
  }
  
  @Test
  public void setRegex() {
    rule.setRegex("^HelloWorld$");
    assertNotNull(rule.getCompiledRegex());
    assertEquals("^HelloWorld$", rule.getCompiledRegex().pattern());
  }
  
  @Test (expected = PatternSyntaxException.class)
  public void setRegexBadPattern() {
    rule.setRegex("Invalid\\\\(pattern");
  }
  
  @Test
  public void setRegexNull() {
    rule.setRegex(null);
    assertNull(rule.getRegex());
    assertNull(rule.getCompiledRegex());
  }
  
  @Test
  public void setRegexEmpty() {
    rule.setRegex("");
    assertTrue(rule.getRegex().isEmpty());
    assertNull(rule.getCompiledRegex());
  }
  
  @Test
  public void stringToTypeMetric() {
    assertEquals(TreeRuleType.METRIC, TreeRule.stringToType("Metric"));
  }
  
  @Test
  public void stringToTypeMetricCustom() {
    assertEquals(TreeRuleType.METRIC_CUSTOM, 
        TreeRule.stringToType("Metric_Custom"));
  }
  
  @Test
  public void stringToTypeTagk() {
    assertEquals(TreeRuleType.TAGK, TreeRule.stringToType("TagK"));
  }
  
  @Test
  public void stringToTypeTagkCustom() {
    assertEquals(TreeRuleType.TAGK_CUSTOM, TreeRule.stringToType("TagK_Custom"));
  }
  
  @Test
  public void stringToTypeTagvCustom() {
    assertEquals(TreeRuleType.TAGV_CUSTOM, TreeRule.stringToType("TagV_Custom"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToTypeNull() {
    TreeRule.stringToType(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToTypeEmpty() {
    TreeRule.stringToType("");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToTypeInvalid() {
    TreeRule.stringToType("NotAType");
  }

  @Test
  public void fetchRule() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = TreeRule.fetchRule(tsdb, 1, 2, 1)
//      .joinUninterruptibly();
//    assertNotNull(rule);
//    assertEquals(1, rule.getTreeId());
//    assertEquals(2, rule.getLevel());
//    assertEquals(1, rule.getOrder());
//    assertEquals("Host owner", rule.getDescription());
  }
  
  @Test
  public void fetchRuleDoesNotExist() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = TreeRule.fetchRule(tsdb, 1, 2, 2)
//      .joinUninterruptibly();
//    assertNull(rule);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchRuleBadTreeID0() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    TreeRule.fetchRule(tsdb, 0, 2, 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchRuleBadTreeID65536() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    TreeRule.fetchRule(tsdb, 65536, 2, 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchRuleBadLevel() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    TreeRule.fetchRule(tsdb, 1, -1, 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchRuleBadOrder() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    TreeRule.fetchRule(tsdb, 1, 2, -1);
  }
  
  @Test
  public void storeRule() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = new TreeRule(1);
//    rule.setLevel(1);
//    rule.setOrder(0);
//    rule.setType(TreeRuleType.METRIC);
//    rule.setNotes("Just some notes");
//    assertTrue(rule.syncToStorage(tsdb, false).joinUninterruptibly());
//    assertEquals(3, tsdb_store.numColumns(new byte[] { 0, 1 }));
  }
  
  @Test
  public void storeRuleMege() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = new TreeRule(1);
//    rule.setLevel(2);
//    rule.setOrder(1);
//    rule.setNotes("Just some notes");
//    assertTrue(rule.syncToStorage(tsdb, false).joinUninterruptibly());
//    assertEquals(2, tsdb_store.numColumns(new byte[] { 0, 1 }));
//    final TreeRule stored = JSON.parseToObject(
//        tsdb_store.getColumn(new byte[] { 0, 1 },
//        "tree_rule:2:1".getBytes(Const.CHARSET_ASCII)), TreeRule.class);
//    assertEquals("Host owner", stored.getDescription());
//    assertEquals("Just some notes", stored.getNotes());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleBadID0() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = new TreeRule(0);
//    rule.syncToStorage(tsdb, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleBadID65536() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = new TreeRule(65536);
//    rule.syncToStorage(tsdb, false);
  }
  
  @Test (expected = IllegalStateException.class)
  public void storeRuleNoChanges() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = TreeRule.fetchRule(tsdb, 1, 2, 1)
//      .joinUninterruptibly();
//    rule.syncToStorage(tsdb, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidType() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = new TreeRule(1);
//    rule.setLevel(1);
//    rule.setOrder(0);
//    rule.setNotes("Just some notes");
//    rule.syncToStorage(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingFieldTagk() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = new TreeRule(1);
//    rule.setLevel(1);
//    rule.setOrder(0);
//    rule.setType(TreeRuleType.TAGK);
//    rule.setNotes("Just some notes");
//    rule.syncToStorage(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingFieldTagkCustom() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = new TreeRule(1);
//    rule.setLevel(1);
//    rule.setOrder(0);
//    rule.setType(TreeRuleType.TAGK_CUSTOM);
//    rule.setNotes("Just some notes");
//    rule.syncToStorage(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingFieldTagvCustom() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = new TreeRule(1);
//    rule.setLevel(1);
//    rule.setOrder(0);
//    rule.setType(TreeRuleType.TAGV_CUSTOM);
//    rule.setNotes("Just some notes");
//    rule.syncToStorage(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingFieldMetricCustom() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = new TreeRule(1);
//    rule.setLevel(1);
//    rule.setOrder(0);
//    rule.setType(TreeRuleType.METRIC_CUSTOM);
//    rule.setNotes("Just some notes");
//    rule.syncToStorage(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingCustomFieldTagkCustom() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = new TreeRule(1);
//    rule.setLevel(1);
//    rule.setOrder(0);
//    rule.setType(TreeRuleType.TAGK_CUSTOM);
//    rule.setNotes("Just some notes");
//    rule.setField("foo");
//    rule.syncToStorage(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingCustomFieldTagvCustom() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = new TreeRule(1);
//    rule.setLevel(1);
//    rule.setOrder(0);
//    rule.setType(TreeRuleType.TAGV_CUSTOM);
//    rule.setNotes("Just some notes");
//    rule.setField("foo");
//    rule.syncToStorage(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingCustomFieldMetricCustom() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = new TreeRule(1);
//    rule.setLevel(1);
//    rule.setOrder(0);
//    rule.setType(TreeRuleType.METRIC_CUSTOM);
//    rule.setNotes("Just some notes");
//    rule.setField("foo");
//    rule.syncToStorage(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidRegexIdx() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    final TreeRule rule = new TreeRule(1);
//    rule.setLevel(1);
//    rule.setOrder(0);
//    rule.setType(TreeRuleType.TAGK);
//    rule.setRegex("^.*$");
//    rule.setRegexGroupIdx(-1);
//    rule.syncToStorage(tsdb, false).joinUninterruptibly();
  }
  
  @Test
  public void deleteRule() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    assertNotNull(tsdb.deleteTreeRule(1, 2, 1));
//    assertEquals(1, tsdb_store.numColumns(new byte[] { 0, 1 }));
  }
  
  @Test
  public void deleteAllRules() throws Exception {
    fail(); //TODO move this test to TSDB also add test for method in HBaseStore
//    setupStorage();
//    TreeRule.deleteAllRules(tsdb, 1);
//    assertEquals(1, tsdb_store.numColumns(new byte[] { 0, 1 }));
  }

  @Test
  public void RULE_PREFIX() throws Exception {
    assertEquals("tree_rule:",
        new String(Const.TREE_RULE_PREFIX, Const.CHARSET_ASCII));
  }
  
  @Test
  public void getQualifier() throws Exception {
    assertEquals("tree_rule:1:2",
        new String(TreeRule.getQualifier(1, 2), Const.CHARSET_ASCII));
  }
  
  /**
   * Mocks classes for testing the storage calls
   */
  private void setupStorage() throws Exception {
    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, new Config(false));

    final TreeRule stored_rule = new TreeRule(1);
    stored_rule.setLevel(2);
    stored_rule.setOrder(1);
    stored_rule.setType(TreeRuleType.METRIC_CUSTOM);
    stored_rule.setField("host");
    stored_rule.setCustomField("owner");
    stored_rule.setDescription("Host owner");
    stored_rule.setNotes("Owner of the host machine");
    
    // pretend there's a tree definition in the storage row
    tsdb_store.addColumn(new byte[] { 0, 1 }, "tree".getBytes(Const.CHARSET_ASCII),
        new byte[] { 1 });
    
    // add a rule to the row
    tsdb_store.addColumn(new byte[]{0, 1},
      "tree_rule:2:1".getBytes(Const.CHARSET_ASCII),
      jsonMapper.writeValueAsBytes(stored_rule));
  }
}
