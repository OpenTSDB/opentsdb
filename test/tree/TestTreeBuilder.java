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
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.tree.TreeRule.TreeRuleType;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({KeyValue.class, Tree.class})
public final class TestTreeBuilder {
  private MemoryStore tsdb_store;
  private TSDB tsdb;

  private Tree tree = TestTree.buildTestTree();
  private TreeBuilder treebuilder;
  // for UTs we'll use 1 byte tag IDs
  private String tsuid = "0102030405";
  private TSMeta meta = new TSMeta(tsuid);
  private UIDMeta metric = new UIDMeta(UniqueIdType.METRIC, new byte[] { 1 }, 
      "sys.cpu.0");
  private UIDMeta tagk1 = new UIDMeta(UniqueIdType.TAGK, new byte[] { 2 }, 
      "host");
  private UIDMeta tagv1 = new UIDMeta(UniqueIdType.TAGV, new byte[] { 3 }, 
      "web-01.lga.mysite.com");
  private UIDMeta tagk2 = new UIDMeta(UniqueIdType.TAGK, new byte[] { 4 }, 
      "type");
  private UIDMeta tagv2 = new UIDMeta(UniqueIdType.TAGV, new byte[] { 5 }, 
      "user");
  
  final static private Method toStorageJson;
  static {
    try {
      toStorageJson = Branch.class.getDeclaredMethod("toStorageJson");
      toStorageJson.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }

  @Before
  public void before() throws Exception {
    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, new Config(false));

    treebuilder = new TreeBuilder(tsdb, tree);
    PowerMockito.spy(Tree.class);
    PowerMockito.doReturn(Deferred.fromResult(tree)).when(Tree.class,
      "fetchTree", (TSDB) any(), anyInt());
    
    // set private fields via reflection so the UTs can change things at will
    Field tag_metric = TSMeta.class.getDeclaredField("metric");
    tag_metric.setAccessible(true);
    tag_metric.set(meta, metric);
    tag_metric.setAccessible(false);
    
    ArrayList<UIDMeta> tags = new ArrayList<UIDMeta>(4);
    tags.add(tagk1);
    tags.add(tagv1);
    tags.add(tagk2);
    tags.add(tagv2);
    Field tags_field = TSMeta.class.getDeclaredField("tags");
    tags_field.setAccessible(true);
    tags_field.set(meta, tags);
    tags_field.setAccessible(false);

    // store root
    final TreeMap<Integer, String> root_path = new TreeMap<Integer, String>();
    final Branch root = new Branch(tree.getTreeId());
    root.setDisplayName("ROOT");
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    tsdb_store.addColumn(root.compileBranchId(),
      "branch".getBytes(Const.CHARSET_ASCII),
      (byte[]) toStorageJson.invoke(root));
  }
  
  @Test
  public void processTimeseriesMetaDefaults() throws Exception {
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(Branch.stringToId(
        "00010001A2460001CB54247F72020001BECD000181A800000030")));
    final Branch branch = JSON.parseToObject(
        tsdb_store.getColumn(Branch.stringToId(
        "00010001A2460001CB54247F72020001BECD000181A800000030"), 
        "branch".getBytes(Const.CHARSET_ASCII)), Branch.class);
    assertNotNull(branch);
    assertEquals("0", branch.getDisplayName());
    final Leaf leaf = JSON.parseToObject(tsdb_store.getColumn(Branch.stringToId(
        "00010001A2460001CB54247F72020001BECD000181A800000030"), 
        new Leaf("user", "").columnQualifier()), Leaf.class);
    assertNotNull(leaf);
    assertEquals("user", leaf.getDisplayName());
  }
  
  @Test
  public void processTimeseriesMetaNewRoot() throws Exception {
    tsdb_store.flushStorage();
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    assertEquals(1, tsdb_store.numColumns(new byte[] { 0, 1 }));
  }

  @Test
  public void processTimeseriesMetaMiddleNonMatchedRules() throws Exception {
    // tests to make sure we collapse branches if rules at the front or middle
    // of the rule set are not matched
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setField("host");
    rule.setCustomField("dept");
    rule.setDescription("Department");
    rule.setLevel(0);
    rule.setOrder(1);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(1);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setField("host");
    rule.setCustomField("dept");
    rule.setDescription("Department");
    rule.setLevel(1);
    rule.setOrder(1);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(5, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId("0001247F72020001BECD000181A800000030")));
  }
  
  @Test
  public void processTimeseriesMetaEndNonMatchedRules() throws Exception {
    // tests to make sure we collapse branches if rules at the end
    // of the rule set are not matched
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(5);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setField("host");
    rule.setCustomField("dept");
    rule.setDescription("Department");
    rule.setLevel(5);
    rule.setOrder(1);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(6);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setField("host");
    rule.setCustomField("dept");
    rule.setDescription("Department");
    rule.setLevel(6);
    rule.setOrder(1);
    tree.addRule(rule);
    treebuilder.setTree(tree);
    
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId(
            "00010001A2460001CB54247F72020001BECD000181A800000030")));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void processTimeseriesMetaNullMeta() throws Exception {
    treebuilder.processTimeseriesMeta(null, false).joinUninterruptibly();
  }

  @Test (expected = IllegalStateException.class)
  public void processTimeseriesMetaNullMetaMetric() throws Exception {
    Field tag_metric = TSMeta.class.getDeclaredField("metric");
    tag_metric.setAccessible(true);
    tag_metric.set(meta, null);
    tag_metric.setAccessible(false);
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalStateException.class)
  public void processTimeseriesMetaNullMetaTags() throws Exception {
    Field tags = TSMeta.class.getDeclaredField("tags");
    tags.setAccessible(true);
    tags.set(meta, null);
    tags.setAccessible(false);
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
  }
  
  @Test
  public void processTimeseriesMetaNullMetaOddNumTags() throws Exception {
    ArrayList<UIDMeta> tags = new ArrayList<UIDMeta>(4);
    tags.add(tagk1);
    //tags.add(tagv1); <-- whoops. This will process through but missing host
    tags.add(tagk2);
    tags.add(tagv2);
    Field tags_field = TSMeta.class.getDeclaredField("tags");
    tags_field.setAccessible(true);
    tags_field.set(meta, tags);
    tags_field.setAccessible(false);
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(5, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId(
            "00010036EBCB0001BECD000181A800000030")));
  }
  
  @Test
  public void processTimeseriesMetaTesting() throws Exception {
    treebuilder.processTimeseriesMeta(meta, true).joinUninterruptibly();
    assertEquals(1, tsdb_store.numRows());
  }

  @Test
  public void processTimeseriesMetaStrict() throws Exception {
    tree.setStrictMatch(true);
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId(
            "00010001A2460001CB54247F72020001BECD000181A800000030")));
  }
  
  @Test
  public void processTimeseriesMetaStrictNoMatch() throws Exception {
    Field name = UIDMeta.class.getDeclaredField("name");
    name.setAccessible(true);
    name.set(tagv1, "foobar");
    name.setAccessible(false);
    tree.setStrictMatch(true);
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(1, tsdb_store.numRows());
  }

  @Test
  public void processTimeseriesMetaNoSplit() throws Exception {
    tree.getRules().get(3).get(0).setSeparator("");
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(5, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId("00010001A2460001CB54247F7202CBBF5B09")));
  }
  
  @Test
  public void processTimeseriesMetBadSeparator() throws Exception {
    tree.getRules().get(3).get(0).setSeparator(".");
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(4, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId("00010001A2460001CB54247F7202")));
  }
  
  @Test
  public void processTimeseriesMetaInvalidRegexIdx() throws Exception {
    tree.getRules().get(1).get(1).setRegexGroupIdx(42);
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(6, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId("00010001A246247F72020001BECD000181A800000030")));
  }
  
  @Test
  public void processTimeseriesMetaMetricCustom() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "John Doe");
    custom.put("dc", "lga");
    metric.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.METRIC_CUSTOM);
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId(
          "0001AE805CA50001CB54247F72020001BECD000181A800000030")));
  }
  
  @Test (expected = IllegalStateException.class)
  public void processTimeseriesMetaMetricCustomNullValue() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", null);
    custom.put("dc", "lga");
    metric.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.METRIC_CUSTOM);
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();    
  }
  
  @Test
  public void processTimeseriesMetaMetricCustomEmptyValue() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "");
    custom.put("dc", "lga");
    metric.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.METRIC_CUSTOM);
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId(
          "00010001A2460001CB54247F72020001BECD000181A800000030")));
  }
  
  @Test
  public void processTimeseriesMetaTagkCustom() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "John Doe");
    custom.put("dc", "lga");
    tagk1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId(
          "0001AE805CA50001CB54247F72020001BECD000181A800000030")));
  }
  
  @Test (expected = IllegalStateException.class)
  public void processTimeseriesMetaTagkCustomNull() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", null);
    custom.put("dc", "lga");
    tagk1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();    
  }
  
  @Test
  public void processTimeseriesMetaTagkCustomEmptyValue() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "");
    custom.put("dc", "lga");
    tagk1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId(
          "00010001A2460001CB54247F72020001BECD000181A800000030")));
  }
  
  @Test
  public void processTimeseriesMetaTagkCustomNoField() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "John Doe");
    custom.put("dc", "lga");
    tagk1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    //rule.setField("host"); <-- must be set to match
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId(
          "00010001A2460001CB54247F72020001BECD000181A800000030")));
  }
  
  @Test
  public void processTimeseriesMetaTagvCustom() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "John Doe");
    custom.put("dc", "lga");
    tagv1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setField("web-01.lga.mysite.com");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId(
          "0001AE805CA50001CB54247F72020001BECD000181A800000030")));
  }
  
  @Test (expected = IllegalStateException.class)
  public void processTimeseriesMetaTagvCustomNullValue() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", null);
    custom.put("dc", "lga");
    tagv1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setField("web-01.lga.mysite.com");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();    
  }
  
  @Test
  public void processTimeseriesMetaTagvCustomEmptyValue() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "");
    custom.put("dc", "lga");
    tagv1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId(
          "00010001A2460001CB54247F72020001BECD000181A800000030")));
  }
  
  @Test
  public void processTimeseriesMetaTagvCustomNoField() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "John Doe");
    custom.put("dc", "lga");
    tagv1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    //rule.setField("host"); <-- must be set to match
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    assertEquals(2, tsdb_store.numColumns(
        Branch.stringToId(
          "00010001A2460001CB54247F72020001BECD000181A800000030")));
  }
  
  @Test
  public void processTimeseriesMetaFormatOvalue() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat("OV: {ovalue}");
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    final Branch branch = JSON.parseToObject(
        tsdb_store.getColumn(Branch.stringToId("00010001A24637E140D5"),
            "branch".getBytes(Const.CHARSET_ASCII)), Branch.class);
    assertEquals("OV: web-01.lga.mysite.com", branch.getDisplayName());
  }
  
  @Test
  public void processTimeseriesMetaFormatValue() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat("V: {value}");
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    final Branch branch = JSON.parseToObject(
        tsdb_store.getColumn(Branch.stringToId("00010001A24696026FD8"),
            "branch".getBytes(Const.CHARSET_ASCII)), Branch.class);
    assertEquals("V: web", branch.getDisplayName());
  }
  
  @Test
  public void processTimeseriesMetaFormatTSUID() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat("TSUID: {tsuid}");
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    final Branch branch = JSON.parseToObject(
        tsdb_store.getColumn(Branch.stringToId("00010001A246E0A07086"),
            "branch".getBytes(Const.CHARSET_ASCII)), Branch.class);
    assertEquals("TSUID: " + tsuid, branch.getDisplayName());    
  }
  
  @Test
  public void processTimeseriesMetaFormatTagName() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat("TAGNAME: {tag_name}");
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    final Branch branch = JSON.parseToObject(
        tsdb_store.getColumn(Branch.stringToId("00010001A2467BFCCB13"),
            "branch".getBytes(Const.CHARSET_ASCII)), Branch.class);
    assertEquals("TAGNAME: host", branch.getDisplayName());    
  }
  
  @Test
  public void processTimeseriesMetaFormatMulti() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat(
        "{ovalue}:{value}:{tag_name}:{tsuid}");
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(7, tsdb_store.numRows());
    final Branch branch = JSON.parseToObject(
        tsdb_store.getColumn(Branch.stringToId("00010001A246E4592083"),
            "branch".getBytes(Const.CHARSET_ASCII)), Branch.class);
    assertEquals("web-01.lga.mysite.com:web:host:0102030405", 
        branch.getDisplayName());    
  }
  
  @Test
  public void processTimeseriesMetaFormatBadType() throws Exception {
    tree.getRules().get(3).get(0).setDisplayFormat("Wrong: {tag_name}");
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(5, tsdb_store.numRows());
    final Branch branch = JSON.parseToObject(
        tsdb_store.getColumn(Branch.stringToId(
          "00010001A2460001CB54247F7202C3165573"), 
              "branch".getBytes(Const.CHARSET_ASCII)), Branch.class);
    assertEquals("Wrong: ", branch.getDisplayName());    
  }
  
  @Test
  public void processTimeseriesMetaFormatOverride() throws Exception {
    tree.getRules().get(3).get(0).setDisplayFormat("OVERRIDE");
    treebuilder.processTimeseriesMeta(meta, false).joinUninterruptibly();
    assertEquals(5, tsdb_store.numRows());
    final Branch branch = JSON.parseToObject(
        tsdb_store.getColumn(Branch.stringToId(
          "00010001A2460001CB54247F72024E3D0BCC"), 
            "branch".getBytes(Const.CHARSET_ASCII)), Branch.class);
    assertEquals("OVERRIDE", branch.getDisplayName());    
  }
}
