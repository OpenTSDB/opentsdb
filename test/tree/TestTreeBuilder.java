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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.TreeMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TreeClient;
import net.opentsdb.core.TsdbBuilder;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.json.StorageModule;
import net.opentsdb.tree.TreeRule.TreeRuleType;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.opentsdb.utils.Pair;
import org.junit.Before;
import org.junit.Test;

public final class TestTreeBuilder {
  private MemoryStore tsdb_store;
  private TSDB tsdb;

  private Tree tree = TestTree.buildTestTree();
  private TreeBuilder treebuilder;
  // for UTs we'll use 1 byte tag IDs
  private String tsuid = "0102030405";
  private TSMeta meta = new TSMeta(tsuid);
  private UIDMeta metric = new UIDMeta(UniqueIdType.METRIC, new byte[] { 0, 0, 1
  },
      "sys.cpu.0");
  private UIDMeta tagk1 = new UIDMeta(UniqueIdType.TAGK, new byte[] { 0, 0, 2 },
      "host");
  private UIDMeta tagv1 = new UIDMeta(UniqueIdType.TAGV, new byte[] { 0, 0, 3 },
      "web-01.lga.mysite.com");
  private UIDMeta tagk2 = new UIDMeta(UniqueIdType.TAGK, new byte[] { 0, 0, 4 },
      "type");
  private UIDMeta tagv2 = new UIDMeta(UniqueIdType.TAGV, new byte[] { 0, 0, 5 },
      "user");

  @Before
  public void before() throws Exception {
    tsdb_store = new MemoryStore();
    tsdb = TsdbBuilder.createFromConfig(new Config(false))
            .withStore(tsdb_store)
            .build();

    treebuilder = new TreeBuilder(tsdb.getTreeClient(), tree);

    meta.setMetric(metric);
    
    ArrayList<UIDMeta> tags = new ArrayList<UIDMeta>(4);
    tags.add(tagk1);
    tags.add(tagv1);
    tags.add(tagk2);
    tags.add(tagv2);
    Field tags_field = TSMeta.class.getDeclaredField("tags");
    tags_field.setAccessible(true);
    tags_field.set(meta, tags);
    tags_field.setAccessible(false);

    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.registerModule(new StorageModule());

    // store root
    final TreeMap<Integer, String> root_path = new TreeMap<Integer, String>();
    final Branch root = new Branch(tree.getTreeId());
    root.setDisplayName("ROOT");
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);

    tsdb_store.storeBranch(tree, root, false);
  }
  
  @Test
  public void processTimeseriesMetaDefaults() throws Exception {
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("00010001A24600" +
                    "01CB54247F72020001BECD000181A800000030"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
  }
  
  @Test
  public void processTimeseriesMetaNewRoot() throws Exception {
    tsdb_store.purgeBranches();
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("00010001A246" +
                    "0001CB54247F72020001BECD000181A800000030"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
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
    
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001247F72020" +
                    "001BECD000181A800000030"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));


    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(4, response.getBranches().size());
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

    tsdb_store.storeTree(tree, true);
    
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("00010001A2460" +
                    "001CB54247F72020001BECD000181A800000030"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));


    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void processTimeseriesMetaNullMeta() throws Exception {
    treebuilder.processTimeseriesMeta(null).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = IllegalStateException.class)
  public void processTimeseriesMetaNullMetaMetric() throws Exception {
    Field tag_metric = TSMeta.class.getDeclaredField("metric");
    tag_metric.setAccessible(true);
    tag_metric.set(meta, null);
    tag_metric.setAccessible(false);
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);
  }
  
  @Test (expected = IllegalStateException.class)
  public void processTimeseriesMetaNullMetaTags() throws Exception {
    Field tags = TSMeta.class.getDeclaredField("tags");
    tags.setAccessible(true);
    tags.set(meta, null);
    tags.setAccessible(false);
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);
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
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("00010036EBCB0" +
                    "001BECD000181A800000030"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(4, response.getBranches().size());
  }

  @Test
  public void processTimeseriesMetaStrict() throws Exception {
    tree.setStrictMatch(true);
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("00010001A246" +
                    "0001CB54247F72020001BECD000181A800000030"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
  }
  
  @Test
  public void processTimeseriesMetaStrictNoMatch() throws Exception {
    Field name = UIDMeta.class.getDeclaredField("name");
    name.setAccessible(true);
    name.set(tagv1, "foobar");
    name.setAccessible(false);
    tree.setStrictMatch(true);
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(4, response.getBranches().size());
  }

  @Test
  public void processTimeseriesMetaNoSplit() throws Exception {
    tree.getRules().get(3).get(0).setSeparator("");
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "sys.cpu.0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(4, response.getBranches().size());
  }
  
  @Test
  public void processTimeseriesMetBadSeparator() throws Exception {
    tree.getRules().get(3).get(0).setSeparator(".");
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "web-01.lga.mysite.com"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(3, response.getBranches().size());
  }
  
  @Test
  public void processTimeseriesMetaInvalidRegexIdx() throws Exception {
    tree.getRules().get(1).get(1).setRegexGroupIdx(42);
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(5, response.getBranches().size());
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
    
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
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
    
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);
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
    
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
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
    
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
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
    
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);
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
    
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
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
    
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
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
    
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
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
    
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
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
    
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
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
    
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
  }
  
  @Test
  public void processTimeseriesMetaFormatOvalue() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat("OV: {ovalue}");
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
    boolean found = false;
    for (Branch branch : response.getBranches()) {
      if (branch.getBranchId().equals("00010001A24637E140D5")) {
        assertEquals("OV: web-01.lga.mysite.com", branch.getDisplayName());
        found = true;
      }
    }
    assertTrue(found);
  }
  
  @Test
  public void processTimeseriesMetaFormatValue() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat("V: {value}");
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
    boolean found = false;
    for (Branch branch : response.getBranches()) {
      if (branch.getBranchId().equals("00010001A24696026FD8")) {
        assertEquals("V: web", branch.getDisplayName());
        found = true;
      }
    }
    assertTrue(found);
  }
  
  @Test
  public void processTimeseriesMetaFormatTSUID() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat("TSUID: {tsuid}");
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
    boolean found = false;
    for (Branch branch : response.getBranches()) {
      if (branch.getBranchId().equals("00010001A246E0A07086")) {
        assertEquals("TSUID: " + tsuid, branch.getDisplayName());
        found = true;
      }
    }
    assertTrue(found);
  }
  
  @Test
  public void processTimeseriesMetaFormatTagName() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat("TAGNAME: {tag_name}");
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
    boolean found = false;
    for (Branch branch : response.getBranches()) {
      if (branch.getBranchId().equals("00010001A2467BFCCB13")) {
        assertEquals("TAGNAME: host", branch.getDisplayName());
        found = true;
      }
    }
    assertTrue(found);
  }
  
  @Test
  public void processTimeseriesMetaFormatMulti() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat(
        "{ovalue}:{value}:{tag_name}:{tsuid}");
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "0"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(6, response.getBranches().size());
    boolean found = false;
    for (Branch branch : response.getBranches()) {
      if (branch.getBranchId().equals("00010001A246E4592083")) {
        assertEquals("web-01.lga.mysite.com:web:host:0102030405",
                branch.getDisplayName());
        found = true;
      }
    }
    assertTrue(found);
  }
  
  @Test
  public void processTimeseriesMetaFormatBadType() throws Exception {
    tree.getRules().get(3).get(0).setDisplayFormat("Wrong: {tag_name}");
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "Wrong: "));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(4, response.getBranches().size());
    boolean found = false;
    for (Branch branch : response.getBranches()) {
      if (branch.getBranchId().equals("00010001A2460001CB54247F7202C3165573")) {
        assertEquals("Wrong: ",
                branch.getDisplayName());
        found = true;
      }
    }
    assertTrue(found);
  }
  
  @Test
  public void processTimeseriesMetaFormatOverride() throws Exception {
    tree.getRules().get(3).get(0).setDisplayFormat("OVERRIDE");
    treebuilder.processTimeseriesMeta(meta).joinUninterruptibly(
            MockBase.DEFAULT_TIMEOUT);

    Branch response = tsdb_store.fetchBranch(Branch.stringToId("0001"), false,
            tsdb).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    Collection<Leaf> leafs = tsdb_store.getLeaf(new Pair<Integer, String>(1,
            "OVERRIDE"));

    assertEquals(1, leafs.size());
    assertNotNull(response);
    assertEquals(4, response.getBranches().size());
    boolean found = false;
    for (Branch branch : response.getBranches()) {
      if (branch.getBranchId().equals("00010001A2460001CB54247F72024E3D0BCC")) {
        assertEquals("OVERRIDE",
                branch.getDisplayName());
        found = true;
      }
    }
    assertTrue(found);
  }
}
