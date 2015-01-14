package net.opentsdb.storage.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TreeMixInTest {
  private ObjectMapper jsonMapper;

  @Before
  public void setUp() throws Exception {
    jsonMapper = new ObjectMapper();
    jsonMapper.registerModule(new StorageModule());
  }

  @Test
  public void serialize() throws Exception {
    final String json = jsonMapper.writeValueAsString(buildTestTree());
    assertNotNull(json);
    assertTrue(json.contains("\"created\":1356998400"));
    assertTrue(json.contains("\"name\":\"Test Tree\""));
    assertTrue(json.contains("\"description\":\"My Description\""));
    assertTrue(json.contains("\"enabled\":true"));
  }

  @Test
  public void deserialize() throws Exception {
    fail();
    //TODO NOT IMPLEMENTED YET, will be implemented with jackson_json branch
    /*Tree t = JSON.parseToObject((byte[])TreetoStorageJson.invoke(
        buildTestTree()), Tree.class);
    assertTrue(t.getEnabled());*/
  }

  /**
   * Returns a 5 level rule set that parses a data center, a service, the
   * hostname, metric and some tags from meta data.
   * @param tree The tree to add the rules to
   */
  public static void buildTestRuleSet(final Tree tree) {
    // level 0
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRule.TreeRuleType.TAGK);
    rule.setRegex("^.*\\.([a-zA-Z]{3,4})[0-9]{0,1}\\..*\\..*$");
    rule.setField("fqdn");
    rule.setDescription("Datacenter");
    tree.addRule(rule);

    rule = new TreeRule(1);
    rule.setType(TreeRule.TreeRuleType.TAGK);
    rule.setRegex("^.*\\.([a-zA-Z]{3,4})[0-9]{0,1}\\..*\\..*$");
    rule.setField("host");
    rule.setDescription("Datacenter");
    rule.setOrder(1);
    tree.addRule(rule);

    // level 1
    rule = new TreeRule(1);
    rule.setType(TreeRule.TreeRuleType.TAGK);
    rule.setRegex("^([a-zA-Z]+)(\\-|[0-9])*.*\\..*$");
    rule.setField("fqdn");
    rule.setDescription("Service");
    rule.setLevel(1);
    tree.addRule(rule);

    rule = new TreeRule(1);
    rule.setType(TreeRule.TreeRuleType.TAGK);
    rule.setRegex("^([a-zA-Z]+)(\\-|[0-9])*.*\\..*$");
    rule.setField("host");
    rule.setDescription("Service");
    rule.setLevel(1);
    rule.setOrder(1);
    tree.addRule(rule);

    // level 2
    rule = new TreeRule(1);
    rule.setType(TreeRule.TreeRuleType.TAGK);
    rule.setField("fqdn");
    rule.setDescription("Hostname");
    rule.setLevel(2);
    tree.addRule(rule);

    rule = new TreeRule(1);
    rule.setType(TreeRule.TreeRuleType.TAGK);
    rule.setField("host");
    rule.setDescription("Hostname");
    rule.setLevel(2);
    rule.setOrder(1);
    tree.addRule(rule);

    // level 3
    rule = new TreeRule(1);
    rule.setType(TreeRule.TreeRuleType.METRIC);
    rule.setDescription("Metric split");
    rule.setSeparator("\\.");
    rule.setLevel(3);
    tree.addRule(rule);

    // level 4
    rule = new TreeRule(1);
    rule.setType(TreeRule.TreeRuleType.TAGK);
    rule.setField("type");
    rule.setDescription("Type Tag");
    rule.setLevel(4);
    rule.setOrder(0);
    tree.addRule(rule);

    rule = new TreeRule(1);
    rule.setType(TreeRule.TreeRuleType.TAGK);
    rule.setField("method");
    rule.setDescription("Method Tag");
    rule.setLevel(4);
    rule.setOrder(1);
    tree.addRule(rule);

    rule = new TreeRule(1);
    rule.setType(TreeRule.TreeRuleType.TAGK);
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