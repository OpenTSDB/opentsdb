package net.opentsdb.storage.json;

import java.util.TreeMap;

import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.TestTree;
import net.opentsdb.tree.Tree;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class BranchMixInTest {
  private Tree tree = TestTree.buildTestTree();
  private ObjectMapper jsonMapper;

  @Before
  public void setUp() throws Exception {
    jsonMapper = new ObjectMapper();
    jsonMapper.registerModule(new GuavaModule());
    jsonMapper.registerModule(new StorageModule());
  }

  @Test
  public void testToStorageJson() throws JsonProcessingException {
    Branch branch = buildTestBranch(tree);
    byte[] answer = {123,34,112,97,116,104,34,58,123,34,48,34,58,34,82,
            79,79,84,34,125,44,34,100,105,115,112,108,97,121,78,97,109,
            101,34,58,34,82,79,79,84,34,125};
    assertArrayEquals(answer, jsonMapper.writeValueAsBytes(branch));
  }

  @Test
  public void testBuildFromJSON() {
    fail();
    /*
     * Right now we do not test the buildFromJSON function.
     * Needs to be done.
     */
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