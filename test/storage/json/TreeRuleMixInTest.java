package net.opentsdb.storage.json;

import java.io.IOException;

import net.opentsdb.tree.TreeRule;
import net.opentsdb.utils.JSON;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TreeRuleMixInTest {
  private ObjectMapper jsonMapper;

  @Before
  public void before() throws Exception {
    jsonMapper = new ObjectMapper();
    jsonMapper.registerModule(new StorageModule());
  }

  @Test
  public void serialize() throws JsonProcessingException {
    final TreeRule rule = new TreeRule();
    rule.setField("host");

    final String json = jsonMapper.writeValueAsString(rule);
    assertNotNull(json);
    assertTrue(json.contains("\"field\":\"host\""));
  }

  @Test
  public void deserialize() throws IOException {
    final String json = "{\"type\":\"METRIC\",\"field\":\"host\",\"regex\":" +
            "\"^[a-z]$\",\"separator\":\".\",\"description\":\"My Description\"," +
            "\"notes\":\"Got Notes?\",\"display_format\":\"POP {ovalue}\",\"level\":1" +
            ",\"order\":2,\"customField\":\"\",\"regexGroupIdx\":1,\"treeId\":42," +
            "\"UnknownKey\":\"UnknownVal\"}";
    final TreeRule rule = jsonMapper.readValue(json, TreeRule.class);
    assertNotNull(rule);
    assertEquals(42, rule.getTreeId());
    assertEquals("^[a-z]$", rule.getRegex());
    assertNotNull(rule.getCompiledRegex());
  }

  @Test (expected = JsonMappingException.class)
  public void deserializeBadRegexCompile() throws IOException {
    final String json = "{\"type\":\"METRIC\",\"field\":\"host\",\"regex\":" +
            "\"^(ok$\",\"separator\":\".\",\"description\":\"My Description\"," +
            "\"notes\":\"Got Notes?\",\"display_format\":\"POP {ovalue}\",\"level\":1" +
            ",\"order\":2,\"customField\":\"\",\"regexGroupIdx\":1,\"treeId\":42," +
            "\"UnknownKey\":\"UnknownVal\"}";
    final TreeRule rule = jsonMapper.readValue(json, TreeRule.class);
  }
}
