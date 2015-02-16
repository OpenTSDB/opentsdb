package net.opentsdb.storage.json;

import java.io.IOException;
import java.math.BigInteger;

import net.opentsdb.tree.Leaf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class LeafMixInTest {
  private ObjectMapper jsonMapper;

  @Before
  public void setUp() throws Exception {
    jsonMapper = new ObjectMapper();
    jsonMapper.registerModule(new GuavaModule());
    jsonMapper.registerModule(new StorageModule());
  }

  @Test
  public void testBuildFromJSON() throws IOException {
    final Leaf leaf = new Leaf("Leaf", "000001000001000001");
    final String s = "7b22646973706c61794e616d65223a224c6561" +
            "66222c227473756964223a22303030303031303030303031303030303031227d";

    byte [] json = new BigInteger(s, 16).toByteArray();

    final Leaf jsonLeaf = jsonMapper.readValue(json, Leaf.class);
    assertEquals(leaf.getDisplayName(), jsonLeaf.getDisplayName());
    assertEquals(leaf.getTsuid(), jsonLeaf.getTsuid());
  }

  @Test
  public void testGetStorageJSON() throws JsonProcessingException {
    final Leaf leaf = new Leaf("Leaf", "000001000001000001");

    byte[] storage_json = jsonMapper.writeValueAsBytes(leaf);

    final String s = "7b22646973706c61794e616d65223a224c656166222c2274737569" +
            "64223a22303030303031303030303031303030303031227d";

    byte [] json = new BigInteger(s, 16).toByteArray();

    assertArrayEquals(json, storage_json);
  }
}