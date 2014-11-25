package net.opentsdb.storage.json;

import java.util.Iterator;
import java.util.Map;

import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.UniqueIdType;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.junit.Before;
import org.junit.Test;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class UIDMetaMixInTest {
  private ObjectMapper jsonMapper;
  private UIDMeta uidMeta;

  @Before
  public void before() throws Exception {
    uidMeta = new UIDMeta(METRIC, new byte[] {0, 0, 1}, "sys.cpu.0");
    uidMeta.setDescription("Description");
    uidMeta.setNotes("MyNotes");
    uidMeta.setCreated(1328140801);
    uidMeta.setDisplayName("System CPU");

    jsonMapper = new ObjectMapper();
    jsonMapper.registerModule(new StorageModule());
  }

  @Test
  public void serializeFieldSet() throws Exception {
    final byte[] json = jsonMapper.writeValueAsBytes(uidMeta);

    ObjectNode rootNode = jsonMapper.readValue(json, ObjectNode.class);

    assertEquals(6, Iterators.size(rootNode.fields()));
    assertEquals("METRIC", rootNode.get("type").asText());
    assertEquals("System CPU", rootNode.get("displayName").textValue());
    assertEquals("Description", rootNode.get("description").textValue());
    assertEquals("MyNotes", rootNode.get("notes").textValue());
    assertEquals(JsonNodeType.NULL, rootNode.get("custom").getNodeType());
    assertEquals(1328140801, rootNode.get("created").longValue());
  }

  @Test
  public void serializeCustomSet() throws Exception {
    final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("A", "B");
    uidMeta.setCustom(builder.build());
    final byte[] json = jsonMapper.writeValueAsBytes(uidMeta);

    ObjectNode rootNode = jsonMapper.readValue(json, ObjectNode.class);

    assertEquals(6, Iterators.size(rootNode.fields()));
    assertEquals("METRIC", rootNode.get("type").asText());
    assertEquals("System CPU", rootNode.get("displayName").textValue());
    assertEquals("Description", rootNode.get("description").textValue());
    assertEquals("MyNotes", rootNode.get("notes").textValue());
    assertEquals("A", rootNode.get("custom").fieldNames().next());
    assertEquals(1328140801, rootNode.get("created").longValue());
  }

  @Test
  public void deserialize() throws Exception {
    String json = "{\"uid\":\"ABCD\",\"type\":\"MeTriC\",\"name\":\"MyName\"," +
    "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
    "1328140801,\"displayName\":\"Empty\",\"unknownkey\":null}";

    final byte[] uid = {0, (byte) 16, (byte) -125};

    InjectableValues vals = new InjectableValues.Std()
            .addValue(byte[].class, uid)
            .addValue(UniqueIdType.class, UniqueIdType.METRIC)
            .addValue(String.class, "MyOtherName");

    UIDMeta meta = jsonMapper.reader(UIDMeta.class)
            .with(vals)
            .readValue(json);

    assertNotNull(meta);
    assertArrayEquals(uid, meta.getUID());
    assertEquals(UniqueIdType.METRIC, meta.getType());
    assertEquals("MyOtherName", meta.getName());
    assertEquals("Description", meta.getDescription());
    assertEquals("MyNotes", meta.getNotes());
    assertEquals(1328140801, meta.getCreated());
    assertEquals("Empty", meta.getDisplayName());
  }

  /**
   * This method tests what happens when you try to deserialize a UIDMeta
   * object from JSON. It should throw an IllegalArgumentException due to how
   * {@link net.opentsdb.utils.JSON.UniqueIdTypeDeserializer} parses types.
   * This conforms to opentsdb/opentsdb as of commit
   * 8e3d0cc8ed82842819c7adee3339c274604be277.
   */
  @Test (expected = IllegalArgumentException.class)
  public void deserializeWithNullType() throws Exception {
    String json = "{\"uid\":\"ABCD\",\"type\":\"null\",\"name\":\"MyName\"," +
            "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
            "1328140801,\"displayName\":\"Empty\",\"unknownkey\":null}";

    final byte[] uid = {0, (byte) 16, (byte) -125};

    InjectableValues vals = new InjectableValues.Std()
            .addValue(byte[].class, uid)
            .addValue(String.class, "MyOtherName");

    jsonMapper.reader(UIDMeta.class)
            .with(vals)
            .readValue(json);
  }
}