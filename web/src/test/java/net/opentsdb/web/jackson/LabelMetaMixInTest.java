package net.opentsdb.web.jackson;

import net.opentsdb.meta.LabelMeta;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.UniqueIdType;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterators;
import org.junit.Before;
import org.junit.Test;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;


public class LabelMetaMixInTest {
  private ObjectMapper jsonMapper;
  private LabelMeta labelMeta;

  @Before
  public void before() throws Exception {
    labelMeta = LabelMeta.create(mock(LabelId.class), METRIC, "sys.cpu.0", "Description", 1328140801);

    jsonMapper = new ObjectMapper();
    jsonMapper.registerModule(new JacksonModule());
  }

  @Test
  public void serializeFieldSet() throws Exception {
    final byte[] json = jsonMapper.writeValueAsBytes(labelMeta);

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
    final byte[] json = jsonMapper.writeValueAsBytes(labelMeta);

    ObjectNode rootNode = jsonMapper.readValue(json, ObjectNode.class);

    assertEquals(3, Iterators.size(rootNode.fields()));
    assertEquals("METRIC", rootNode.get("type").asText());
    assertEquals("Description", rootNode.get("description").textValue());
    assertEquals(1328140801, rootNode.get("created").longValue());
  }

  @Test
  public void deserialize() throws Exception {
    String json = "{\"identifier\":\"ABCD\",\"type\":\"MeTriC\",\"name\":\"MyName\"," +
    "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
    "1328140801,\"displayName\":\"Empty\",\"unknownkey\":null}";

    final LabelId uid = mock(LabelId.class);

    InjectableValues vals = new InjectableValues.Std()
            .addValue(LabelId.class, uid)
            .addValue(UniqueIdType.class, UniqueIdType.METRIC)
            .addValue(String.class, "MyOtherName");

    LabelMeta meta = jsonMapper.reader(LabelMeta.class)
            .with(vals)
            .readValue(json);

    assertNotNull(meta);
    assertEquals(uid, meta.identifier());
    assertEquals(UniqueIdType.METRIC, meta.type());
    assertEquals("MyOtherName", meta.name());
    assertEquals("Description", meta.description());
    assertEquals(1328140801, meta.created());
  }

  /**
   * This method tests what happens when you try to deserialize a {@link LabelMeta}
   * object from JSON. It should throw an IllegalArgumentException due to how
   * {@link UniqueIdTypeDeserializer} parses types.
   * This conforms to opentsdb/opentsdb as of commit
   * 8e3d0cc8ed82842819c7adee3339c274604be277.
   */
  @Test (expected = IllegalArgumentException.class)
  public void deserializeWithNullType() throws Exception {
    String json = "{\"identifier\":\"ABCD\",\"type\":\"null\",\"name\":\"MyName\"," +
            "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
            "1328140801,\"displayName\":\"Empty\",\"unknownkey\":null}";

    final byte[] uid = {0, (byte) 16, (byte) -125};

    InjectableValues vals = new InjectableValues.Std()
            .addValue(byte[].class, uid)
            .addValue(String.class, "MyOtherName");

    jsonMapper.reader(LabelMeta.class)
            .with(vals)
            .readValue(json);
  }
}