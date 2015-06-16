package net.opentsdb.web.jackson;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import net.opentsdb.meta.LabelMeta;
import net.opentsdb.storage.MemoryLabelId;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.web.HttpModule;
import net.opentsdb.web.TestHttpModule;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterators;
import dagger.ObjectGraph;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import javax.inject.Inject;


public class LabelMetaMixInTest {
  private final String id = "d2576c75-8825-4ec2-8d93-311423c05c98";
  private final LabelId labelId = new MemoryLabelId(UUID.fromString(id));

  @Inject ObjectMapper jsonMapper;

  private LabelMeta labelMeta;

  @Before
  public void before() throws Exception {
    ObjectGraph.create(new TestHttpModule(), new HttpModule()).inject(this);

    labelMeta = LabelMeta.create(labelId, METRIC, "sys.cpu.0", "Description", 1328140801L);
  }

  @Test
  public void serializesFields() throws Exception {
    final String json = jsonMapper.writeValueAsString(labelMeta);

    final ObjectNode rootNode = jsonMapper.readValue(json, ObjectNode.class);

    assertEquals(5, Iterators.size(rootNode.fields()));
    assertEquals(id, rootNode.get("identifier").asText());
    assertEquals("METRIC", rootNode.get("type").asText());
    assertEquals("sys.cpu.0", rootNode.get("name").textValue());
    assertEquals("Description", rootNode.get("description").textValue());
    assertEquals(1328140801, rootNode.get("created").longValue());
  }

  @Test
  public void deserialize() throws Exception {
    final String json = "{\"identifier\":\"d2576c75-8825-4ec2-8d93-311423c05c98\","
                        + "\"type\":\"MeTriC\",\"name\":\"MyName\",\"description\":\"Description\","
                        + "\"notes\":\"MyNotes\",\"created\":1328140801,\"displayName\":\"Empty\","
                        + "\"unknownkey\":null}";

    LabelMeta meta = jsonMapper.reader(LabelMeta.class)
        .readValue(json);

    assertNotNull(meta);
    assertEquals(labelId, meta.identifier());
    assertEquals(UniqueIdType.METRIC, meta.type());
    assertEquals("MyName", meta.name());
    assertEquals("Description", meta.description());
    assertEquals(1328140801L, meta.created());
  }

  /**
   * This method tests what happens when you try to deserialize a {@link LabelMeta} object from
   * JSON. It should throw an IllegalArgumentException due to how {@link UniqueIdTypeDeserializer}
   * parses types.
   */
  @Test(expected = IllegalArgumentException.class)
  public void deserializeWithNullType() throws Exception {
    String json = "{\"identifier\":\"ABCD\",\"type\":\"null\",\"name\":\"MyName\"," +
                  "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
                  "1328140801,\"displayName\":\"Empty\",\"unknownkey\":null}";

    jsonMapper.reader(LabelMeta.class)
        .readValue(json);
  }
}