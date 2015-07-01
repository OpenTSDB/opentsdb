package net.opentsdb.web.jackson;

import static net.opentsdb.uid.LabelType.METRIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import net.opentsdb.meta.LabelMeta;
import net.opentsdb.storage.MemoryLabelId;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.LabelType;
import net.opentsdb.web.DaggerTestHttpComponent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterators;
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
    DaggerTestHttpComponent.create().inject(this);

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
                        + "\"type\":\"METRIC\",\"name\":\"sys.cpu.0\","
                        + "\"description\":\"Description\",\"created\":1328140801}";

    LabelMeta meta = jsonMapper.reader(LabelMeta.class)
        .readValue(json);

    assertNotNull(meta);
    assertEquals(labelId, meta.identifier());
    assertEquals(LabelType.METRIC, meta.type());
    assertEquals("sys.cpu.0", meta.name());
    assertEquals("Description", meta.description());
    assertEquals(1328140801L, meta.created());
  }

  @Test(expected = UnrecognizedPropertyException.class)
  public void deserializeUnknownField() throws Exception {
    final String jsonWithUnknown = "{\"identifier\":\"d2576c75-8825-4ec2-8d93-311423c05c98\","
                                   + "\"type\":\"METRIC\",\"name\":\"sys.cpu.0\","
                                   + "\"description\":\"Description\",\"created\":1328140801,"
                                   + "\"unknown\":null}";

    jsonMapper.reader(LabelMeta.class)
        .readValue(jsonWithUnknown);
  }
}