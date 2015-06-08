package net.opentsdb.web.jackson;

import static org.junit.Assert.assertEquals;

import net.opentsdb.meta.Annotation;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

public class AnnotationMixInTest {
  private ObjectMapper jsonMapper;

  private Annotation note;
  private String note_json;

  @Before
  public void setUp() throws Exception {
    jsonMapper = new ObjectMapper();
    jsonMapper.registerModule(new JacksonModule());

    note = new Annotation("000001000001000001", 1328140800, 1328140801,
        "Description", "Notes", null);

    note_json = "{\"tsuid\":\"000001000001000001\",\"startTime\":1328140800," +
        "\"endTime\":1328140801,\"description\":\"Description\",\"notes\":\"Notes\"}";
  }

  @Test
  public void serializeMatchesExactly() throws Exception {
    final String json = new String(jsonMapper.writeValueAsBytes(note));
    assertEquals(note_json, json);
  }

  @Test
  public void serializeSkipsNull() throws Exception {
    note = new Annotation(null, 1328140800, 1328140801,
        "Description", "Notes", null);
    note_json = "{\"startTime\":1328140800,\"endTime\":1328140801," +
        "\"description\":\"Description\",\"notes\":\"Notes\"}";
    final String json = new String(jsonMapper.writeValueAsBytes(note));
    assertEquals(note_json, json);
  }

  @Test
  public void deserialize() throws Exception {
    Annotation parsed_note = jsonMapper.reader(Annotation.class)
        .readValue(note_json);
    assertEquals(note, parsed_note);
  }

  @Test
  public void deserializeIgnoresUnknown() throws Exception {
    final String note_json_with_unknown = "{\"tsuid\":\"000001000001000001\"," +
        "\"startTime\":1328140800,\"endTime\":1328140801,\"unknown\":1328140801," +
        "\"description\":\"Description\",\"notes\":\"Notes\",\"custom\":null}";
    Annotation parsed_note = jsonMapper.reader(Annotation.class)
        .readValue(note_json_with_unknown);
    assertEquals(note, parsed_note);
  }
}
