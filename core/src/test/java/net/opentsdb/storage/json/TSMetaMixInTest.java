package net.opentsdb.storage.json;

import net.opentsdb.meta.TSMeta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TSMetaMixInTest {
  private ObjectMapper jsonMapper;
  private TSMeta meta;

  @Before
  public void setUp() throws Exception {
    jsonMapper = new ObjectMapper();
    jsonMapper.registerModule(new GuavaModule());
    jsonMapper.registerModule(new StorageModule());

    meta = new TSMeta();
  }

  @Test
  public void serialize() throws Exception {
    final String json = jsonMapper.writeValueAsString(meta);
    assertNotNull(json);
    assertTrue(json.contains("\"created\":0"));
  }

  @Test
  public void deserialize() throws Exception {
    String json = "{\"tsuid\":\"ABCD\",\"" +
            "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
            "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
            "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\",\"lastReceived" +
            "\":1328140801,\"unknownkey\":null}";
    TSMeta tsmeta = jsonMapper.readValue(json, TSMeta.class);
    assertNotNull(tsmeta);
    assertEquals("ABCD", tsmeta.getTSUID());
    assertEquals("Notes", tsmeta.getNotes());
    assertEquals(42, tsmeta.getRetention());
  }
}