// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.configuration.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.never;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
//import com.google.common.hash.HashFunction;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.Configuration;
//import net.opentsdb.utils.UnitTestException;

public class TestYamlJsonFileProvider {
  private ProviderFactory factory;
  private Configuration config;
  private HashedWheelTimer timer;

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    factory = mock(ProviderFactory.class);
    config = mock(Configuration.class);
    timer = mock(HashedWheelTimer.class);
  }

  @Test
  public void fileEmpty() throws Exception {
    final File jsonFile = tempDir.newFile("test.json");

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + jsonFile);

    assertTrue(provider.cache.isEmpty());
    assertEquals(jsonFile.toString(), provider.file_name);
    assertEquals(0, provider.last_hash);
  }

  @Test
  public void fileEmptyJsonObject() throws Exception {
    final File jsonFile = tempDir.newFile("test.json");
    final FileWriter writer = new FileWriter(jsonFile, false);
    writer.write("{}");
    writer.close();

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + jsonFile);

    assertTrue(provider.cache.isEmpty());
    assertEquals(jsonFile.toString(), provider.file_name);
    assertEquals(0x466E20057851C2D2L,provider.last_hash);
  }

  @Test
  public void fileJsonArray() throws Exception {
    final File jsonFile = tempDir.newFile("test.json");
    final FileWriter writer = new FileWriter(jsonFile, false);
    writer.write("[]");
    writer.close();

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + jsonFile);

    assertTrue(provider.cache.isEmpty());
    assertEquals(jsonFile.toString(), provider.file_name);
    assertEquals(0xCF252FDCD0C57791L, provider.last_hash);
  }

  /*
  @Test
  public void hashException() throws Exception {
    final File jsonFile = tempDir.newFile("test.json");

    Mockito.doThrow(new UnitTestException()).when(source).hash(
        any(HashFunction.class));

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + jsonFile);

    verify(source, never()).openStream();
    assertTrue(provider.cache.isEmpty());
    assertEquals(jsonFile.toString(), provider.file_name);
    assertEquals(0, provider.last_hash);
  }
  */

  @Test
  public void flatJsonObject() throws Exception {
    final File jsonFile = tempDir.newFile("test.json");
    final FileWriter writer = new FileWriter(jsonFile, false);
    writer.write("{\"key.a\":\"a String\",\"key.b\":null,\"key.c\":"
         + "42.5,\"key.d\":24,\"key.e\":true,\"key.f\":[\"s1\",\"s2\"],"
         + "\"key.g\":{\"k1\":\"v1\",\"k2\":\"v2\"}}");
    writer.close();

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + jsonFile);

    assertEquals(jsonFile.toString(), provider.file_name);
    assertEquals(0xE31ED7ED74C4AA12L, provider.last_hash);

    assertEquals(6, provider.cache.size());

    assertTrue(provider.cache.get("key.a") instanceof String);
    assertEquals("a String", provider.getSetting("key.a").getValue());

    assertFalse(provider.cache.containsKey("key.b"));
    assertNull(provider.getSetting("key.b"));

    assertTrue(Double.class.isInstance(provider.cache.get("key.c")));
    assertEquals(42.5, (double) provider.getSetting("key.c").getValue(), 0.001);

    assertTrue(Long.class.isInstance(provider.cache.get("key.d")));
    assertEquals(24, (long) provider.getSetting("key.d").getValue());

    assertTrue(Boolean.class.isInstance(provider.cache.get("key.e")));
    assertTrue((boolean) provider.getSetting("key.e").getValue());

    TypeReference<List<String>> ref = new TypeReference<List<String>>() { };
    assertTrue(provider.getSetting("key.f").getValue() instanceof JsonNode);
    List<String> list = Configuration.OBJECT_MAPPER.convertValue(
        provider.getSetting("key.f").getValue(), ref);
    assertEquals(2, list.size());
    assertTrue(list.contains("s1"));
    assertTrue(list.contains("s2"));

    assertTrue(provider.getSetting("key.g").getValue() instanceof JsonNode);
    PojoTest pojo = Configuration.OBJECT_MAPPER.convertValue(
        provider.getSetting("key.g").getValue(), PojoTest.class);
    assertEquals("v1", pojo.k1);
    assertEquals("v2", pojo.k2);
  }

  @Test
  public void flatYamlObject() throws Exception {
    final String yaml = "--- \n" +
        "key.a: \"a String\"\n" +
        "key.b: null\n" +
        "key.c: 42.5\n" +
        "key.d: 24\n" +
        "key.e: true\n" +
        "key.f: \n" +
        "  - s1\n" +
        "  - s2\n" +
        "key.g: \n" +
        "  k1: v1\n" +
        "  k2: v2\n";

    final File yamlFile = tempDir.newFile("test.json");
    final FileWriter writer = new FileWriter(yamlFile, false);
    writer.write(yaml);
    writer.close();

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + yamlFile);

    assertEquals(yamlFile.toString(), provider.file_name);
    assertEquals(0x4DEEE69DB7B627D7L, provider.last_hash);

    assertEquals(6, provider.cache.size());

    assertTrue(provider.cache.get("key.a") instanceof String);
    assertEquals("a String", provider.getSetting("key.a").getValue());

    assertFalse(provider.cache.containsKey("key.b"));
    assertNull(provider.getSetting("key.b"));

    assertTrue(Double.class.isInstance(provider.cache.get("key.c")));
    assertEquals(42.5, (double) provider.getSetting("key.c").getValue(), 0.001);

    assertTrue(Long.class.isInstance(provider.cache.get("key.d")));
    assertEquals(24, (long) provider.getSetting("key.d").getValue());

    assertTrue(Boolean.class.isInstance(provider.cache.get("key.e")));
    assertTrue((boolean) provider.getSetting("key.e").getValue());

    TypeReference<List<String>> ref = new TypeReference<List<String>>() { };
    assertTrue(provider.getSetting("key.f").getValue() instanceof JsonNode);
    List<String> list = Configuration.OBJECT_MAPPER.convertValue(
        provider.getSetting("key.f").getValue(), ref);
    assertEquals(2, list.size());
    assertTrue(list.contains("s1"));
    assertTrue(list.contains("s2"));

    assertTrue(provider.getSetting("key.g").getValue() instanceof JsonNode);
    PojoTest pojo = Configuration.OBJECT_MAPPER.convertValue(
        provider.getSetting("key.g").getValue(), PojoTest.class);
    assertEquals("v1", pojo.k1);
    assertEquals("v2", pojo.k2);
  }

  @Test
  public void nestedJson() throws Exception {
    final String json = "{\"root\":{\"a\":{\"b\":\"Hello\",\"c\":\"World\"},"
        + "\"array\":[{\"k\":\"v\"}]}}";

    final File jsonFile = tempDir.newFile("test.json");
    final FileWriter writer = new FileWriter(jsonFile, false);
    writer.write(json);
    writer.close();

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + jsonFile);

    assertEquals(jsonFile.toString(), provider.file_name);
    assertEquals(0x6CFBB3C2F4FA54D8L, provider.last_hash);

    assertEquals(1, provider.cache.size());
    assertTrue(provider.getSetting("root").getValue() instanceof JsonNode);

    JsonNode node = (JsonNode) provider.getSetting("root.a").getValue();
    assertEquals(JsonNodeType.OBJECT, node.getNodeType());
    assertNotNull(node.get("b"));
    assertEquals(2, provider.cache.size());
    assertSame(node, provider.cache.get("root.a"));

    assertEquals("Hello", provider.getSetting("root.a.b").getValue());
    assertEquals(3, provider.cache.size());
    assertEquals("Hello", provider.cache.get("root.a.b"));

    assertNull(provider.getSetting("root.a.d"));
    assertNull(provider.getSetting("root.array.k"));
    assertEquals(3, provider.cache.size());
  }

  @Test
  public void nestedYaml() throws Exception {
    final String yaml = "--- \n" +
        "root: \n" +
        "  a: \n" +
        "    b: Hello\n" +
        "    c: World\n" +
        "  array: \n" +
        "    - \n" +
        "      k: v\n" +
        "";

    final File yamlFile = tempDir.newFile("test.json");
    final FileWriter writer = new FileWriter(yamlFile, false);
    writer.write(yaml);
    writer.close();

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + yamlFile);

    assertEquals(yamlFile.toString(), provider.file_name);
    assertEquals(0xFBFD981526D227B0L, provider.last_hash);

    assertEquals(1, provider.cache.size());
    assertTrue(provider.getSetting("root").getValue() instanceof JsonNode);

    JsonNode node = (JsonNode) provider.getSetting("root.a").getValue();
    assertEquals(JsonNodeType.OBJECT, node.getNodeType());
    assertNotNull(node.get("b"));
    assertEquals(2, provider.cache.size());
    assertSame(node, provider.cache.get("root.a"));

    assertEquals("Hello", provider.getSetting("root.a.b").getValue());
    assertEquals(3, provider.cache.size());
    assertEquals("Hello", provider.cache.get("root.a.b"));

    assertNull(provider.getSetting("root.a.d"));
    assertNull(provider.getSetting("root.array.k"));
    assertEquals(3, provider.cache.size());
  }

  @Test
  public void nestedTypes() throws Exception {
    final String yaml = "--- \n" +
        "root: \n" +
        "  a: \n" +
        "    b: Hello\n" +
        "    c: 24\n" +
        "    d: 42.5\n" +
        "    e: true\n" +
        "    f: ~\n" +
        "";

    final File yamlFile = tempDir.newFile("test.json");
    final FileWriter writer = new FileWriter(yamlFile, false);
    writer.write(yaml);
    writer.close();

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + yamlFile);

    assertEquals(yamlFile.toString(), provider.file_name);
    assertEquals(0x006197E3E9FD901BL, provider.last_hash);

    assertEquals(1, provider.cache.size());
    assertTrue(provider.getSetting("root").getValue() instanceof JsonNode);

    assertEquals("Hello", provider.getSetting("root.a.b").getValue());
    assertEquals(2, provider.cache.size());

    assertEquals(24, (long) provider.getSetting("root.a.c").getValue());
    assertEquals(3, provider.cache.size());

    assertEquals(42.5, (double) provider.getSetting("root.a.d").getValue(), 0.001);
    assertEquals(4, provider.cache.size());

    assertTrue((boolean) provider.getSetting("root.a.e").getValue());
    assertEquals(5, provider.cache.size());

    assertNull(provider.getSetting("root.a.f"));
  }

  @Test
  public void badParse() throws Exception {
    final String json = "{\"key.a\":\"a String\",\"key.b\":null,\"key.c\":"
        + "42.5,\"key.d\":24,\"key.e\":true,\"key.f\":[\"s1\",\"s2\"],"
        + "\"key.g\":{\"k1\":\"v1\",\"k}";

    final File jsonFile = tempDir.newFile("test.json");
    final FileWriter writer = new FileWriter(jsonFile, false);
    writer.write(json);
    writer.close();

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + jsonFile);

    assertTrue(provider.cache.isEmpty());
  }

  @Test
  public void reloadFlatSameHash() throws Exception {
    final String json = "{\"key.a\":\"a String\",\"key.b\":null,\"key.c\":"
        + "42.5,\"key.d\":24,\"key.e\":true,\"key.f\":[\"s1\",\"s2\"],"
        + "\"key.g\":{\"k1\":\"v1\",\"k2\":\"v2\"}}";

    final File jsonFile = tempDir.newFile("test.json");
    final FileWriter writer = new FileWriter(jsonFile, false);
    writer.write(json);
    writer.close();

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + jsonFile);

    final long expectedHashCode = 0xE31ED7ED74C4AA12L;
    assertEquals(jsonFile.toString(), provider.file_name);
    assertEquals(expectedHashCode, provider.last_hash);
    assertEquals(6, provider.cache.size());

    provider.reload();

    assertEquals(jsonFile.toString(), provider.file_name);
    assertEquals(expectedHashCode, provider.last_hash);
    assertEquals(6, provider.cache.size());
  }

  @Test
  public void reloadFlatChanges() throws Exception {
    String json = "{\"key.a\":\"a String\",\"key.b\":null,\"key.c\":"
        + "42.5,\"key.d\":24,\"key.e\":true,\"key.f\":[\"s1\",\"s2\"],"
        + "\"key.g\":{\"k1\":\"v1\",\"k2\":\"v2\"}}";

    final File jsonFile = tempDir.newFile("test.json");
    FileWriter writer = new FileWriter(jsonFile, false);
    writer.write(json);
    writer.close();

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + jsonFile);

    assertEquals(6, provider.cache.size());

    assertTrue(provider.cache.get("key.a") instanceof String);
    assertEquals("a String", provider.getSetting("key.a").getValue());

    assertFalse(provider.cache.containsKey("key.b"));
    assertNull(provider.getSetting("key.b"));

    assertTrue(Double.class.isInstance(provider.cache.get("key.c")));
    assertEquals(42.5, (double) provider.getSetting("key.c").getValue(), 0.001);

    assertTrue(Long.class.isInstance(provider.cache.get("key.d")));
    assertEquals(24, (long) provider.getSetting("key.d").getValue());

    assertTrue(Boolean.class.isInstance(provider.cache.get("key.e")));
    assertTrue((boolean) provider.getSetting("key.e").getValue());

    TypeReference<List<String>> ref = new TypeReference<List<String>>() { };
    assertTrue(provider.getSetting("key.f").getValue() instanceof JsonNode);
    List<String> list = Configuration.OBJECT_MAPPER.convertValue(
        provider.getSetting("key.f").getValue(), ref);
    assertEquals(2, list.size());
    assertTrue(list.contains("s1"));
    assertTrue(list.contains("s2"));

    assertTrue(provider.getSetting("key.g").getValue() instanceof JsonNode);
    PojoTest pojo = Configuration.OBJECT_MAPPER.convertValue(
        provider.getSetting("key.g").getValue(), PojoTest.class);
    assertEquals("v1", pojo.k1);
    assertEquals("v2", pojo.k2);

    // reload
    json = "{\"key.a\":\"Diff string\",\"key.b\":\"Set\",\"key.c\":"
        + "42.5,\"key.e\":false,\"key.f\":[\"s2\"],"
        + "\"key.g\":{\"k1\":\"va\",\"k3\":\"vb\"}}";
    writer = new FileWriter(jsonFile, false);
    writer.write(json);
    writer.close();

    provider.reload();
    assertEquals(6, provider.cache.size());

    assertTrue(provider.cache.get("key.a") instanceof String);
    assertEquals("Diff string", provider.getSetting("key.a").getValue());

    assertTrue(provider.cache.containsKey("key.b"));
    assertEquals("Set", provider.getSetting("key.b").getValue());

    assertTrue(Double.class.isInstance(provider.cache.get("key.c")));
    assertEquals(42.5, (double) provider.getSetting("key.c").getValue(), 0.001);

    assertFalse(provider.cache.containsKey("key.d"));
    assertNull(provider.getSetting("key.d"));

    assertTrue(Boolean.class.isInstance(provider.cache.get("key.e")));
    assertFalse((boolean) provider.getSetting("key.e").getValue());

    assertTrue(provider.getSetting("key.f").getValue() instanceof JsonNode);
    list = Configuration.OBJECT_MAPPER.convertValue(
        provider.getSetting("key.f").getValue(), ref);
    assertEquals(1, list.size());
    assertTrue(list.contains("s2"));

    assertTrue(provider.getSetting("key.g").getValue() instanceof JsonNode);
    pojo = Configuration.OBJECT_MAPPER.convertValue(
        provider.getSetting("key.g").getValue(), PojoTest.class);
    assertEquals("va", pojo.k1);
    assertNull(pojo.k2);
  }

  @Test
  public void reloadFlatToEmpty() throws Exception {
    String json = "{\"key.a\":\"a String\",\"key.b\":null,\"key.c\":"
        + "42.5,\"key.d\":24,\"key.e\":true,\"key.f\":[\"s1\",\"s2\"],"
        + "\"key.g\":{\"k1\":\"v1\",\"k2\":\"v2\"}}";

    final File jsonFile = tempDir.newFile("test.json");
    FileWriter writer = new FileWriter(jsonFile, false);
    writer.write(json);
    writer.close();

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + jsonFile);

    assertEquals(6, provider.cache.size());

    assertTrue(provider.cache.get("key.a") instanceof String);
    assertEquals("a String", provider.getSetting("key.a").getValue());

    assertFalse(provider.cache.containsKey("key.b"));
    assertNull(provider.getSetting("key.b"));

    assertTrue(Double.class.isInstance(provider.cache.get("key.c")));
    assertEquals(42.5, (double) provider.getSetting("key.c").getValue(), 0.001);

    assertTrue(Long.class.isInstance(provider.cache.get("key.d")));
    assertEquals(24, (long) provider.getSetting("key.d").getValue());

    assertTrue(Boolean.class.isInstance(provider.cache.get("key.e")));
    assertTrue((boolean) provider.getSetting("key.e").getValue());

    TypeReference<List<String>> ref = new TypeReference<List<String>>() { };
    assertTrue(provider.getSetting("key.f").getValue() instanceof JsonNode);
    List<String> list = Configuration.OBJECT_MAPPER.convertValue(
        provider.getSetting("key.f").getValue(), ref);
    assertEquals(2, list.size());
    assertTrue(list.contains("s1"));
    assertTrue(list.contains("s2"));

    assertTrue(provider.getSetting("key.g").getValue() instanceof JsonNode);
    PojoTest pojo = Configuration.OBJECT_MAPPER.convertValue(
        provider.getSetting("key.g").getValue(), PojoTest.class);
    assertEquals("v1", pojo.k1);
    assertEquals("v2", pojo.k2);

    // reload
    json = "{}";
    writer = new FileWriter(jsonFile, false);
    writer.write(json);
    writer.close();

    provider.reload();
    assertEquals(0, provider.cache.size());
  }

  @Test
  public void reloadNested() throws Exception {
    String json = "{\"root\":{\"a\":{\"b\":\"Hello\",\"c\":\"World\"},"
        + "\"array\":[{\"k\":\"v\"}]}}";

    final File jsonFile = tempDir.newFile("test.json");
    FileWriter writer = new FileWriter(jsonFile, false);
    writer.write(json);
    writer.close();

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + jsonFile);

    assertEquals(1, provider.cache.size());
    assertTrue(provider.getSetting("root").getValue() instanceof JsonNode);

    JsonNode node = (JsonNode) provider.getSetting("root.a").getValue();
    assertEquals(JsonNodeType.OBJECT, node.getNodeType());
    assertNotNull(node.get("b"));
    assertEquals(2, provider.cache.size());
    assertSame(node, provider.cache.get("root.a"));

    assertEquals("Hello", provider.getSetting("root.a.b").getValue());
    assertEquals(3, provider.cache.size());
    assertEquals("Hello", provider.cache.get("root.a.b"));

    assertNull(provider.getSetting("root.a.d"));
    assertNull(provider.getSetting("root.array.k"));
    assertEquals(3, provider.cache.size());

    json = "{\"root\":{\"a\":{\"b\":\"Diff\",\"c\":\"Value\"},"
        + "\"array\":[{\"k1\":\"v1\"}]}}";
    writer = new FileWriter(jsonFile, false);
    writer.write(json);
    writer.close();

    provider.reload();
    assertEquals(3, provider.cache.size());

    node = (JsonNode) provider.getSetting("root.a").getValue();
    assertEquals(JsonNodeType.OBJECT, node.getNodeType());
    assertNotNull(node.get("b"));
    assertEquals(3, provider.cache.size());
    assertSame(node, provider.cache.get("root.a"));

    TypeReference<Map<String, String>> ref =
        new TypeReference<Map<String, String>>() { };
    Map<String, String> map = Configuration.OBJECT_MAPPER.convertValue(node, ref);
    assertEquals(2, map.size());
    assertEquals("Diff", map.get("b"));
    assertEquals("Value", map.get("c"));

    assertEquals("Diff", provider.getSetting("root.a.b").getValue());
    assertEquals(3, provider.cache.size());
    assertEquals("Diff", provider.cache.get("root.a.b"));

    assertNull(provider.getSetting("root.a.d"));
    assertNull(provider.getSetting("root.array.k"));
    assertNull(provider.getSetting("root.array.k1"));
  }

  @Test
  public void reloadNestedEmpty() throws Exception {
    String json = "{\"root\":{\"a\":{\"b\":\"Hello\",\"c\":\"World\"},"
        + "\"array\":[{\"k\":\"v\"}]}}";

    final File jsonFile = tempDir.newFile("test.json");
    FileWriter writer = new FileWriter(jsonFile, false);
    writer.write(json);
    writer.close();

    final YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://" + jsonFile);

    assertEquals(1, provider.cache.size());
    assertTrue(provider.getSetting("root").getValue() instanceof JsonNode);

    JsonNode node = (JsonNode) provider.getSetting("root.a").getValue();
    assertEquals(JsonNodeType.OBJECT, node.getNodeType());
    assertNotNull(node.get("b"));
    assertEquals(2, provider.cache.size());
    assertSame(node, provider.cache.get("root.a"));

    assertEquals("Hello", provider.getSetting("root.a.b").getValue());
    assertEquals(3, provider.cache.size());
    assertEquals("Hello", provider.cache.get("root.a.b"));

    assertNull(provider.getSetting("root.a.d"));
    assertNull(provider.getSetting("root.array.k"));
    assertEquals(3, provider.cache.size());

    json = "{}";
    writer = new FileWriter(jsonFile, false);
    writer.write(json);
    writer.close();

    provider.reload();
    assertEquals(0, provider.cache.size());
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class PojoTest {
    public String k1;
    public String k2;
  }
}
