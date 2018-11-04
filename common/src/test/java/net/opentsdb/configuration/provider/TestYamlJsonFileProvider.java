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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ YamlJsonFileProvider.class, File.class, Files.class })
public class TestYamlJsonFileProvider {
  private ProviderFactory factory;
  private Configuration config;
  private HashedWheelTimer timer;
  private ByteSource source;
  private File file;
  private HashCode hash;
  
  @Before
  public void before() throws Exception {
    factory = mock(ProviderFactory.class);
    config = mock(Configuration.class);
    timer = mock(HashedWheelTimer.class);
    source = mock(ByteSource.class);
    file = mock(File.class);
    
    when(file.exists()).thenReturn(true);
    
    PowerMockito.whenNew(File.class)
      .withAnyArguments()
      .thenReturn(file);
    
    PowerMockito.mockStatic(Files.class);
    when(Files.asByteSource(any(File.class))).thenReturn(source);
    
    hash = Const.HASH_FUNCTION().hashInt(1);
    when(source.hash(any(HashFunction.class))).thenReturn(hash);
  }
  
  @Test
  public void ctorEmpty() throws Exception {
    String json = "";
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.json");
    
    verify(source, times(1)).openStream();
    assertTrue(provider.cache.isEmpty());
    assertEquals("test.json", provider.file_name);
    assertEquals(hash.asLong(), provider.last_hash);
  }
  
  @Test
  public void ctorEmptyJsonObject() throws Exception {
    String json = "{}";
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.json");
    
    verify(source, times(1)).openStream();
    assertTrue(provider.cache.isEmpty());
    assertEquals("test.json", provider.file_name);
    assertEquals(hash.asLong(), provider.last_hash);
  }
  
  @Test
  public void ctorJsonArray() throws Exception {
    String json = "[]";
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.json");
    
    verify(source, times(1)).openStream();
    assertTrue(provider.cache.isEmpty());
    assertEquals("test.json", provider.file_name);
    assertEquals(hash.asLong(), provider.last_hash);
  }
  
  @Test
  public void ctorException() throws Exception {
    String json = "";
    when(source.hash(any(HashFunction.class))).thenThrow(
        new UnitTestException());
    
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.json");
    
    verify(source, never()).openStream();
    assertTrue(provider.cache.isEmpty());
    assertEquals("test.json", provider.file_name);
    assertEquals(0, provider.last_hash);
  }
  
  @Test
  public void flatJsonObject() throws Exception {
    String json = "{\"key.a\":\"a String\",\"key.b\":null,\"key.c\":"
        + "42.5,\"key.d\":24,\"key.e\":true,\"key.f\":[\"s1\",\"s2\"],"
        + "\"key.g\":{\"k1\":\"v1\",\"k2\":\"v2\"}}";
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.json");
    
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
    String json = "--- \n" + 
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
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.yaml");
    
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
    String json = "{\"root\":{\"a\":{\"b\":\"Hello\",\"c\":\"World\"},"
        + "\"array\":[{\"k\":\"v\"}]}}";
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.json");
    
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
    String yaml = "--- \n" + 
        "root: \n" + 
        "  a: \n" + 
        "    b: Hello\n" + 
        "    c: World\n" + 
        "  array: \n" + 
        "    - \n" + 
        "      k: v\n" + 
        "";
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(yaml.getBytes()));
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.json");
    
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
    String yaml = "--- \n" + 
        "root: \n" + 
        "  a: \n" + 
        "    b: Hello\n" + 
        "    c: 24\n" + 
        "    d: 42.5\n" +
        "    e: true\n" + 
        "    f: ~\n" + 
        "";
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(yaml.getBytes()));
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.json");
    
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
    String json = "{\"key.a\":\"a String\",\"key.b\":null,\"key.c\":"
        + "42.5,\"key.d\":24,\"key.e\":true,\"key.f\":[\"s1\",\"s2\"],"
        + "\"key.g\":{\"k1\":\"v1\",\"k}";
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.json");
    assertTrue(provider.cache.isEmpty());
  }
  
  @Test
  public void reloadFlatSameHash() throws Exception {
    String json = "{\"key.a\":\"a String\",\"key.b\":null,\"key.c\":"
        + "42.5,\"key.d\":24,\"key.e\":true,\"key.f\":[\"s1\",\"s2\"],"
        + "\"key.g\":{\"k1\":\"v1\",\"k2\":\"v2\"}}";
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.json");
    
    assertEquals(6, provider.cache.size());
    verify(source, times(1)).openStream();
    
    provider.reload();
    verify(source, times(1)).openStream();
  }
  
  @Test
  public void reloadFlatChanges() throws Exception {
    String json = "{\"key.a\":\"a String\",\"key.b\":null,\"key.c\":"
        + "42.5,\"key.d\":24,\"key.e\":true,\"key.f\":[\"s1\",\"s2\"],"
        + "\"key.g\":{\"k1\":\"v1\",\"k2\":\"v2\"}}";
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.json");
    
    assertEquals(6, provider.cache.size());
    verify(source, times(1)).openStream();
    
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
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    
    hash = Const.HASH_FUNCTION().hashInt(2);
    when(source.hash(any(HashFunction.class))).thenReturn(hash);
    provider.reload();
    verify(source, times(2)).openStream();
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
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.json");
    
    assertEquals(6, provider.cache.size());
    verify(source, times(1)).openStream();
    
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
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    
    hash = Const.HASH_FUNCTION().hashInt(2);
    when(source.hash(any(HashFunction.class))).thenReturn(hash);
    provider.reload();
    verify(source, times(2)).openStream();
    assertEquals(0, provider.cache.size());
  }
  
  @Test
  public void reloadNested() throws Exception {
    String json = "{\"root\":{\"a\":{\"b\":\"Hello\",\"c\":\"World\"},"
        + "\"array\":[{\"k\":\"v\"}]}}";
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.json");
    
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
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    
    hash = Const.HASH_FUNCTION().hashInt(2);
    when(source.hash(any(HashFunction.class))).thenReturn(hash);
    provider.reload();
    verify(source, times(2)).openStream();
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
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    YamlJsonFileProvider provider = new YamlJsonFileProvider(
        factory, config, timer, "file://test.json");
    
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
    when(source.openStream()).thenReturn(
        new ByteArrayInputStream(json.getBytes()));
    
    hash = Const.HASH_FUNCTION().hashInt(2);
    when(source.hash(any(HashFunction.class))).thenReturn(hash);
    provider.reload();
    verify(source, times(2)).openStream();
    assertEquals(0, provider.cache.size());
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class PojoTest {
    public String k1;
    public String k2;
  }
  
}
