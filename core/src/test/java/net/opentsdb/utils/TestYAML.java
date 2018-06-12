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
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;

import org.junit.Test;

public final class TestYAML {

  @Test
  public void getMapperNotNull() {
    assertNotNull(YAML.getMapper());
  }
  
  @Test
  public void getFactoryNotNull() {
    assertNotNull(YAML.getFactory());
  }
  
  @Test
  public void mapperAllowNonNumerics() {
    assertTrue(YAML.getMapper().isEnabled(
        JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS));
  }
  
  // parseToObject - Strings && Class
  @Test
  @SuppressWarnings("unchecked")
  public void parseToObjectStringUTFString() throws Exception {
    HashMap<String, String> map = YAML.parseToObject(
        "utf: aériennes\nascii: aariennes", HashMap.class);
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
    
    map = YAML.parseToObject(
        "--- \nutf: aériennes\nascii: aariennes", HashMap.class);
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void parseToObjectStringAsciiString() throws Exception {
    HashMap<String, String> map = YAML.parseToObject(
        "utf: aeriennes\nascii: aariennes", HashMap.class);
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
    
    map = YAML.parseToObject(
        "--- \nutf: aeriennes\nascii: aariennes", HashMap.class);
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringNull() throws Exception {
    YAML.parseToObject((String)null, HashMap.class);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringEmpty() throws Exception {
    YAML.parseToObject("", HashMap.class);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringBad() throws Exception {
    String yaml = "utf: aeriennes\\nascii:";
    YAML.parseToObject(yaml, HashMap.class);
  }
 
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringBadMap() throws Exception {
    YAML.parseToObject(
        "utf: aeriennes\nascii: aariennes", HashSet.class);
  }

  // parseToObject - Byte && Class
  public void parseToObjectByteUTFString() throws Exception {
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = YAML.parseToObject(
        "utf: aériennes\nascii: aariennes".getBytes(), 
        HashMap.class);
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test
  public void parseToObjectByteString() throws Exception {
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = YAML.parseToObject(
        "utf: aeriennes\nascii: aariennes".getBytes(), 
        HashMap.class);
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteNull() throws Exception {
    byte[] yaml = null;
    YAML.parseToObject(yaml, HashMap.class);
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteBad() throws Exception {
    byte[] yaml = "utf: aeriennes\nascii".getBytes();
    YAML.parseToObject(yaml, HashMap.class);
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteBadMap() throws Exception {
    YAML.parseToObject(
        "utf: aeriennes\nascii: aariennes".getBytes(), 
        HashSet.class);
  }

  //parseToObject - Strings && Type
  @Test
  public void parseToObjectStringTypeUTFString() throws Exception {
    HashMap<String, String> map = YAML.parseToObject(
        "utf: aériennes\nascii: aariennes", getTRMap());
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test
  public void parseToObjectStringTypeAsciiString() throws Exception {
    HashMap<String, String> map = YAML.parseToObject(
        "utf: aeriennes\nascii: aariennes", getTRMap());
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringTypeNull() throws Exception {
    YAML.parseToObject((String)null, getTRMap());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringTypeEmpty() throws Exception {
    YAML.parseToObject("", getTRMap());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringTypeBad() throws Exception {
    YAML.parseToObject("sutf: aeriennes\nascii", getTRMap());
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringTypeBadMap() throws Exception {
    YAML.parseToObject(
        "utf: aeriennes\nascii: aariennes", getTRSet());
  }

  // parseToObject - Byte && Class
  public void parseToObjectByteTypeUTFString() throws Exception {
    HashMap<String, String> map = 
      YAML.parseToObject(
          "utf: aériennes\nascii: aariennes".getBytes(), 
          getTRMap());
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test
  public void parseToObjectByteTypeString() throws Exception {
    HashMap<String, String> map = 
      YAML.parseToObject(
          "utf: aeriennes\nascii: aariennes".getBytes(), 
          getTRMap());
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteTypeNull() throws Exception {
    YAML.parseToObject((byte[])null, getTRMap());
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteTypeBad() throws Exception {
    byte[] yaml = "utf: aeriennes\nascii".getBytes();
    YAML.parseToObject(yaml, getTRMap());
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteTypeBadMap() throws Exception {
    YAML.parseToObject(
        "utf: aeriennes\nascii: aariennes".getBytes(), 
        getTRSet());
  }

  // parseToStream - String
  @Test
  public void parseToStreamUTFString() throws Exception {
    YAMLParser jp = YAML.parseToStream(
        "utf: aériennes\nascii: aariennes");
    HashMap<String, String> map = this.parseToMap(jp);
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }

  @Test
  public void parseToStreamASCIIString() throws Exception {
    YAMLParser jp = YAML.parseToStream(
        "utf: aeriennes\nascii: aariennes");
    HashMap<String, String> map = this.parseToMap(jp);
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamStringNull() throws Exception {
    YAML.parseToStream((String)null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamStringEmpty() throws Exception {
    YAML.parseToStream("");
  }

  @Test
  public void parseToStreamStringUnfinished() throws Exception {
    String yaml = "utf: aeriennes\nascii";
    YAMLParser jp = YAML.parseToStream(yaml);
    assertNotNull(jp);
  }
  
  // parseToStream - Byte
  @Test
  public void parseToStreamUTFSByte() throws Exception {
    YAMLParser jp = YAML.parseToStream(
        "utf: aériennes\nascii: aariennes".getBytes("UTF8"));
    HashMap<String, String> map = this.parseToMap(jp);
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test
  public void parseToStreamASCIIByte() throws Exception {
    YAMLParser jp = YAML.parseToStream(
        "utf: aeriennes\nascii: aariennes".getBytes());
    HashMap<String, String> map = this.parseToMap(jp);
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamByteNull() throws Exception {
    YAML.parseToStream((byte[])null);
  }

  // parseToStream - Stream
  @Test
  public void parseToStreamUTFSStream() throws Exception {
    InputStream is = new ByteArrayInputStream(
        "utf: aériennes\nascii: aariennes".getBytes("UTF8"));
    HashMap<String, String> map = this.parseToMap(is);
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test
  public void parseToStreamASCIIStream() throws Exception {
    InputStream is = new ByteArrayInputStream(
        "utf: aeriennes\nascii: aariennes".getBytes());
    HashMap<String, String> map = this.parseToMap(is);
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamStreamNull() throws Exception {
    YAML.parseToStream((InputStream)null);
  }

  // serializeToString
  @Test
  public void serializeToString() throws Exception {
    HashMap<String, String> map = new HashMap<String, String>();
    map.put("utf", "aériennes");
    map.put("ascii", "aariennes");
    String yaml = YAML.serializeToString(map);
    assertTrue(yaml.startsWith("---"));
    assertTrue(yaml.contains("utf: \"aériennes\""));
    assertTrue(yaml.contains("ascii: \"aariennes\""));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void serializeToStringNull() throws Exception {
    YAML.serializeToString((HashMap<String, String>)null);
  }

  // serializeToBytes
  @Test
  public void serializeToBytes() throws Exception {
    HashMap<String, String> map = new HashMap<String, String>();
    map.put("utf", "aériennes");
    map.put("ascii", "aariennes");
    byte[] raw = YAML.serializeToBytes(map);
    assertNotNull(raw);
    String yaml = new String(raw, "UTF8");
    assertTrue(yaml.startsWith("---"));
    assertTrue(yaml.contains("utf: \"aériennes\""));
    assertTrue(yaml.contains("ascii: \"aariennes\""));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void serializeToBytesNull() throws Exception {
    YAML.serializeToString((HashMap<String, String>)null);
  }
  
  /** Helper to parse an input stream into a map */
  private HashMap<String, String> parseToMap(final InputStream is) 
    throws Exception {
    YAMLParser node = YAML.parseToStream(is);
    HashMap<String, String> map = new HashMap<String, String>();
    String field = "";
    String value;
    while (node.nextToken() != null) {
      if (node.getCurrentToken() == JsonToken.FIELD_NAME && 
          node.getCurrentName() != null) {
        field = node.getCurrentName();
      } else if (node.getCurrentToken() == JsonToken.VALUE_STRING) {
        value = node.getText();
        map.put(field, value);
      }        
    }
    return map;
  }
  
  /** Helper to parse an input stream into a map */
  private HashMap<String, String> parseToMap(final YAMLParser node) 
    throws Exception {
    HashMap<String, String> map = new HashMap<String, String>();
    String field = "";
    String value;
    while (node.nextToken() != null) {
      if (node.getCurrentToken() == JsonToken.FIELD_NAME && 
          node.getCurrentName() != null) {
        field = node.getCurrentName();
      } else if (node.getCurrentToken() == JsonToken.VALUE_STRING) {
        value = node.getText();
        map.put(field, value);
      }        
    }
    return map;
  }
  
  /** Helper to return a TypeReference for a Hash Map */
  private final TypeReference<HashMap<String, String>> getTRMap(){
    return new TypeReference<HashMap<String, String>>() {};
  }
  
  /** Helper to return a TypeReference for a Hash Set */
  private final TypeReference<HashSet<String>> getTRSet(){
    return new TypeReference<HashSet<String>>() {};
  }
}