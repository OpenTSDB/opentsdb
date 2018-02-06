// This file is part of OpenTSDB.
// Copyright (C) 2013-2017  The OpenTSDB Authors.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Test;

public final class TestJSON {

  @Test
  public void getMapperNotNull() {
    assertNotNull(JSON.getMapper());
  }
  
  @Test
  public void getFactoryNotNull() {
    assertNotNull(JSON.getFactory());
  }
  
  @Test
  public void mapperAllowNonNumerics() {
    assertTrue(JSON.getMapper().isEnabled(
        JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS));
  }
  
  // parseToObject - Strings && Class
  @Test
  public void parseToObjectStringUTFString() throws Exception {
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = JSON.parseToObject(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}", HashMap.class);
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test
  public void parseToObjectStringAsciiString() throws Exception {
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = JSON.parseToObject(
        "{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}", HashMap.class);
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringNull() throws Exception {
    JSON.parseToObject((String)null, HashMap.class);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringEmpty() throws Exception {
    JSON.parseToObject("", HashMap.class);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringBad() throws Exception {
    String json = "{\"notgonnafinish";
    JSON.parseToObject(json, HashMap.class);
  }
 
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringBadMap() throws Exception {
    JSON.parseToObject(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}", HashSet.class);
  }

  // parseToObject - Byte && Class
  public void parseToObjectByteUTFString() throws Exception {
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = JSON.parseToObject(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes(), 
        HashMap.class);
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test
  public void parseToObjectByteString() throws Exception {
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = JSON.parseToObject(
        "{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}".getBytes(), 
        HashMap.class);
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteNull() throws Exception {
    byte[] json = null;
    JSON.parseToObject(json, HashMap.class);
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteBad() throws Exception {
    byte[] json = "{\"notgonnafinish".getBytes();
    JSON.parseToObject(json, HashMap.class);
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteBadMap() throws Exception {
    JSON.parseToObject(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes(), 
        HashSet.class);
  }

  //parseToObject - Strings && Type
  @Test
  public void parseToObjectStringTypeUTFString() throws Exception {
    HashMap<String, String> map = JSON.parseToObject(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}", getTRMap());
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test
  public void parseToObjectStringTypeAsciiString() throws Exception {
    HashMap<String, String> map = JSON.parseToObject(
        "{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}", getTRMap());
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringTypeNull() throws Exception {
    JSON.parseToObject((String)null, getTRMap());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringTypeEmpty() throws Exception {
    JSON.parseToObject("", getTRMap());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringTypeBad() throws Exception {
    JSON.parseToObject("{\"notgonnafinish", getTRMap());
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringTypeBadMap() throws Exception {
    JSON.parseToObject(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}", getTRSet());
  }

  // parseToObject - Byte && Class
  public void parseToObjectByteTypeUTFString() throws Exception {
    HashMap<String, String> map = 
      JSON.parseToObject(
          "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes(), 
          getTRMap());
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test
  public void parseToObjectByteTypeString() throws Exception {
    HashMap<String, String> map = 
      JSON.parseToObject(
          "{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}".getBytes(), 
          getTRMap());
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteTypeNull() throws Exception {
    JSON.parseToObject((byte[])null, getTRMap());
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteTypeBad() throws Exception {
    byte[] json = "{\"notgonnafinish".getBytes();
    JSON.parseToObject(json, getTRMap());
  }

  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteTypeBadMap() throws Exception {
    JSON.parseToObject(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes(), 
        getTRSet());
  }

  // parseToStream - String
  @Test
  public void parseToStreamUTFString() throws Exception {
    JsonParser jp = JSON.parseToStream(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}");
    HashMap<String, String> map = this.parseToMap(jp);
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test
  public void parseToStreamASCIIString() throws Exception {
    JsonParser jp = JSON.parseToStream(
        "{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}");
    HashMap<String, String> map = this.parseToMap(jp);
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamStringNull() throws Exception {
    JSON.parseToStream((String)null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamStringEmpty() throws Exception {
    JSON.parseToStream("");
  }

  @Test
  public void parseToStreamStringUnfinished() throws Exception {
    String json = "{\"notgonnafinish";
    JsonParser jp = JSON.parseToStream(json);
    assertNotNull(jp);
  }
  
  // parseToStream - Byte
  @Test
  public void parseToStreamUTFSByte() throws Exception {
    JsonParser jp = JSON.parseToStream(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes("UTF8"));
    HashMap<String, String> map = this.parseToMap(jp);
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test
  public void parseToStreamASCIIByte() throws Exception {
    JsonParser jp = JSON.parseToStream(
        "{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}".getBytes());
    HashMap<String, String> map = this.parseToMap(jp);
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamByteNull() throws Exception {
    JSON.parseToStream((byte[])null);
  }

  // parseToStream - Stream
  @Test
  public void parseToStreamUTFSStream() throws Exception {
    InputStream is = new ByteArrayInputStream(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes("UTF8"));
    HashMap<String, String> map = this.parseToMap(is);
    assertEquals("aériennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test
  public void parseToStreamASCIIStream() throws Exception {
    InputStream is = new ByteArrayInputStream(
        "{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}".getBytes());
    HashMap<String, String> map = this.parseToMap(is);
    assertEquals("aeriennes", map.get("utf"));
    assertEquals("aariennes", map.get("ascii"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamStreamNull() throws Exception {
    JSON.parseToStream((InputStream)null);
  }

  // serializeToString
  @Test
  public void serializeToString() throws Exception {
    HashMap<String, String> map = new HashMap<String, String>();
    map.put("utf", "aériennes");
    map.put("ascii", "aariennes");
    String json = JSON.serializeToString(map);
    assertNotNull(json);
    assertFalse(json.isEmpty());
    assertTrue(json.matches(".*[{,]\"ascii\":\"aariennes\"[,}].*"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void serializeToStringNull() throws Exception {
    JSON.serializeToString((HashMap<String, String>)null);
  }

  // serializeToBytes
  @Test
  public void serializeToBytes() throws Exception {
    HashMap<String, String> map = new HashMap<String, String>();
    map.put("utf", "aériennes");
    map.put("ascii", "aariennes");
    byte[] raw = JSON.serializeToBytes(map);
    assertNotNull(raw);
    String json = new String(raw, "UTF8");
    assertTrue(json.matches(".*[{,]\"ascii\":\"aariennes\"[,}].*"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void serializeToBytesNull() throws Exception {
    JSON.serializeToString((HashMap<String, String>)null);
  }

  // serializeToJSONString
  @Test
  public void serializeToJSONString() throws Exception {
    HashMap<String, String> map = new HashMap<String, String>();
    map.put("utf", "aériennes");
    map.put("ascii", "aariennes");
    String json = JSON.serializeToJSONPString("dummycb", map);
    assertNotNull(json);
    assertFalse(json.isEmpty());
    assertTrue(json.matches("dummycb\\(.*[{,]\"ascii\":\"aariennes\"[,}].*\\)"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void serializeToJSONStringNullData() throws Exception {
    JSON.serializeToJSONPString("dummycb", (HashMap<String, String>)null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void serializeToJSONStringNullCB() throws Exception {
    JSON.serializeToJSONPString((String)null, (HashMap<String, String>)null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void serializeToJSONStringEmptyCB() throws Exception {
    JSON.serializeToJSONPString("", (HashMap<String, String>)null);
  }
  
  // serializeToJSONPBytes
  @Test
  public void serializeToJSONPBytes() throws Exception {
    HashMap<String, String> map = new HashMap<String, String>();
    map.put("utf", "aériennes");
    map.put("ascii", "aariennes");
    byte[] raw = JSON.serializeToJSONPBytes("dummycb", map);
    assertNotNull(raw);
    String json = new String(raw, "UTF8");
    assertTrue(json.matches("dummycb\\(.*[{,]\"ascii\":\"aariennes\"[,}].*\\)"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void serializeToJSONPBytesNullData() throws Exception {
    JSON.serializeToJSONPBytes("dummycb", (HashMap<String, String>)null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void serializeToJSONPBytesNullCB() throws Exception {
    JSON.serializeToJSONPBytes((String)null, (HashMap<String, String>)null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void serializeToJSONPBytesEmptyCB() throws Exception {
    JSON.serializeToJSONPBytes("", (HashMap<String, String>)null);
  }

  /** Helper to parse an input stream into a map */
  private HashMap<String, String> parseToMap(final InputStream is) 
    throws Exception {
    JsonParser jp = JSON.parseToStream(is);
    HashMap<String, String> map = new HashMap<String, String>();
    String field = "";
    String value;
    while (jp.nextToken() != null) {
      if (jp.getCurrentToken() == JsonToken.FIELD_NAME && 
          jp.getCurrentName() != null) {
        field = jp.getCurrentName();
      } else if (jp.getCurrentToken() == JsonToken.VALUE_STRING) {
        value = jp.getText();
        map.put(field, value);
      }        
    }
    return map;
  }
  
  /** Helper to parse an input stream into a map */
  private HashMap<String, String> parseToMap(final JsonParser jp) 
    throws Exception {
    HashMap<String, String> map = new HashMap<String, String>();
    String field = "";
    String value;
    while (jp.nextToken() != null) {
      if (jp.getCurrentToken() == JsonToken.FIELD_NAME && 
          jp.getCurrentName() != null) {
        field = jp.getCurrentName();
      } else if (jp.getCurrentToken() == JsonToken.VALUE_STRING) {
        value = jp.getText();
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
