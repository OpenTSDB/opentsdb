// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.type.TypeReference;
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
    assertEquals(map.get("utf"), "aériennes");
    assertEquals(map.get("ascii"), "aariennes");
  }
  
  @Test
  public void parseToObjectStringAsciiString() throws Exception {
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = JSON.parseToObject(
        "{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}", HashMap.class);
    assertEquals(map.get("utf"), "aeriennes");
    assertEquals(map.get("ascii"), "aariennes");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringNull() throws Exception {
    String json = null;
    @SuppressWarnings({ "unused", "unchecked" })
    HashMap<String, String> map = 
      JSON.parseToObject(json, HashMap.class);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringEmpty() throws Exception {
    String json = "";
    @SuppressWarnings({ "unused", "unchecked" })
    HashMap<String, String> map = 
      JSON.parseToObject(json, HashMap.class);
  }
  
  @Test (expected = JsonParseException.class)
  public void parseToObjectStringBad() throws Exception {
    String json = "{\"notgonnafinish";
    @SuppressWarnings({ "unused", "unchecked" })
    HashMap<String, String> map = 
      JSON.parseToObject(json, HashMap.class);
  }
 
  @Test (expected = JsonMappingException.class)
  public void parseToObjectStringBadMap() throws Exception {
    @SuppressWarnings({ "unused", "unchecked" })
    HashSet<String> set = JSON.parseToObject(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}", HashSet.class);
  }

  // parseToObject - Byte && Class
  public void parseToObjectByteUTFString() throws Exception {
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = JSON.parseToObject(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes(), 
        HashMap.class);
    assertEquals(map.get("utf"), "aériennes");
    assertEquals(map.get("ascii"), "aariennes");
  }
  
  @Test
  public void parseToObjectByteString() throws Exception {
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = JSON.parseToObject(
        "{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}".getBytes(), 
        HashMap.class);
    assertEquals(map.get("utf"), "aeriennes");
    assertEquals(map.get("ascii"), "aariennes");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteNull() throws Exception {
    byte[] json = null;
    @SuppressWarnings({ "unused", "unchecked" })
    HashMap<String, String> map = 
      JSON.parseToObject(json, HashMap.class);
  }

  @Test (expected = JsonParseException.class)
  public void parseToObjectByteBad() throws Exception {
    byte[] json = "{\"notgonnafinish".getBytes();
    @SuppressWarnings({ "unused", "unchecked" })
    HashMap<String, String> map = 
      JSON.parseToObject(json, HashMap.class);
  }

  @Test (expected = JsonMappingException.class)
  public void parseToObjectByteBadMap() throws Exception {
    @SuppressWarnings({ "unused", "unchecked" })
    HashSet<String> set = JSON.parseToObject(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes(), 
        HashSet.class);
  }

  //parseToObject - Strings && Type
  @Test
  public void parseToObjectStringTypeUTFString() throws Exception {
    HashMap<String, String> map = JSON.parseToObject(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}", getTRMap());
    assertEquals(map.get("utf"), "aériennes");
    assertEquals(map.get("ascii"), "aariennes");
  }
  
  @Test
  public void parseToObjectStringTypeAsciiString() throws Exception {
    HashMap<String, String> map = JSON.parseToObject(
        "{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}", getTRMap());
    assertEquals(map.get("utf"), "aeriennes");
    assertEquals(map.get("ascii"), "aariennes");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringTypeNull() throws Exception {
    String json = null;
    @SuppressWarnings("unused")
    HashMap<String, String> map = 
      JSON.parseToObject(json, getTRMap());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringTypeEmpty() throws Exception {
    String json = "";
    @SuppressWarnings("unused")
    HashMap<String, String> map = 
      JSON.parseToObject(json, getTRMap());
  }
  
  @Test (expected = JsonParseException.class)
  public void parseToObjectStringTypeBad() throws Exception {
    String json = "{\"notgonnafinish";
    @SuppressWarnings("unused")
    HashMap<String, String> map = 
      JSON.parseToObject(json, getTRMap());
  }

  @Test (expected = JsonMappingException.class)
  public void parseToObjectStringTypeBadMap() throws Exception {
    @SuppressWarnings("unused")
    HashSet<String> set = JSON.parseToObject(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}", getTRSet());
  }

  // parseToObject - Byte && Class
  public void parseToObjectByteTypeUTFString() throws Exception {
    final TypeReference<HashMap<String, String>> tr = 
      new TypeReference<HashMap<String, String>>() {
    };
    HashMap<String, String> map = 
      JSON.parseToObject(
          "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes(), 
          getTRMap());
    assertEquals(map.get("utf"), "aériennes");
    assertEquals(map.get("ascii"), "aariennes");
  }
  
  @Test
  public void parseToObjectByteTypeString() throws Exception {
    HashMap<String, String> map = 
      JSON.parseToObject(
          "{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}".getBytes(), 
          getTRMap());
    assertEquals(map.get("utf"), "aeriennes");
    assertEquals(map.get("ascii"), "aariennes");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteTypeNull() throws Exception {
    byte[] json = null;
    @SuppressWarnings("unused")
    HashMap<String, String> map = 
      JSON.parseToObject(json, getTRMap());
  }

  @Test (expected = JsonParseException.class)
  public void parseToObjectByteTypeBad() throws Exception {
    byte[] json = "{\"notgonnafinish".getBytes();
    @SuppressWarnings("unused")
    HashMap<String, String> map = 
      JSON.parseToObject(json, getTRMap());
  }

  @Test (expected = JsonMappingException.class)
  public void parseToObjectByteTypeBadMap() throws Exception {
    @SuppressWarnings("unused")
    HashSet<String> set = JSON.parseToObject(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes(), 
        getTRSet());
  }

  // parseToStream - String
  @Test
  public void parseToStreamUTFString() throws Exception {
    JsonParser jp = JSON.parseToStream(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}");
    HashMap<String, String> map = this.parseToMap(jp);
    assertEquals(map.get("utf"), "aériennes");
    assertEquals(map.get("ascii"), "aariennes");
  }
  
  @Test
  public void parseToStreamASCIIString() throws Exception {
    JsonParser jp = JSON.parseToStream(
        "{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}");
    HashMap<String, String> map = this.parseToMap(jp);
    assertEquals(map.get("utf"), "aeriennes");
    assertEquals(map.get("ascii"), "aariennes");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamStringNull() throws Exception {
    String json = null;
    @SuppressWarnings("unused")
    JsonParser jp = JSON.parseToStream(json);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamStringEmpty() throws Exception {
    String json = "";
    @SuppressWarnings("unused")
    JsonParser jp = JSON.parseToStream(json);
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
    assertEquals(map.get("utf"), "aériennes");
    assertEquals(map.get("ascii"), "aariennes");
  }
  
  @Test
  public void parseToStreamASCIIByte() throws Exception {
    JsonParser jp = JSON.parseToStream(
        "{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}".getBytes());
    HashMap<String, String> map = this.parseToMap(jp);
    assertEquals(map.get("utf"), "aeriennes");
    assertEquals(map.get("ascii"), "aariennes");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamByteNull() throws Exception {
    byte[] json = null;
    @SuppressWarnings("unused")
    JsonParser jp = JSON.parseToStream(json);
  }

  // parseToStream - Stream
  @Test
  public void parseToStreamUTFSStream() throws Exception {
    InputStream is = new ByteArrayInputStream(
        "{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes("UTF8"));
    HashMap<String, String> map = this.parseToMap(is);
    assertEquals(map.get("utf"), "aériennes");
    assertEquals(map.get("ascii"), "aariennes");
  }
  
  @Test
  public void parseToStreamASCIIStream() throws Exception {
    InputStream is = new ByteArrayInputStream(
        "{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}".getBytes());
    HashMap<String, String> map = this.parseToMap(is);
    assertEquals(map.get("utf"), "aeriennes");
    assertEquals(map.get("ascii"), "aariennes");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamStreamNull() throws Exception {
    InputStream is = null;
    @SuppressWarnings("unused")
    JsonParser jp = JSON.parseToStream(is);
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
    HashMap<String, String> map = null;
    JSON.serializeToString(map);
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
    HashMap<String, String> map = null;
    JSON.serializeToString(map);
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
    HashMap<String, String> map = null;
    JSON.serializeToJSONPString("dummycb", map);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void serializeToJSONStringNullCB() throws Exception {
    HashMap<String, String> map = null;
    String cb = null;
    JSON.serializeToJSONPString(cb, map);
  }

  @Test (expected = IllegalArgumentException.class)
  public void serializeToJSONStringEmptyCB() throws Exception {
    HashMap<String, String> map = null;
    String cb = "";
    JSON.serializeToJSONPString(cb, map);
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
    HashMap<String, String> map = null;
    JSON.serializeToJSONPBytes("dummycb", map);
  }

  @Test (expected = IllegalArgumentException.class)
  public void serializeToJSONPBytesNullCB() throws Exception {
    HashMap<String, String> map = null;
    String cb = null;
    JSON.serializeToJSONPBytes(cb, map);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void serializeToJSONPBytesEmptyCB() throws Exception {
    HashMap<String, String> map = null;
    String cb = "";
    JSON.serializeToJSONPBytes(cb, map);
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
