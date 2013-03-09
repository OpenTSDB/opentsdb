package net.opentsdb.utils;

import static org.junit.Assert.*;

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
  public void getMapperNotNull(){
    assertNotNull(JSON.getMapper());
  }
  
  @Test
  public void getFactoryNotNull(){
    assertNotNull(JSON.getFactory());
  }
  
  @Test
  public void mapperAllowNonNumerics(){
    assertTrue(JSON.getMapper().isEnabled(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS));
  }
  
  // parseToObject - Strings && Class
  @Test
  public void parseToObjectStringUTFString() throws Exception {
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}", HashMap.class);
    assertTrue(map.get("utf").equals("aériennes"));
    assertTrue(map.get("ascii").equals("aariennes"));
  }
  
  @Test
  public void parseToObjectStringAsciiString() throws Exception {
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject("{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}", HashMap.class);
    assertTrue(map.get("utf").equals("aeriennes"));
    assertTrue(map.get("ascii").equals("aariennes"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringNull() throws Exception {
    String json = null;
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject(json, HashMap.class);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringEmpty() throws Exception {
    String json = "";
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject(json, HashMap.class);
  }
  
  @Test (expected = JsonParseException.class)
  public void parseToObjectStringBad() throws Exception {
    String json = "{\"notgonnafinish";
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject(json, HashMap.class);
  }

  @Test (expected = ClassCastException.class)
  public void parseToObjectStringBadCast() throws Exception {
    @SuppressWarnings("unchecked")
    HashSet<String> set = (HashSet<String>) JSON.parseToObject("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}", HashMap.class);
  }
  
  @Test (expected = JsonMappingException.class)
  public void parseToObjectStringBadMap() throws Exception {
    @SuppressWarnings("unchecked")
    HashSet<String> set = (HashSet<String>) JSON.parseToObject("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}", HashSet.class);
  }

  // parseToObject - Byte && Class
  public void parseToObjectByteUTFString() throws Exception {
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes(), HashMap.class);
    assertTrue(map.get("utf").equals("aériennes"));
    assertTrue(map.get("ascii").equals("aariennes"));
  }
  
  @Test
  public void parseToObjectByteString() throws Exception {
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject("{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}".getBytes(), HashMap.class);
    assertTrue(map.get("utf").equals("aeriennes"));
    assertTrue(map.get("ascii").equals("aariennes"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteNull() throws Exception {
    byte[] json = null;
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject(json, HashMap.class);
  }

  @Test (expected = JsonParseException.class)
  public void parseToObjectByteBad() throws Exception {
    byte[] json = "{\"notgonnafinish".getBytes();
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject(json, HashMap.class);
  }

  @Test (expected = ClassCastException.class)
  public void parseToObjectByteBadCast() throws Exception {
    @SuppressWarnings("unchecked")
    HashSet<String> set = (HashSet<String>) JSON.parseToObject("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes("UTF-8"), HashMap.class);
  }
  
//  @Test (expected = JsonParseException.class)
//  public void parseToObjectByteBadEncoding() throws Exception {
//    @SuppressWarnings("unchecked")
//    HashSet<String> set = (HashSet<String>) JSON.parseToObject("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes(), HashMap.class);
//  }
  
  @Test (expected = JsonMappingException.class)
  public void parseToObjectByteBadMap() throws Exception {
    @SuppressWarnings("unchecked")
    HashSet<String> set = (HashSet<String>) JSON.parseToObject("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes(), HashSet.class);
  }

  //parseToObject - Strings && Type
  @Test
  public void parseToObjectStringTypeUTFString() throws Exception {
    final TypeReference<HashMap<String, String>> tr = 
      new TypeReference<HashMap<String, String>>() {
    };
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}", tr);
    assertTrue(map.get("utf").equals("aériennes"));
    assertTrue(map.get("ascii").equals("aariennes"));
  }
  
  @Test
  public void parseToObjectStringTypeAsciiString() throws Exception {
    final TypeReference<HashMap<String, String>> tr = 
      new TypeReference<HashMap<String, String>>() {
    };
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject("{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}", tr);
    assertTrue(map.get("utf").equals("aeriennes"));
    assertTrue(map.get("ascii").equals("aariennes"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringTypeNull() throws Exception {
    final TypeReference<HashMap<String, String>> tr = 
      new TypeReference<HashMap<String, String>>() {
    };
    String json = null;
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject(json, tr);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectStringTypeEmpty() throws Exception {
    final TypeReference<HashMap<String, String>> tr = 
      new TypeReference<HashMap<String, String>>() {
    };
    String json = "";
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject(json, tr);
  }
  
  @Test (expected = JsonParseException.class)
  public void parseToObjectStringTypeBad() throws Exception {
    final TypeReference<HashMap<String, String>> tr = 
      new TypeReference<HashMap<String, String>>() {
    };
    String json = "{\"notgonnafinish";
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject(json, tr);
  }

  @Test (expected = ClassCastException.class)
  public void parseToObjectStringTypeBadCast() throws Exception {
    final TypeReference<HashMap<String, String>> tr = 
      new TypeReference<HashMap<String, String>>() {
    };
    @SuppressWarnings("unchecked")
    HashSet<String> set = (HashSet<String>) JSON.parseToObject("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}", tr);
  }
  
  @Test (expected = JsonMappingException.class)
  public void parseToObjectStringTypeBadMap() throws Exception {
    final TypeReference<HashSet<String>> tr = 
      new TypeReference<HashSet<String>>() {
    };
    @SuppressWarnings("unchecked")
    HashSet<String> set = (HashSet<String>) JSON.parseToObject("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}", tr);
  }

  // parseToObject - Byte && Class
  public void parseToObjectByteTypeUTFString() throws Exception {
    final TypeReference<HashMap<String, String>> tr = 
      new TypeReference<HashMap<String, String>>() {
    };
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes(), tr);
    assertTrue(map.get("utf").equals("aériennes"));
    assertTrue(map.get("ascii").equals("aariennes"));
  }
  
  @Test
  public void parseToObjectByteTypeString() throws Exception {
    final TypeReference<HashMap<String, String>> tr = 
      new TypeReference<HashMap<String, String>>() {
    };
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject("{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}".getBytes(), tr);
    assertTrue(map.get("utf").equals("aeriennes"));
    assertTrue(map.get("ascii").equals("aariennes"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToObjectByteTypeNull() throws Exception {
    final TypeReference<HashMap<String, String>> tr = 
      new TypeReference<HashMap<String, String>>() {
    };
    byte[] json = null;
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject(json, tr);
  }

  @Test (expected = JsonParseException.class)
  public void parseToObjectByteTypeBad() throws Exception {
    final TypeReference<HashMap<String, String>> tr = 
      new TypeReference<HashMap<String, String>>() {
    };
    byte[] json = "{\"notgonnafinish".getBytes();
    @SuppressWarnings("unchecked")
    HashMap<String, String> map = (HashMap<String, String>) JSON.parseToObject(json, tr);
  }

  @Test (expected = ClassCastException.class)
  public void parseToObjectByteTypeBadCast() throws Exception {
    final TypeReference<HashMap<String, String>> tr = 
      new TypeReference<HashMap<String, String>>() {
    };
    @SuppressWarnings("unchecked")
    HashSet<String> set = (HashSet<String>) JSON.parseToObject("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes("UTF-8"), tr);
  }
  
//  @Test (expected = JsonParseException.class)
//  public void parseToObjectByteBadTypeEncoding() throws Exception {
//    final TypeReference<HashMap<String, String>> tr = 
//      new TypeReference<HashMap<String, String>>() {
//    };
//    @SuppressWarnings("unchecked")
//    HashSet<String> set = (HashSet<String>) JSON.parseToObject("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes(), tr);
//  }
  
  @Test (expected = JsonMappingException.class)
  public void parseToObjectByteTypeBadMap() throws Exception {
    final TypeReference<HashSet<String>> tr = 
      new TypeReference<HashSet<String>>() {
    };
    @SuppressWarnings("unchecked")
    HashSet<String> set = (HashSet<String>) JSON.parseToObject("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes(), tr);
  }

  // parseToStream - String
  @Test
  public void parseToStreamUTFString() throws Exception {
    JsonParser jp = JSON.parseToStream("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}");
    assertNotNull(jp);
    
    HashMap<String, String> map = new HashMap<String, String>();
    String field = "";
    String value;
    while (jp.nextToken() != null) {
      if (jp.getCurrentToken() == JsonToken.FIELD_NAME && 
          jp.getCurrentName() != null)
        field = jp.getCurrentName();
      else if (jp.getCurrentToken() == JsonToken.VALUE_STRING){
        value = jp.getText();
        map.put(field, value);
      }        
    }
    
    assertTrue(map.get("utf").equals("aériennes"));
    assertTrue(map.get("ascii").equals("aariennes"));
  }
  
  @Test
  public void parseToStreamASCIIString() throws Exception {
    JsonParser jp = JSON.parseToStream("{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}");
    assertNotNull(jp);
    
    HashMap<String, String> map = new HashMap<String, String>();
    String field = "";
    String value;
    while (jp.nextToken() != null) {
      if (jp.getCurrentToken() == JsonToken.FIELD_NAME && 
          jp.getCurrentName() != null)
        field = jp.getCurrentName();
      else if (jp.getCurrentToken() == JsonToken.VALUE_STRING){
        value = jp.getText();
        map.put(field, value);
      }        
    }
    
    assertTrue(map.get("utf").equals("aeriennes"));
    assertTrue(map.get("ascii").equals("aariennes"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamStringNull() throws Exception {
    String json = null;
    JsonParser jp = JSON.parseToStream(json);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamStringEmpty() throws Exception {
    String json = "";
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
    JsonParser jp = JSON.parseToStream("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes("UTF8"));
    assertNotNull(jp);
    
    HashMap<String, String> map = new HashMap<String, String>();
    String field = "";
    String value;
    while (jp.nextToken() != null) {
      if (jp.getCurrentToken() == JsonToken.FIELD_NAME && 
          jp.getCurrentName() != null)
        field = jp.getCurrentName();
      else if (jp.getCurrentToken() == JsonToken.VALUE_STRING){
        value = jp.getText();
        map.put(field, value);
      }        
    }
    
    assertTrue(map.get("utf").equals("aériennes"));
    assertTrue(map.get("ascii").equals("aariennes"));
  }
  
  @Test
  public void parseToStreamASCIIByte() throws Exception {
    JsonParser jp = JSON.parseToStream("{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}".getBytes());
    assertNotNull(jp);
    
    HashMap<String, String> map = new HashMap<String, String>();
    String field = "";
    String value;
    while (jp.nextToken() != null) {
      if (jp.getCurrentToken() == JsonToken.FIELD_NAME && 
          jp.getCurrentName() != null)
        field = jp.getCurrentName();
      else if (jp.getCurrentToken() == JsonToken.VALUE_STRING){
        value = jp.getText();
        map.put(field, value);
      }        
    }
    
    assertTrue(map.get("utf").equals("aeriennes"));
    assertTrue(map.get("ascii").equals("aariennes"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamByteNull() throws Exception {
    byte[] json = null;
    JsonParser jp = JSON.parseToStream(json);
  }

  // parseToStream - Stream
  @Test
  public void parseToStreamUTFSStream() throws Exception {
    InputStream is = new ByteArrayInputStream("{\"utf\":\"aériennes\",\"ascii\":\"aariennes\"}".getBytes("UTF8"));
    JsonParser jp = JSON.parseToStream(is);
    assertNotNull(jp);
    
    HashMap<String, String> map = new HashMap<String, String>();
    String field = "";
    String value;
    while (jp.nextToken() != null) {
      if (jp.getCurrentToken() == JsonToken.FIELD_NAME && 
          jp.getCurrentName() != null)
        field = jp.getCurrentName();
      else if (jp.getCurrentToken() == JsonToken.VALUE_STRING){
        value = jp.getText();
        map.put(field, value);
      }        
    }
    
    assertTrue(map.get("utf").equals("aériennes"));
    assertTrue(map.get("ascii").equals("aariennes"));
  }
  
  @Test
  public void parseToStreamASCIIStream() throws Exception {
    InputStream is = new ByteArrayInputStream("{\"utf\":\"aeriennes\",\"ascii\":\"aariennes\"}".getBytes());
    JsonParser jp = JSON.parseToStream(is);
    assertNotNull(jp);
    
    HashMap<String, String> map = new HashMap<String, String>();
    String field = "";
    String value;
    while (jp.nextToken() != null) {
      if (jp.getCurrentToken() == JsonToken.FIELD_NAME && 
          jp.getCurrentName() != null)
        field = jp.getCurrentName();
      else if (jp.getCurrentToken() == JsonToken.VALUE_STRING){
        value = jp.getText();
        map.put(field, value);
      }        
    }
    
    assertTrue(map.get("utf").equals("aeriennes"));
    assertTrue(map.get("ascii").equals("aariennes"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseToStreamStreamNull() throws Exception {
    InputStream is = null;
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
}
