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

import java.io.IOException;
import java.io.InputStream;

import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;

/**
 * This class simply provides a static initialization and configuration of the
 * Jackson ObjectMapper for use throughout OpenTSDB. Since the mapper takes a
 * fair amount of construction and is thread safe, the Jackson docs recommend
 * initializing it once per app.
 * <p>
 * The class also provides some simple wrappers around commonly used
 * serialization and deserialization methods for POJOs as well as a JSONP
 * wrapper. These work wonderfully for smaller objects and you can use JAVA
 * annotations to control the de/serialization for your POJO class.
 * <p>
 * For streaming of large objects, access the mapper directly via {@link
 * getMapper()} or {@link getFactory()}
 * <p>
 * Unfortunately since Jackson provides typed exceptions, most of these
 * methods will pass them along so you'll have to handle them where
 * you are making a call.
 * <p>
 * Troubleshooting POJO de/serialization:
 * <p>
 * If you get mapping errors, check some of these 
 * <ul><li>The class must provide a constructor without parameters</li> 
 * <li>Make sure fields are accessible via getters/setters or by the 
 * {@link @JsonAutoDetect} annotation</li>
 * <li>Make sure any child objects are accessible, have the empty constructor 
 * and applicable annotations</li></ul>
 * <p>
 * Useful Class Annotations:
 * @JsonAutoDetect(fieldVisibility = Visibility.ANY) - will serialize any,
 *   public or private values
 * <p>
 * @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL) - will 
 *   automatically ignore any fields set to NULL, otherwise they are serialized
 *   with a literal null value
 * <p>
 * Useful Method Annotations:
 * @JsonIgnore - Ignores the method for de/serialization purposes. CRITICAL for
 *   any methods that could cause a de/serialization infinite loop
 * @since 2.0
 */
public final class JSON {
  /**
   * Jackson de/serializer initialized, configured and shared
   */
  private static final ObjectMapper jsonMapper = new ObjectMapper();
  static {
    // allows parsing NAN and such without throwing an exception. This is
    // important
    // for incoming data points with multiple points per put so that we can
    // toss only the bad ones but keep the good
    jsonMapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
  }

  /**
   * Deserializes a JSON formatted string to a specific class type
   * <b>Note:</b> If you get mapping exceptions you may need to provide a 
   * TypeReference
   * @param json The string to deserialize
   * @param pojo The class type of the object used for deserialization
   * @return An object of the {@link pojo} type
   * @throws IllegalArgumentException if the data or class was null or parsing 
   * failed
   * @throws JSONException if the data could not be parsed
   */
  public static final <T> T parseToObject(final String json,
      final Class<T> pojo) {
    if (json == null || json.isEmpty())
      throw new IllegalArgumentException("Incoming data was null or empty");
    if (pojo == null)
      throw new IllegalArgumentException("Missing class type");
    
    try {
      return jsonMapper.readValue(json, pojo);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new JSONException(e);
    }
  }

  /**
   * Deserializes a JSON formatted byte array to a specific class type
   * <b>Note:</b> If you get mapping exceptions you may need to provide a 
   * TypeReference
   * @param json The byte array to deserialize
   * @param pojo The class type of the object used for deserialization
   * @return An object of the {@link pojo} type
   * @throws IllegalArgumentException if the data or class was null or parsing 
   * failed
   * @throws JSONException if the data could not be parsed
   */
  public static final <T> T parseToObject(final byte[] json,
      final Class<T> pojo) {
    if (json == null)
      throw new IllegalArgumentException("Incoming data was null");
    if (pojo == null)
      throw new IllegalArgumentException("Missing class type");
    try {
      return jsonMapper.readValue(json, pojo);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new JSONException(e);
    }
  }

  /**
   * Deserializes a JSON formatted string to a specific class type
   * @param json The string to deserialize
   * @param type A type definition for a complex object
   * @return An object of the {@link pojo} type
   * @throws IllegalArgumentException if the data or type was null or parsing
   * failed
   * @throws JSONException if the data could not be parsed
   */
  @SuppressWarnings("unchecked")
  public static final <T> T parseToObject(final String json,
      final TypeReference<T> type) {
    if (json == null || json.isEmpty())
      throw new IllegalArgumentException("Incoming data was null or empty");
    if (type == null)
      throw new IllegalArgumentException("Missing type reference");
    try {
      return (T)jsonMapper.readValue(json, type);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new JSONException(e);
    }
  }

  /**
   * Deserializes a JSON formatted byte array to a specific class type
   * @param json The byte array to deserialize
   * @param type A type definition for a complex object
   * @return An object of the {@link pojo} type
   * @throws IllegalArgumentException if the data or type was null or parsing
   * failed
   * @throws JSONException if the data could not be parsed
   */
  @SuppressWarnings("unchecked")
  public static final <T> T parseToObject(final byte[] json,
      final TypeReference<T> type) {
    if (json == null)
      throw new IllegalArgumentException("Incoming data was null");
    if (type == null)
      throw new IllegalArgumentException("Missing type reference");
    try {
      return (T)jsonMapper.readValue(json, type);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new JSONException(e);
    }
  }

  /**
   * Parses a JSON formatted string into raw tokens for streaming or tree
   * iteration
   * <b>Warning:</b> This method can parse an invalid JSON object without
   * throwing an error until you start processing the data
   * @param json The string to parse
   * @return A JsonParser object to be used for iteration
   * @throws IllegalArgumentException if the data was null or parsing failed
   * @throws JSONException if the data could not be parsed
   */
  public static final JsonParser parseToStream(final String json) {
    if (json == null || json.isEmpty())
      throw new IllegalArgumentException("Incoming data was null or empty");
    try {
      return jsonMapper.getFactory().createJsonParser(json);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new JSONException(e);
    }
  }

  /**
   * Parses a JSON formatted byte array into raw tokens for streaming or tree
   * iteration
   * <b>Warning:</b> This method can parse an invalid JSON object without
   * throwing an error until you start processing the data
   * @param json The byte array to parse
   * @return A JsonParser object to be used for iteration
   * @throws IllegalArgumentException if the data was null or parsing failed
   * @throws JSONException if the data could not be parsed
   */
  public static final JsonParser parseToStream(final byte[] json) {
    if (json == null)
      throw new IllegalArgumentException("Incoming data was null");
    try {
      return jsonMapper.getFactory().createJsonParser(json);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new JSONException(e);
    }
  }

  /**
   * Parses a JSON formatted inputs stream into raw tokens for streaming or tree
   * iteration
   * <b>Warning:</b> This method can parse an invalid JSON object without
   * throwing an error until you start processing the data
   * @param json The input stream to parse
   * @return A JsonParser object to be used for iteration
   * @throws IllegalArgumentException if the data was null or parsing failed
   * @throws JSONException if the data could not be parsed
   */
  public static final JsonParser parseToStream(final InputStream json) {
    if (json == null)
      throw new IllegalArgumentException("Incoming data was null");
    try {
      return jsonMapper.getFactory().createJsonParser(json);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new JSONException(e);
    }
  }

  /**
   * Serializes the given object to a JSON string
   * @param object The object to serialize
   * @return A JSON formatted string
   * @throws IllegalArgumentException if the object was null
   * @throws JSONException if the object could not be serialized
   * @throws IOException Thrown when there was an issue reading the object
   */
  public static final String serializeToString(final Object object) {
    if (object == null)
      throw new IllegalArgumentException("Object was null");
    try {
      return jsonMapper.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new JSONException(e);
    }
  }

  /**
   * Serializes the given object to a JSON byte array
   * @param object The object to serialize
   * @return A JSON formatted byte array
   * @throws IllegalArgumentException if the object was null
   * @throws JSONException if the object could not be serialized
   * @throws IOException Thrown when there was an issue reading the object
   */
  public static final byte[] serializeToBytes(final Object object) {
    if (object == null)
      throw new IllegalArgumentException("Object was null");
    try {
      return jsonMapper.writeValueAsBytes(object);
    } catch (JsonProcessingException e) {
      throw new JSONException(e);
    }
  }

  /**
   * Serializes the given object and wraps it in a callback function
   * i.e. &lt;callback&gt;(&lt;json&gt)
   * Note: This will not append a trailing semicolon
   * @param callback The name of the Javascript callback to prepend
   * @param object The object to serialize
   * @return A JSONP formatted string
   * @throws IllegalArgumentException if the callback method name was missing 
   * or object was null
   * @throws JSONException if the object could not be serialized
   * @throws IOException Thrown when there was an issue reading the object
   */
  public static final String serializeToJSONPString(final String callback,
      final Object object) {
    if (callback == null || callback.isEmpty())
      throw new IllegalArgumentException("Missing callback name");
    if (object == null)
      throw new IllegalArgumentException("Object was null");
    try {
      return jsonMapper.writeValueAsString(new JSONPObject(callback, object));
    } catch (JsonProcessingException e) {
      throw new JSONException(e);
    }
  }

  /**
   * Serializes the given object and wraps it in a callback function
   * i.e. &lt;callback&gt;(&lt;json&gt)
   * Note: This will not append a trailing semicolon
   * @param callback The name of the Javascript callback to prepend
   * @param object The object to serialize
   * @return A JSONP formatted byte array
   * @throws IllegalArgumentException if the callback method name was missing 
   * or object was null
   * @throws JSONException if the object could not be serialized
   * @throws IOException Thrown when there was an issue reading the object
   */
  public static final byte[] serializeToJSONPBytes(final String callback,
      final Object object) {
    if (callback == null || callback.isEmpty())
      throw new IllegalArgumentException("Missing callback name");
    if (object == null)
      throw new IllegalArgumentException("Object was null");
    try {
      return jsonMapper.writeValueAsBytes(new JSONPObject(callback, object));
    } catch (JsonProcessingException e) {
      throw new JSONException(e);
    }
  }

  /**
   * Returns a reference to the static ObjectMapper
   * @return The ObjectMapper
   */
  public final static ObjectMapper getMapper() {
    return jsonMapper;
  }

  /**
   * Returns a reference to the JsonFactory for streaming creation
   * @return The JsonFactory object
   */
  public final static JsonFactory getFactory() {
    return jsonMapper.getFactory();
  }

  /**
   * Helper class for deserializing UID type enum from human readable strings
   */
  public static class UniqueIdTypeDeserializer 
    extends JsonDeserializer<UniqueIdType> {
    
    @Override
    public UniqueIdType deserialize(final JsonParser parser, final
        DeserializationContext context) throws IOException {
      return UniqueId.stringToUniqueIdType(parser.getValueAsString());
    }
  }
}
