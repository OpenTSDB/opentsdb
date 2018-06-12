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

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;

/**
 * This class simply provides a static initialization and configuration of the
 * Jackson ObjectMapper for use throughout OpenTSDB. Since the mapper takes a
 * fair amount of construction and is thread safe, the Jackson docs recommend
 * initializing it once per app.
 * <p>
 * The class also provides some simple wrappers around commonly used
 * serialization and deserialization methods for POJOs as well as a YAMLP
 * wrapper. These work wonderfully for smaller objects and you can use JAVA
 * annotations to control the de/serialization for your POJO class.
 * <p>
 * For streaming of large objects, access the mapper directly via {@link
 * #getMapper()} or {@link #getFactory()}
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
 * {@code @JsonAutoDetect} annotation</li>
 * <li>Make sure any child objects are accessible, have the empty constructor 
 * and applicable annotations</li></ul>
 * <p>
 * Useful Class Annotations:
 * {@code @JsonAutoDetect(fieldVisibility = Visibility.ANY)} - will serialize 
 *   any, public or private values
 * <p>
 * {@code @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)} - will 
 *   automatically ignore any fields set to NULL, otherwise they are serialized
 *   with a literal null value
 * <p>
 * Useful Method Annotations:
 * {@code @JsonIgnore} - Ignores the method for de/serialization purposes. 
 *   CRITICAL for any methods that could cause a de/serialization infinite loop
 * @since 3.0
 */
public final class YAML {
  /**
   * Jackson de/serializer initialized, configured and shared
   */
  private static final ObjectMapper MAPPER = 
      new ObjectMapper(new YAMLFactory());
  static {
    // allows parsing NAN and such without throwing an exception. This is
    // important
    // for incoming data points with multiple points per put so that we can
    // toss only the bad ones but keep the good
    MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
  }

  /**
   * Deserializes a YAML formatted string to a specific class type
   * <b>Note:</b> If you get mapping exceptions you may need to provide a 
   * TypeReference
   * @param yaml The string to deserialize
   * @param pojo The class type of the object used for deserialization
   * @return An object of the {@code pojo} type
   * @throws IllegalArgumentException if the data or class was null or parsing 
   * failed
   * @throws YAMLException if the data could not be parsed
   * @param <T> The type of object to parse to.
   */
  public static final <T> T parseToObject(final String yaml,
      final Class<T> pojo) {
    if (yaml == null || yaml.isEmpty())
      throw new IllegalArgumentException("Incoming data was null or empty");
    if (pojo == null)
      throw new IllegalArgumentException("Missing class type");
    
    try {
      return MAPPER.readValue(yaml, pojo);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new YAMLException(e);
    }
  }

  /**
   * Deserializes a YAML formatted byte array to a specific class type
   * <b>Note:</b> If you get mapping exceptions you may need to provide a 
   * TypeReference
   * @param yaml The byte array to deserialize
   * @param pojo The class type of the object used for deserialization
   * @return An object of the {@code pojo} type
   * @throws IllegalArgumentException if the data or class was null or parsing 
   * failed
   * @throws YAMLException if the data could not be parsed
   * @param <T> The type of object to parse to.
   */
  public static final <T> T parseToObject(final byte[] yaml,
      final Class<T> pojo) {
    if (yaml == null)
      throw new IllegalArgumentException("Incoming data was null");
    if (pojo == null)
      throw new IllegalArgumentException("Missing class type");
    try {
      return MAPPER.readValue(yaml, pojo);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new YAMLException(e);
    }
  }
  
  /**
   * Deserializes a YAML formatted input stream to a specific class type
   * <b>Note:</b> If you get mapping exceptions you may need to provide a 
   * TypeReference
   * @param stream The stream to deserialize
   * @param pojo The class type of the object used for deserialization
   * @return An object of the {@code pojo} type
   * @throws IllegalArgumentException if the data or class was null or parsing 
   * failed
   * @throws YAMLException if the data could not be parsed
   * @param <T> The type of object to parse to.
   * 
   * @since 3.0
   */
  public static final <T> T parseToObject(final InputStream stream,
      final Class<T> pojo) {
    if (stream == null)
      throw new IllegalArgumentException("Incoming data was null");
    if (pojo == null)
      throw new IllegalArgumentException("Missing class type");
    try {
      return MAPPER.readValue(stream, pojo);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new YAMLException(e);
    }
  }

  /**
   * Deserializes a YAML formatted string to a specific class type
   * @param yaml The string to deserialize
   * @param type A type definition for a complex object
   * @return An object of the {@code pojo} type
   * @throws IllegalArgumentException if the data or type was null or parsing
   * failed
   * @throws YAMLException if the data could not be parsed
   * @param <T> The type of object to parse to.
   */
  @SuppressWarnings("unchecked")
  public static final <T> T parseToObject(final String yaml,
      final TypeReference<T> type) {
    if (yaml == null || yaml.isEmpty())
      throw new IllegalArgumentException("Incoming data was null or empty");
    if (type == null)
      throw new IllegalArgumentException("Missing type reference");
    try {
      return (T)MAPPER.readValue(yaml, type);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new YAMLException(e);
    }
  }

  /**
   * Deserializes a YAML formatted byte array to a specific class type
   * @param yaml The byte array to deserialize
   * @param type A type definition for a complex object
   * @return An object of the {@code pojo} type
   * @throws IllegalArgumentException if the data or type was null or parsing
   * failed
   * @throws YAMLException if the data could not be parsed
   * @param <T> The type of object to parse to.
   */
  @SuppressWarnings("unchecked")
  public static final <T> T parseToObject(final byte[] yaml,
      final TypeReference<T> type) {
    if (yaml == null)
      throw new IllegalArgumentException("Incoming data was null");
    if (type == null)
      throw new IllegalArgumentException("Missing type reference");
    try {
      return (T)MAPPER.readValue(yaml, type);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new YAMLException(e);
    }
  }

  /**
   * Deserializes a YAML formatted input stream to a specific class type
   * @param stream The input stream to deserialize
   * @param type A type definition for a complex object
   * @return An object of the {@code pojo} type
   * @throws IllegalArgumentException if the data or type was null or parsing
   * failed
   * @throws YAMLException if the data could not be parsed
   * @param <T> The type of object to parse to.
   * 
   * @since 3.0
   */
  @SuppressWarnings("unchecked")
  public static final <T> T parseToObject(final InputStream stream,
      final TypeReference<T> type) {
    if (stream == null)
      throw new IllegalArgumentException("Incoming data was null");
    if (type == null)
      throw new IllegalArgumentException("Missing type reference");
    try {
      return (T)MAPPER.readValue(stream, type);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new YAMLException(e);
    }
  }
  
  /**
   * Parses a YAML formatted string into raw tokens for streaming or tree
   * iteration
   * <b>Warning:</b> This method can parse an invalid YAML object without
   * throwing an error until you start processing the data
   * @param yaml The string to parse
   * @return A JsonParser object to be used for iteration
   * @throws IllegalArgumentException if the data was null or parsing failed
   * @throws YAMLException if the data could not be parsed
   */
  public static final YAMLParser parseToStream(final String yaml) {
    if (yaml == null || yaml.isEmpty())
      throw new IllegalArgumentException("Incoming data was null or empty");
    try {
      return (YAMLParser) MAPPER.getFactory().createParser(yaml);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new YAMLException(e);
    }
  }

  /**
   * Parses a YAML formatted byte array into raw tokens for streaming or tree
   * iteration
   * <b>Warning:</b> This method can parse an invalid YAML object without
   * throwing an error until you start processing the data
   * @param yaml The byte array to parse
   * @return A JsonParser object to be used for iteration
   * @throws IllegalArgumentException if the data was null or parsing failed
   * @throws YAMLException if the data could not be parsed
   */
  public static final YAMLParser parseToStream(final byte[] yaml) {
    if (yaml == null)
      throw new IllegalArgumentException("Incoming data was null");
    try {
      return (YAMLParser) MAPPER.getFactory().createParser(yaml);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new YAMLException(e);
    }
  }

  /**
   * Parses a YAML formatted inputs stream into raw tokens for streaming or tree
   * iteration
   * <b>Warning:</b> This method can parse an invalid YAML object without
   * throwing an error until you start processing the data
   * @param yaml The input stream to parse
   * @return A JsonParser object to be used for iteration
   * @throws IllegalArgumentException if the data was null or parsing failed
   * @throws YAMLException if the data could not be parsed
   */
  public static final YAMLParser parseToStream(final InputStream yaml) {
    if (yaml == null)
      throw new IllegalArgumentException("Incoming data was null");
    try {
      return (YAMLParser) MAPPER.getFactory().createParser(yaml);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new YAMLException(e);
    }
  }

  /**
   * Serializes the given object to a YAML string
   * @param object The object to serialize
   * @return A YAML formatted string
   * @throws IllegalArgumentException if the object was null
   * @throws YAMLException if the object could not be serialized
   */
  public static final String serializeToString(final Object object) {
    if (object == null)
      throw new IllegalArgumentException("Object was null");
    try {
      return MAPPER.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new YAMLException(e);
    }
  }

  /**
   * Serializes the given object to a YAML byte array
   * @param object The object to serialize
   * @return A YAML formatted byte array
   * @throws IllegalArgumentException if the object was null
   * @throws YAMLException if the object could not be serialized
   */
  public static final byte[] serializeToBytes(final Object object) {
    if (object == null)
      throw new IllegalArgumentException("Object was null");
    try {
      return MAPPER.writeValueAsBytes(object);
    } catch (JsonProcessingException e) {
      throw new YAMLException(e);
    }
  }
  
  /**
   * Returns a reference to the static ObjectMapper
   * @return The ObjectMapper
   */
  public final static ObjectMapper getMapper() {
    return MAPPER;
  }

  /**
   * Returns a reference to the JsonFactory for streaming creation
   * @return The YAMLFactory object
   */
  public final static YAMLFactory getFactory() {
    return (YAMLFactory) MAPPER.getFactory();
  }

}