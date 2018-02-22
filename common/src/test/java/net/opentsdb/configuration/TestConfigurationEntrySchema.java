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
package net.opentsdb.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;

import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.configuration.ConfigurationValueValidator;
import net.opentsdb.configuration.ConfigurationValueValidator.ValidationResult;

public class TestConfigurationEntrySchema {

  @Test
  public void builder() throws Exception {
    ConfigurationEntrySchema.Builder builder = 
        ConfigurationEntrySchema.newBuilder();
    builder.setKey("tsd.conf");
    assertEquals("tsd.conf", builder.key);
    try {
      builder.setKey(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      builder.setKey("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    builder.setType(Long.class);
    assertEquals(Long.class, builder.type);
    try {
      builder.setType((Class) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
   
    builder.setType(new TypeReference<Double>() { });
    assertEquals(Double.class, builder.type_reference.getType());
    try {
      builder.setType((TypeReference) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    builder.setSource("net.opentsdb.tsd");
    assertEquals("net.opentsdb.tsd", builder.source);
    try {
      builder.setSource(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      builder.setSource("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    builder.setDescription("Description");
    assertEquals("Description", builder.description);
    try {
      builder.setDescription(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      builder.setDescription("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    ConfigurationValueValidator validator = 
        mock(ConfigurationValueValidator.class);
    when(validator.validate(any(ConfigurationEntrySchema.class)))
      .thenReturn(ConfigurationValueValidator.OK);
    when(validator.validate(any(ConfigurationEntrySchema.class), any()))
      .thenReturn(ConfigurationValueValidator.OK);
    builder.setValidator(validator);
    assertSame(validator, builder.validator);
    builder.setValidator(null);
    assertNull(builder.validator);
    
    builder.setDefaultValue(42L);
    assertEquals(42L, builder.default_value);
    builder.setDefaultValue(null);
    assertNull(builder.default_value);
    
    assertFalse(builder.dynamic);
    builder.isDynamic();
    assertTrue(builder.dynamic);
    builder.notDynamic();
    assertFalse(builder.dynamic);
    
    assertFalse(builder.nullable);
    builder.isNullable();
    assertTrue(builder.nullable);
    builder.notNullable();
    assertFalse(builder.nullable);
    
    builder.setValidator(validator);
    builder.isDynamic();
    builder.setDefaultValue(42L);
    
    // reset
    builder.type = null;
    
    final ConfigurationEntrySchema schema = builder.build();
    assertEquals("tsd.conf", schema.getKey());
    assertEquals(Double.class, schema.getTypeReference().getType());
    assertEquals("net.opentsdb.tsd", schema.getSource());
    assertEquals("Description", schema.getDescription());
    assertEquals(42L, schema.getDefaultValue());
    assertSame(validator, schema.getValidator());
    assertTrue(schema.isDynamic());
  }
  
  @Test
  public void builderNullKey() throws Exception {
    try {
      ConfigurationEntrySchema.newBuilder()
        .setDefaultValue(42L)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void builderNoType() throws Exception {
    try {
      ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(42L)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void builderNotNullableButNullDefaultValue() throws Exception {
    try {
      ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setType(long.class)
        .notNullable()
        //.setDefaultValue(42L)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void builderNullPrimitive() throws Exception {
    try {
      ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setType(long.class)
        .isNullable()
        .setSource("here")
        //.setDefaultValue(42L)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // but not a primitive is ok
    ConfigurationEntrySchema.newBuilder()
      .setKey("tsd.conf")
      .setType(Long.class)
      .isNullable()
      .setSource("here")
      //.setDefaultValue(42L)
      .build();
  }
  
  @Test
  public void builderTypeParsing() throws Exception {
    // nope, can't cast that string to a number
    try {
      ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setType(long.class)
        .setDefaultValue("A nice string to hide")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // this is ok, Jackson will cast it!
    ConfigurationEntrySchema.newBuilder()
      .setKey("tsd.conf")
      .setType(long.class)
      .setDefaultValue("42")
      .setSource("here")
      .build();
    
    // can't cast from a float to an int
    try {
      ConfigurationEntrySchema.newBuilder()
          .setKey("tsd.conf")
          .setType(long.class)
          .setDefaultValue("42.968")
          .setSource("here")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setType(double.class)
        .setDefaultValue("42")
        .setSource("here")
        .build();
    
    // this works
    final Map<String, Long> map = Maps.newHashMapWithExpectedSize(1);
    map.put("Key", 42L);
    ConfigurationEntrySchema.newBuilder()
      .setKey("tsd.conf")
      .setType(Map.class)
      .setDefaultValue(map)
      .setSource("here")
      .build();
    
    TypeReference<Map<String, Long>> type_reference = 
        new TypeReference<Map<String, Long>>() { };
    ConfigurationEntrySchema.newBuilder()
      .setKey("tsd.conf")
      .setType(type_reference)
      .setDefaultValue(map)
      .setSource("here")
      .build();
    
    // nope, need to decode upstream.
    try {
      ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setType(type_reference)
        .setDefaultValue("{\"key\":42}")
        .setSource("here")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void builderValidationError() throws Exception {
    ConfigurationValueValidator validator = mock(ConfigurationValueValidator.class);
    when(validator.validate(any(ConfigurationEntrySchema.class)))
      .thenReturn(ConfigurationValueValidator.NULL);
    try {
      ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setType(long.class)
        .setDefaultValue("42")
        .setValidator(validator)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void secret() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setType(String.class)
        .setDefaultValue("A nice string to hide")
        .setSource("here")
        .build();
    assertTrue(schema.toString().contains("A nice string to hide"));
    assertFalse(schema.toString().contains("********"));
    
    schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setType(String.class)
        .setDefaultValue("A nice string to hide")
        .setSource("here")
        .isSecret()
        .build();
    assertFalse(schema.toString().contains("A nice string to hide"));
    assertTrue(schema.toString().contains("********"));
  }
  
  @Test
  public void validate() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setType(long.class)
        .setDefaultValue(42)
        .setSource("here")
        .build();
    
    ValidationResult result = schema.validate(42);
    assertTrue(result.isValid());
    
    result = schema.validate("42");
    assertTrue(result.isValid());
    
    result = schema.validate(80.50);
    assertTrue(result.isValid());
    
    result = schema.validate("Can't convert");
    assertFalse(result.isValid());
    
    result = schema.validate(null);
    assertFalse(result.isValid());
    
    schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setType(String.class)
        .setDefaultValue(42) // casts it
        .setSource("here")
        .isNullable()
        .build();
    
    result = schema.validate(42);
    assertTrue(result.isValid());
    
    result = schema.validate(null);
    assertTrue(result.isValid());
  }
}
