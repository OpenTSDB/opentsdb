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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;

import net.opentsdb.configuration.ConfigurationOverride;

public class TestConfigurationOverride {

  @Test
  public void builder() throws Exception {
    ConfigurationOverride.Builder builder = ConfigurationOverride.newBuilder();
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
    
    builder.setValue(42L);
    assertEquals(42L, builder.value);
    builder.setValue(null);
    assertNull(builder.value);
    
    try {
      ConfigurationOverride.newBuilder()
          .setValue("Boo!")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void validateNotNullableButNullValue() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
      .setKey("tsd.conf")
      .setType(Long.class)
      .notNullable()
      .setDefaultValue(42L)
      .setSource("here")
      .build();
    try {
      ConfigurationOverride.newBuilder()
        .setSource("source")
        .build()
        .validate(schema);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void validateNullPrimitive() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
      .setKey("tsd.conf")
      .setType(long.class)
      .isNullable()
      .setDefaultValue(42L)
      .setSource("here")
      .build();
    try {
      ConfigurationOverride.newBuilder()
        .setSource("source")
        .build()
        .validate(schema);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void validateTypeParsing() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
      .setKey("tsd.conf")
      .setType(long.class)
      .isNullable()
      .setDefaultValue(42L)
      .setSource("here")
      .build();
    try {
      ConfigurationOverride.newBuilder()
        .setSource("source")
        .setValue("A nice string to hide")
        .build()
        .validate(schema);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // this is ok, Jackson will cast it!
    ConfigurationOverride.newBuilder()
      .setSource("source")
      .setValue("42")
      .build()
      .validate(schema);
    
    // can't cast from a float to an int
    try {
      ConfigurationOverride.newBuilder()
        .setSource("source")
        .setValue("42.968")
        .build()
        .validate(schema);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setType(Map.class)
        .setSource("here")
        .isNullable()
        .build();
    final Map<String, Long> map = Maps.newHashMapWithExpectedSize(1);
    map.put("Key", 42L);
    ConfigurationOverride.newBuilder()
      .setSource("source")
      .setValue(map)
      .build()
      .validate(schema);
  }
  
  @Test
  public void validateValidationError() throws Exception {
    ConfigurationValueValidator validator = mock(ConfigurationValueValidator.class);
    when(validator.validate(any(ConfigurationEntrySchema.class), any()))
      .thenReturn(ConfigurationValueValidator.OK)
      .thenReturn(ConfigurationValueValidator.NULL);
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
      .setKey("tsd.conf")
      .setType(long.class)
      .setDefaultValue(42L)
      .setSource("here")
      .setValidator(validator)
      .setSource("here")
      .build();
    try {
      ConfigurationOverride.newBuilder()
        .setSource("source")
        .setValue("A nice string to hide")
        .build()
        .validate(schema);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void secret() throws Exception {
    final ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource("Here")
        .setValue("A nice string to hide")
        .build();
    assertTrue(setting.toString().contains("A nice string to hide"));
    assertFalse(setting.toString().contains("********"));
    
    setting.secret = true;
    assertFalse(setting.toString().contains("A nice string to hide"));
    assertTrue(setting.toString().contains("********"));
  }
}
