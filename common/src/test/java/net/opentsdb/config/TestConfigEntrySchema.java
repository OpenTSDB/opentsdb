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
package net.opentsdb.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Type;

import org.junit.Test;

public class TestConfigEntrySchema {

  @Test
  public void builder() throws Exception {
    ConfigEntrySchema.Builder builder = ConfigEntrySchema.newBuilder();
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
      builder.setType((Type) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
   
    builder.setType(new ConfigTypeRef<Double>() { });
    assertEquals(Double.class, builder.type);
    try {
      builder.setType((ConfigTypeRef) null);
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
    
    ConfigValueValidator validator = mock(ConfigValueValidator.class); 
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
    
    final ConfigEntrySchema schema = builder.build();
    assertEquals("tsd.conf", schema.getKey());
    assertEquals(Double.class, schema.getType());
    assertEquals("net.opentsdb.tsd", schema.getSource());
    assertEquals("Description", schema.getDescription());
    assertSame(validator, schema.getValidator());
    assertEquals(42L, schema.getDefaultValue());
    assertTrue(schema.isDynamic());
  }
  
  @Test
  public void obfuscate() throws Exception {
    final ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue("A nice string to hide")
        .build();
    assertTrue(schema.toString().contains("A nice string to hide"));
    assertFalse(schema.toString().contains("********"));
    
    schema.obfuscate = true;
    assertFalse(schema.toString().contains("A nice string to hide"));
    assertTrue(schema.toString().contains("********"));
  }
}
