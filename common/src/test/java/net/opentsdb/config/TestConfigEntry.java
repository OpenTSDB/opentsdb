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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.junit.Test;

import net.opentsdb.config.ConfigValueValidator.ValidationResult;

public class TestConfigEntry {

  @Test
  public void setSchemaNull() throws Exception {
    ConfigEntry entry = new ConfigEntry();
    try {
      entry.setSchema(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void setSchema() throws Exception {
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue("localhost")
        .setSource("main")
        .build();
    
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    assertTrue(result.isValid());
    assertSame(schema, entry.schema());
  }

  @Test
  public void setSchemaAlreadySet() throws Exception {
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue("localhost")
        .setSource("main")
        .build();
    
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    assertTrue(result.isValid());
    assertSame(schema, entry.schema());
    
    schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf2")
        .setDefaultValue("remote")
        .setSource("other")
        .build();
    try {
      entry.setSchema(schema);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void setSchemaValidationOK() throws Exception {
    ConfigValueValidator validator = mock(ConfigValueValidator.class);
    when(validator.validate(any(ConfigEntrySchema.class)))
      .thenReturn(ConfigValueValidator.OK);

    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
      .setKey("tsd.conf2")
      .setDefaultValue("remote")
      .setSource("other")
      .setValidator(validator)
      .build();
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    assertTrue(result.isValid());
    assertSame(schema, entry.schema());
  }

  @Test
  public void setSchemaValidationFailed() throws Exception {
    ConfigValueValidator validator = mock(ConfigValueValidator.class);
    when(validator.validate(any(ConfigEntrySchema.class)))
      .thenReturn(ValidationResult.newBuilder().setMessage("Boo!").build());
    
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf2")
        .setDefaultValue("remote")
        .setSource("other")
        .setValidator(validator)
        .build();
      ConfigEntry entry = new ConfigEntry();
      ValidationResult result = entry.setSchema(schema);
    assertFalse(result.isValid());
    assertNull(entry.schema());
  }
  
  @Test
  public void setSchemaNullableOK() throws Exception {
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf2")
        //.setDefaultValue("remote")
        .setSource("other")
        .isNullable()
        .build();
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    assertTrue(result.isValid());
    assertSame(schema, entry.schema());
  }
  
  @Test
  public void setSchemaNullableFailed() throws Exception {
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf2")
        //.setDefaultValue("remote")
        .setSource("other")
        .build();
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    assertFalse(result.isValid());
    assertNull(entry.schema());
    assertSame(ConfigValueValidator.NULL, result);
  }
  
  @Test
  public void setSettingNull() throws Exception {
    ConfigEntry entry = new ConfigEntry();
    try {
      entry.setSetting(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void setSettingNoSchema() throws Exception {
    ConfigSetting setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(42L)
        .build();
    ConfigEntry entry = new ConfigEntry();
    try {
      entry.setSetting(setting);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void setSetting() throws Exception {
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setSource("main")
        .build();
    ConfigSetting setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(42L)
        .build();
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    result = entry.setSetting(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().peek());
  }
  
  @Test
  public void setSettingDiffSources() throws Exception {
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setSource("main")
        .build();
    ConfigSetting setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(42L)
        .build();
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    result = entry.setSetting(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().peek());
    
    setting = ConfigSetting.newBuilder()
        .setSource("Another Source")
        .setValue(24)
        .build();
    entry.setSetting(setting);
    assertTrue(result.isValid());
    assertEquals(2, entry.settings().size());
    assertSame(setting, entry.settings().peek());
  }
  
  @Test
  public void setSettingValidatorOK() throws Exception {
    ConfigValueValidator validator = mock(ConfigValueValidator.class);
    when(validator.validate(any(ConfigEntrySchema.class)))
      .thenReturn(ConfigValueValidator.OK);
    when(validator.validate(any(ConfigSetting.class)))
      .thenReturn(ConfigValueValidator.OK);
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setSource("main")
        .setValidator(validator)
        .build();
    ConfigSetting setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(42L)
        .build();
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    result = entry.setSetting(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().peek());
  }
  
  @Test
  public void setSettingValidatorFailure() throws Exception {
    ConfigValueValidator validator = mock(ConfigValueValidator.class);
    when(validator.validate(any(ConfigEntrySchema.class)))
      .thenReturn(ConfigValueValidator.OK);
    when(validator.validate(any(ConfigSetting.class)))
      .thenReturn(ValidationResult.newBuilder().setMessage("Boo!").build());
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setSource("main")
        .setValidator(validator)
        .build();
    ConfigSetting setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(42L)
        .build();
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    result = entry.setSetting(setting);
    assertFalse(result.isValid());
    assertNull(entry.settings());
  }

  @Test
  public void setSettingNullableOK() throws Exception {
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setSource("main")
        .isNullable()
        .build();
    ConfigSetting setting = ConfigSetting.newBuilder()
        .setSource("Here")
        //.setValue(42L)
        .build();
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    result = entry.setSetting(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().peek());
  }
  
  @Test
  public void setSettingNullableFailed() throws Exception {
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setSource("main")
        .build();
    ConfigSetting setting = ConfigSetting.newBuilder()
        .setSource("Here")
        //.setValue(42L)
        .build();
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    result = entry.setSetting(setting);
    assertFalse(result.isValid());
    assertSame(result, ConfigValueValidator.NULL);
  }
  
  @Test
  public void setSettingUpdateSame() throws Exception {
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setSource("main")
        .build();
    ConfigSetting setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(42L)
        .build();
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    result = entry.setSetting(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().peek());
    
    setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(42L)
        .build();
    result = entry.setSetting(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertEquals(42L, entry.getValue());
  }
  
  @Test
  public void setSettingUpdateNotDynamic() throws Exception {
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setSource("main")
        .build();
    ConfigSetting setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(42L)
        .build();
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    result = entry.setSetting(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().peek());
    
    setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(24L)
        .build();
    result = entry.setSetting(setting);
    assertFalse(result.isValid());
    assertEquals(1, entry.settings().size());
    assertEquals(42L, entry.getValue());
  }
  
  @Test
  public void setSettingUpdateDynamic() throws Exception {
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setSource("main")
        .isDynamic()
        .build();
    ConfigSetting setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(42L)
        .build();
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    result = entry.setSetting(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().peek());
    
    setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(24L)
        .build();
    result = entry.setSetting(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertEquals(24L, entry.getValue());
  }
  
  @Test
  public void setSettingUpdateCallbacks() throws Exception {
    final long[] value = new long[1];
    
    class MyCB implements ConfigCallback<Long> {
      @Override
      public void update(String key, Long v) {
        value[0] = v;
      }
    }
    
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(1L)
        .setSource("main")
        .isDynamic()
        .build();
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    entry.addCallback(new MyCB());
    assertEquals(1L, value[0]);
    
    ConfigSetting setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(42L)
        .build();
    result = entry.setSetting(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().peek());
    assertEquals(42L, value[0]);
    
    setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(24L)
        .build();
    result = entry.setSetting(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertEquals(24L, entry.getValue());
    assertEquals(24L, value[0]);
  }
  
  @Test
  public void setSettingUpdateCallbacksException() throws Exception {
    final long[] value = new long[1];
    
    class MyCB implements ConfigCallback<Long> {
      @Override
      public void update(String key, Long v) {
        value[0] = v;
        throw new RuntimeException("Boo!");
      }
    }
    
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(1L)
        .setSource("main")
        .isDynamic()
        .build();
    ConfigEntry entry = new ConfigEntry();
    ValidationResult result = entry.setSchema(schema);
    entry.addCallback(new MyCB());
    assertEquals(1L, value[0]);
    
    ConfigSetting setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(42L)
        .build();
    result = entry.setSetting(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().peek());
    assertEquals(42L, value[0]);
    
    setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue(24L)
        .build();
    result = entry.setSetting(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertEquals(24L, entry.getValue());
    assertEquals(24L, value[0]);
  }

  @Test
  public void addCallback() throws Exception {
    ConfigEntrySchema schema = ConfigEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(42L)
        .setSource("main")
        .build();
    ConfigEntry entry = new ConfigEntry();
    entry.setSchema(schema);
    
    try {
      entry.addCallback(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    final long[] value = new long[1];
    class MyCB implements ConfigCallback<Long> {
      @Override
      public void update(String key, Long v) {
        value[0] = v;
      }
    }
    
    assertEquals(0L, value[0]);
    assertNull(entry.callbacks());
    entry.addCallback(new MyCB());
    assertEquals(1, entry.callbacks().size());
    assertEquals(42L, value[0]);
    
    value[0] = 0L;
    class BadCB implements ConfigCallback<Long> {
      @Override
      public void update(String key, Long v) {
        value[0] = v;
        throw new RuntimeException("Boo!");
      }
    }
    
    entry.addCallback(new BadCB());
    assertEquals(2, entry.callbacks().size());
    assertEquals(42L, value[0]);
  }
}
