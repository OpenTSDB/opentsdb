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

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.configuration.ConfigurationEntry;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.configuration.ConfigurationOverride;
import net.opentsdb.configuration.ConfigurationValueValidator;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationCallback;
import net.opentsdb.configuration.ConfigurationValueValidator.ValidationResult;
import net.opentsdb.configuration.provider.CommandLineProvider;
import net.opentsdb.configuration.provider.Provider;
import net.opentsdb.configuration.provider.RuntimeOverrideProvider;

public class TestConfigurationEntry {

  private Configuration config = mock(Configuration.class);
  private Provider runtime = mock(Provider.class);
  private Provider cli = mock(Provider.class);
  
  @Before
  public void before() throws Exception {
    when(runtime.source()).thenReturn(RuntimeOverrideProvider.SOURCE);
    when(cli.source()).thenReturn(CommandLineProvider.SOURCE);
    
    when(config.sources()).thenReturn(Lists.newArrayList(cli, runtime));
  }
  
  @Test
  public void setSchemaNull() throws Exception {
    ConfigurationEntry entry = new ConfigurationEntry(config);
    try {
      entry.setSchema(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void setSchema() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue("localhost")
        .setType(String.class)
        .setSource("main")
        .build();
    
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    assertTrue(result.isValid());
    assertSame(schema, entry.schema());
  }

  @Test
  public void setSchemaAlreadySet() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue("localhost")
        .setType(String.class)
        .setSource("main")
        .build();
    
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    assertTrue(result.isValid());
    assertSame(schema, entry.schema());
    
    schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf2")
        .setDefaultValue("remote")
        .setType(String.class)
        .setSource("other")
        .build();
    try {
      entry.setSchema(schema);
      fail("Expected ConfigurationException");
    } catch (ConfigurationException e) { }
  }
  
  @Test
  public void addOverrideNull() throws Exception {
    ConfigurationEntry entry = new ConfigurationEntry(config);
    try {
      entry.addOverride(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void addOverrideNoSchema() throws Exception {
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    try {
      entry.addOverride(setting);
      fail("Expected ConfigurationException");
    } catch (ConfigurationException e) { }
  }
  
  @Test
  public void addOverride() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setType(long.class)
        .setSource("main")
        .build();
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().get(0));
  }
  
  @Test
  public void addOverrideSecret() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setType(long.class)
        .setSource("main")
        .isSecret()
        .build();
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().get(0));
    assertTrue(setting.toString().contains("********"));
  }
  
  @Test
  public void addOverrideNotInList() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setType(long.class)
        .setSource("main")
        .build();
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource("NotWhitelisted")
        .setValue(42L)
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    entry.setSchema(schema);
    try {
      entry.addOverride(setting);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void addOverrideOrderingMiddle() throws Exception {
    final Provider source1 = mock(Provider.class);
    final Provider source2 = mock(Provider.class);
    when(source1.source()).thenReturn("source1");
    when(source2.source()).thenReturn("source2");
    
    config = mock(Configuration.class);
    when(config.sources()).thenReturn(
        Lists.newArrayList(cli, source2, source1, runtime));
    
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setType(long.class)
        .setSource("main")
        .isDynamic()
        .build();
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().get(0));
    assertEquals(42L, entry.getValue());
    
    // skip 2
    setting = ConfigurationOverride.newBuilder()
        .setSource("source1")
        .setValue(24L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(24L, entry.getValue());
    
    // now back to 2
    setting = ConfigurationOverride.newBuilder()
        .setSource("source2")
        .setValue(1L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(24L, entry.getValue());
    
    // runtime for kicks
    setting = ConfigurationOverride.newBuilder()
        .setSource(RuntimeOverrideProvider.SOURCE)
        .setValue(2L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(2L, entry.getValue());
  }
  
  @Test
  public void addOverrideOrderingLast() throws Exception {
    final Provider source1 = mock(Provider.class);
    final Provider source2 = mock(Provider.class);
    when(source1.source()).thenReturn("source1");
    when(source2.source()).thenReturn("source2");
    
    config = mock(Configuration.class);
    when(config.sources()).thenReturn(
        Lists.newArrayList(cli, source2, source1, runtime));
    
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setType(long.class)
        .setSource("main")
        .isDynamic()
        .build();
    // skip CLI
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource("source1")
        .setValue(24L)
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().get(0));
    assertEquals(24L, entry.getValue());
    
    // now back to 2
    setting = ConfigurationOverride.newBuilder()
        .setSource("source2")
        .setValue(1L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(24L, entry.getValue());
    
    // CLI test
    setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(24L, entry.getValue());
    
    // runtime for kicks
    setting = ConfigurationOverride.newBuilder()
        .setSource(RuntimeOverrideProvider.SOURCE)
        .setValue(2L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(2L, entry.getValue());
  }
  
  @Test
  public void addOverrideOrderingFirst() throws Exception {
    final Provider source1 = mock(Provider.class);
    final Provider source2 = mock(Provider.class);
    when(source1.source()).thenReturn("source1");
    when(source2.source()).thenReturn("source2");
    
    config = mock(Configuration.class);
    when(config.sources()).thenReturn(
        Lists.newArrayList(cli, source2, source1, runtime));
    
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setType(long.class)
        .setSource("main")
        .isDynamic()
        .build();
    // skip CLI
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().get(0));
    assertEquals(42L, entry.getValue());
    
    setting = ConfigurationOverride.newBuilder()
        .setSource("source2")
        .setValue(1L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1L, entry.getValue());
    
    setting = ConfigurationOverride.newBuilder()
        .setSource("source1")
        .setValue(24L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(24L, entry.getValue());
    
    // runtime for kicks
    setting = ConfigurationOverride.newBuilder()
        .setSource(RuntimeOverrideProvider.SOURCE)
        .setValue(2L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(2L, entry.getValue());
  }
  
  @Test
  public void addOverrideDiffSources() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setType(long.class)
        .setSource("main")
        .isDynamic()
        .build();
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().get(0));
    
    setting = ConfigurationOverride.newBuilder()
        .setSource(RuntimeOverrideProvider.SOURCE)
        .setValue(24)
        .build();
    entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(2, entry.settings().size());
    assertSame(setting, entry.settings().get(0));
  }
  
  @Test
  public void addOverrideValidatorOK() throws Exception {
    ConfigurationValueValidator validator = mock(ConfigurationValueValidator.class);
    when(validator.validate(any(ConfigurationEntrySchema.class), any()))
      .thenReturn(ConfigurationValueValidator.OK);
    when(validator.validate(any(ConfigurationOverride.class)))
      .thenReturn(ConfigurationValueValidator.OK);
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setType(long.class)
        .setSource("main")
        .setValidator(validator)
        .build();
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().get(0));
  }
  
  @Test
  public void addOverrideValidatorFailure() throws Exception {
    ConfigurationValueValidator validator = mock(ConfigurationValueValidator.class);
    when(validator.validate(any(ConfigurationEntrySchema.class), any()))
      .thenReturn(ConfigurationValueValidator.OK)
      .thenReturn(ValidationResult.newBuilder().setMessage("Boo!").build());
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setType(long.class)
        .setSource("main")
        .setValidator(validator)
        .build();
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    result = entry.addOverride(setting);
    assertFalse(result.isValid());
    assertNull(entry.settings());
  }
  
  @Test
  public void addOverrideUpdateSame() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setType(long.class)
        .setSource("main")
        .build();
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().get(0));
    
    setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertEquals(42L, entry.getValue());
  }
  
  @Test
  public void addOverrideUpdateNotDynamic() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setType(long.class)
        .setSource("main")
        .build();
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().get(0));
    
    setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(24L)
        .build();
    result = entry.addOverride(setting);
    assertFalse(result.isValid());
    assertEquals(1, entry.settings().size());
    assertEquals(42L, entry.getValue());
  }
  
  @Test
  public void addOverrideUpdateDynamic() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setType(long.class)
        .setSource("main")
        .isDynamic()
        .build();
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().get(0));
    
    setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(24L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertEquals(24L, entry.getValue());
  }
  
  @Test
  public void addOverrideUpdateCallbacks() throws Exception {
    final long[] value = new long[1];
    
    class MyCB implements ConfigurationCallback<Long> {
      @Override
      public void update(String key, Long v) {
        value[0] = v;
      }
    }
    
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(1L)
        .setType(long.class)
        .setSource("main")
        .isDynamic()
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    entry.addCallback(new MyCB());
    assertEquals(1L, value[0]);
    
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().get(0));
    assertEquals(42L, value[0]);
    
    setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(24L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertEquals(24L, entry.getValue());
    assertEquals(24L, value[0]);
  }
  
  @Test
  public void addOverrideUpdateCallbacksException() throws Exception {
    final long[] value = new long[1];
    
    class MyCB implements ConfigurationCallback<Long> {
      @Override
      public void update(String key, Long v) {
        value[0] = v;
        throw new RuntimeException("Boo!");
      }
    }
    
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(1L)
        .setType(long.class)
        .setSource("main")
        .isDynamic()
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    ValidationResult result = entry.setSchema(schema);
    entry.addCallback(new MyCB());
    assertEquals(1L, value[0]);
    
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertSame(setting, entry.settings().get(0));
    assertEquals(42L, value[0]);
    
    setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(24L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertEquals(24L, entry.getValue());
    assertEquals(24L, value[0]);
  }

  @Test
  public void removeRuntimeOverride() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(0L)
        .setType(long.class)
        .setSource("main")
        .isDynamic()
        .build();
    ConfigurationOverride setting = ConfigurationOverride.newBuilder()
        .setSource(CommandLineProvider.SOURCE)
        .setValue(42L)
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    assertFalse(entry.removeRuntimeOverride());
    
    ValidationResult result = entry.setSchema(schema);
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(1, entry.settings().size());
    assertEquals(42L, entry.getValue());
    
    assertFalse(entry.removeRuntimeOverride());
    assertEquals(1, entry.settings().size());
    assertEquals(42L, entry.getValue());
    
    setting = ConfigurationOverride.newBuilder()
        .setSource(RuntimeOverrideProvider.SOURCE)
        .setValue(24L)
        .build();
    result = entry.addOverride(setting);
    assertTrue(result.isValid());
    assertEquals(2, entry.settings().size());
    assertEquals(24L, entry.getValue());
    
    assertTrue(entry.removeRuntimeOverride());
    assertEquals(1, entry.settings().size());
    assertEquals(42L, entry.getValue());
  }
  
  @Test
  public void addCallback() throws Exception {
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey("tsd.conf")
        .setDefaultValue(42L)
        .setType(long.class)
        .setSource("main")
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(config);
    entry.setSchema(schema);
    
    try {
      entry.addCallback(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    final long[] value = new long[1];
    class MyCB implements ConfigurationCallback<Long> {
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
    class BadCB implements ConfigurationCallback<Long> {
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
