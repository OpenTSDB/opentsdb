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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.configuration.ConfigurationOverride;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.configuration.ConfigurationValueValidator.ValidationResult;
import net.opentsdb.configuration.provider.CommandLineProvider;
import net.opentsdb.configuration.provider.ProtocolProviderFactory;
import net.opentsdb.configuration.provider.Provider;
import net.opentsdb.configuration.provider.ProviderFactory;
import net.opentsdb.configuration.provider.RuntimeOverrideProvider;
import net.opentsdb.configuration.provider.SystemPropertiesProvider;

public class TestConfiguration {
  
  private static final int BUILT_IN_FACTORIES = 9;
  private static final int BUILT_IN_SCHEMAS = 3;
  
  final String[] cli_args = new String[] {
      "--" + Configuration.CONFIG_PROVIDERS_KEY 
        + "=SystemProperties,CommandLine,RuntimeOverride"
  };
  
  @Test
  public void ctorDefault() throws Exception {
    try (final Configuration config = new Configuration(cli_args)) {
      assertEquals("SystemProperties,CommandLine,RuntimeOverride", 
          config.provider_config);
      assertNull(config.plugin_path);
      assertEquals(BUILT_IN_SCHEMAS, config.merged_config.size());
      assertEquals("SystemProperties,CommandLine,RuntimeOverride", 
          config.merged_config.get(Configuration.CONFIG_PROVIDERS_KEY).getValue());
      assertNull(config.merged_config.get(Configuration.PLUGIN_DIRECTORY_KEY)
          .getValue());
      assertEquals(3, config.providers.size());
      assertTrue(config.providers.get(0) instanceof RuntimeOverrideProvider);
      assertTrue(config.providers.get(1) instanceof CommandLineProvider);
      assertTrue(config.providers.get(2) instanceof SystemPropertiesProvider);
      assertEquals(BUILT_IN_FACTORIES, config.factories.size());
    }
  }
  
  @Test
  public void ctorCliOverrides() throws Exception {
    final String[] cli_args = new String[] {
        "--" + Configuration.CONFIG_PROVIDERS_KEY + "=SystemProperties,CommandLine",
        "--" + Configuration.PLUGIN_DIRECTORY_KEY + "=."
    };
    
    try (final Configuration config = new Configuration(cli_args)) {
      assertEquals("SystemProperties,CommandLine", config.provider_config);
      assertEquals(".", config.plugin_path);
      assertEquals(BUILT_IN_SCHEMAS, config.merged_config.size());
      assertEquals("SystemProperties,CommandLine", 
          config.merged_config.get(Configuration.CONFIG_PROVIDERS_KEY).getValue());
      assertEquals(".", config.merged_config.get(Configuration.PLUGIN_DIRECTORY_KEY)
          .getValue());
      assertEquals(2, config.providers.size());
      assertTrue(config.providers.get(0) instanceof CommandLineProvider);
      assertTrue(config.providers.get(1) instanceof SystemPropertiesProvider);
      assertEquals(BUILT_IN_FACTORIES, config.factories.size());
    }
  }

  @Test
  public void ctorNullArgs() throws Exception {
    try (final Configuration config = new Configuration(null)) {
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  // TODO - find a way to test out the system properties and environment
  // overrides. I can't load the plugins when executing @PrepareForTest
  // as it must be messing with the Guava reflection calls.
  
  @Test
  public void ctorCliDupliateProviders() throws Exception {
    final String[] cli_args = new String[] {
        "--" + Configuration.CONFIG_PROVIDERS_KEY + 
          "=SystemProperties,CommandLine,SystemProperties,CommandLine",
        "--" + Configuration.PLUGIN_DIRECTORY_KEY + "=."
    };
    
    try (final Configuration config = new Configuration(cli_args)) {
      assertEquals("SystemProperties,CommandLine,SystemProperties,CommandLine", 
          config.provider_config);
      assertEquals(".", config.plugin_path);
      assertEquals(3, config.merged_config.size());
      assertEquals("SystemProperties,CommandLine,SystemProperties,CommandLine", 
          config.merged_config.get(Configuration.CONFIG_PROVIDERS_KEY).getValue());
      assertEquals(".", config.merged_config.get(Configuration.PLUGIN_DIRECTORY_KEY)
          .getValue());
      assertEquals(2, config.providers.size());
      assertTrue(config.providers.get(0) instanceof CommandLineProvider);
      assertTrue(config.providers.get(1) instanceof SystemPropertiesProvider);
      assertEquals(BUILT_IN_FACTORIES, config.factories.size());
    }
  }
  
  @Test
  public void ctorCliQuotedAndSpaces() throws Exception {
    final String[] cli_args = new String[] {
        "--" + Configuration.CONFIG_PROVIDERS_KEY + 
          "=\"SystemProperties, CommandLine\"",
        "--" + Configuration.PLUGIN_DIRECTORY_KEY + "=."
    };
    
    try (final Configuration config = new Configuration(cli_args)) {
      assertEquals("SystemProperties, CommandLine", 
          config.provider_config);
      assertEquals(".", config.plugin_path);
      assertEquals(3, config.merged_config.size());
      assertEquals("SystemProperties, CommandLine", 
          config.merged_config.get(Configuration.CONFIG_PROVIDERS_KEY).getValue());
      assertEquals(".", config.merged_config.get(Configuration.PLUGIN_DIRECTORY_KEY)
          .getValue());
      assertEquals(2, config.providers.size());
      assertTrue(config.providers.get(0) instanceof CommandLineProvider);
      assertTrue(config.providers.get(1) instanceof SystemPropertiesProvider);
      assertEquals(BUILT_IN_FACTORIES, config.factories.size());
    }
  }
  
  @Test
  public void ctorCliNoSuchProvider() throws Exception {
    final String[] cli_args = new String[] {
        "--" + Configuration.CONFIG_PROVIDERS_KEY + 
          "=SystemProperties,NoSuchProvider,CommandLine",
        "--" + Configuration.PLUGIN_DIRECTORY_KEY + "=."
    };
    
    try (final Configuration config = new Configuration(cli_args)) {
      fail("Expected ConfigurationException");
    } catch (ConfigurationException e) { }
  }
  
  @Test
  public void ctorCliNoSuchProtocol() throws Exception {
    final String[] cli_args = new String[] {
        "--" + Configuration.CONFIG_PROVIDERS_KEY + 
          "=SystemProperties,rtmp://isflashdead,CommandLine",
        "--" + Configuration.PLUGIN_DIRECTORY_KEY + "=."
    };
    
    try (final Configuration config = new Configuration(cli_args)) {
      fail("Expected ConfigurationException");
    } catch (ConfigurationException e) { }
  }
  
  @Test
  public void ctorProtocol() throws Exception {
    final String[] cli_args = new String[] {
        "--" + Configuration.CONFIG_PROVIDERS_KEY + "=https://mysite.com,CommandLine"
    };
    
    try (final Configuration config = new Configuration(cli_args)) {
      assertEquals("https://mysite.com,CommandLine", config.provider_config);
      assertEquals(3, config.merged_config.size());
      assertEquals("https://mysite.com,CommandLine", 
          config.merged_config.get(Configuration.CONFIG_PROVIDERS_KEY).getValue());
      assertNull(config.merged_config.get(Configuration.PLUGIN_DIRECTORY_KEY)
          .getValue());
      assertEquals(2, config.providers.size());
      assertTrue(config.providers.get(0) instanceof CommandLineProvider);
      assertTrue(config.providers.get(1) instanceof DummyHttpProvider);
      assertEquals(BUILT_IN_FACTORIES, config.factories.size());
    }
  }
  
  @Test
  public void register() throws Exception {
    final String[] cli_args = new String[] {
        "--my.custom.key=42",
        "--another.key=50.75",
        "--" + Configuration.CONFIG_PROVIDERS_KEY 
          + "=SystemProperties,CommandLine"
    };
    
    try (final Configuration config = new Configuration(cli_args)) {
      try {
        config.register((ConfigurationEntrySchema) null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
          .setKey("tsd.conf")
          .setDefaultValue(1L)
          .setType(long.class)
          .setSource("ut")
          .build();
      config.register(schema);
      assertEquals(1L, (long) config.getTyped("tsd.conf", long.class));
      
      // register and pull from previous
      schema = ConfigurationEntrySchema.newBuilder()
          .setKey("my.custom.key")
          .setDefaultValue(0L)
          .setType(long.class)
          .setSource("ut")
          .build();
      config.register(schema);
      assertEquals(42L, (long) config.getTyped("my.custom.key", long.class));
      
      schema = ConfigurationEntrySchema.newBuilder()
          .setKey("another.key")
          .setDefaultValue(0L)
          .setType(double.class)
          .setSource("ut")
          .build();
      config.register(schema);
      assertEquals(50.75, (double) config.getTyped(
          "another.key", double.class), 0.001);
      
      config.register(ConfigurationEntrySchema.newBuilder()
          .setKey("builder.key")
          .setDefaultValue(0L)
          .setType(long.class)
          .setSource("ut"));
      assertEquals(0L, (long) config.getTyped("builder.key", long.class));
      
      // already registered
      schema = ConfigurationEntrySchema.newBuilder()
          .setKey("tsd.conf")
          .setDefaultValue(1L)
          .setType(long.class)
          .setSource("ut")
          .build();
      try {
        config.register(schema);
        fail("Expected ConfigurationException");
      } catch (ConfigurationException e) { }
      
      // test primitive registrations
      assertFalse(config.hasProperty("key.string"));
      config.register("key.string", "Beeblebrox", true, "Testing");
      ConfigurationEntry entry = config.getEntry("key.string");
      assertEquals("Beeblebrox", entry.schema().getDefaultValue());
      assertEquals("Testing", entry.schema().getDescription());
      assertEquals(getClass().getCanonicalName(), entry.schema().getSource());
      assertNull(entry.schema().getTypeReference());
      assertEquals(String.class, entry.schema().getType());
      assertTrue(entry.schema().isNullable());
      assertTrue(entry.schema().isDynamic());
      assertFalse(entry.schema().isSecret());
      
      assertFalse(config.hasProperty("key.string2"));
      config.register("key.string2",null, true, "Testing");
      entry = config.getEntry("key.string2");
      assertNull(entry.schema().getDefaultValue());
      assertEquals("Testing", entry.schema().getDescription());
      assertEquals(getClass().getCanonicalName(), entry.schema().getSource());
      assertNull(entry.schema().getTypeReference());
      assertEquals(String.class, entry.schema().getType());
      assertTrue(entry.schema().isNullable());
      assertTrue(entry.schema().isDynamic());
      assertFalse(entry.schema().isSecret());
      
      assertFalse(config.hasProperty("key.int"));
      config.register("key.int", 42, true, "Testing");
      entry = config.getEntry("key.int");
      assertEquals(42, entry.schema().getDefaultValue());
      assertEquals("Testing", entry.schema().getDescription());
      assertEquals(getClass().getCanonicalName(), entry.schema().getSource());
      assertNull(entry.schema().getTypeReference());
      assertEquals(int.class, entry.schema().getType());
      assertFalse(entry.schema().isNullable());
      assertTrue(entry.schema().isDynamic());
      assertFalse(entry.schema().isSecret());
      
      assertFalse(config.hasProperty("key.long"));
      config.register("key.long", 42L, true, "Testing");
      entry = config.getEntry("key.long");
      assertEquals(42L, entry.schema().getDefaultValue());
      assertEquals("Testing", entry.schema().getDescription());
      assertEquals(getClass().getCanonicalName(), entry.schema().getSource());
      assertNull(entry.schema().getTypeReference());
      assertEquals(long.class, entry.schema().getType());
      assertFalse(entry.schema().isNullable());
      assertTrue(entry.schema().isDynamic());
      assertFalse(entry.schema().isSecret());
      
      assertFalse(config.hasProperty("key.double"));
      config.register("key.double", 42.5D, true, "Testing");
      entry = config.getEntry("key.double");
      assertEquals(42.5D, (double) entry.schema().getDefaultValue(), 0.001);
      assertEquals("Testing", entry.schema().getDescription());
      assertEquals(getClass().getCanonicalName(), entry.schema().getSource());
      assertNull(entry.schema().getTypeReference());
      assertEquals(double.class, entry.schema().getType());
      assertFalse(entry.schema().isNullable());
      assertTrue(entry.schema().isDynamic());
      assertFalse(entry.schema().isSecret());
      
      assertFalse(config.hasProperty("key.bool"));
      config.register("key.bool", true, true, "Testing");
      entry = config.getEntry("key.bool");
      assertTrue((boolean) entry.schema().getDefaultValue());
      assertEquals("Testing", entry.schema().getDescription());
      assertEquals(getClass().getCanonicalName(), entry.schema().getSource());
      assertNull(entry.schema().getTypeReference());
      assertEquals(boolean.class, entry.schema().getType());
      assertFalse(entry.schema().isNullable());
      assertTrue(entry.schema().isDynamic());
      assertFalse(entry.schema().isSecret());
      
      assertFalse(config.hasProperty("key.bool2"));
      config.register("key.bool2", false, true, "Testing");
      entry = config.getEntry("key.bool2");
      assertFalse((boolean) entry.schema().getDefaultValue());
      assertEquals("Testing", entry.schema().getDescription());
      assertEquals(getClass().getCanonicalName(), entry.schema().getSource());
      assertNull(entry.schema().getTypeReference());
      assertEquals(boolean.class, entry.schema().getType());
      assertFalse(entry.schema().isNullable());
      assertTrue(entry.schema().isDynamic());
      assertFalse(entry.schema().isSecret());
      
      try {
        config.register(null, "Hi", false, "Testing");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("", "Hi", false, "Testing");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("key.foo", "Hi", false, null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("key.foo", "Hi", false, "");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register(null, 42, false, "Testing");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("", 42, false, "Testing");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("key.foo", 42, false, null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("key.foo", 42, false, "");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register(null, 42L, false, "Testing");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("", 42L, false, "Testing");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("key.foo", 42L, false, null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("key.foo", 42L, false, "");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register(null, 42.5, false, "Testing");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("", 42.5, false, "Testing");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("key.foo", 42.5, false, null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("key.foo", 42.5, false, "");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register(null, true, false, "Testing");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("", true, false, "Testing");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("key.foo", true, false, null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.register("key.foo", true, false, "");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
    }
  }
  
  @Test
  public void addOverride() throws Exception {
    final String[] cli_args = new String[] {
        "--my.custom.key=42",
        "--another.key=50.75",
        "--" + Configuration.CONFIG_PROVIDERS_KEY 
          + "=SystemProperties,CommandLine,RuntimeOverride"
    };
    
    try (final Configuration config = new Configuration(cli_args)) {
      ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
          .setKey("my.custom.key")
          .setDefaultValue(0L)
          .setType(long.class)
          .setSource("ut")
          .isDynamic()
          .build();
      config.register(schema);
      assertEquals(42L, (long) config.getTyped("my.custom.key", long.class));
      
      schema = ConfigurationEntrySchema.newBuilder()
          .setKey("another.key")
          .setDefaultValue(0L)
          .setType(double.class)
          .setSource("ut")
          .isDynamic()
          .build();
      config.register(schema);
      assertEquals(50.75, (double) config.getTyped(
          "another.key", double.class), 0.001);
      
      config.addOverride("another.key", 42.42);
      assertEquals(42.42, (double) config.getTyped(
          "another.key", double.class), 0.001);
      
      ConfigurationOverride setting = ConfigurationOverride.newBuilder()
          .setSource(RuntimeOverrideProvider.SOURCE)
          .setValue(24L)
          .build();
      
      try {
        config.addOverride(null, setting);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.addOverride("", setting);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.addOverride("my.custom.key", (ConfigurationOverride) null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      ValidationResult result = config.addOverride("my.custom.key", setting);
      assertTrue(result.isValid());
      assertEquals(24L, (long) config.getTyped("my.custom.key", long.class));
      
      try {
        config.addOverride("key.not.registered", setting);
        fail("Expected ConfigurationError");
      } catch (ConfigurationException e) { }
    }
  }
  
  @Test
  public void removeRuntimeOverride() throws Exception {
    final String[] cli_args = new String[] {
        "--my.custom.key=42",
        "--" + Configuration.CONFIG_PROVIDERS_KEY 
          + "=SystemProperties,CommandLine,RuntimeOverride"
    };
    
    try (final Configuration config = new Configuration(cli_args)) {
      ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
          .setKey("my.custom.key")
          .setDefaultValue(0L)
          .setType(long.class)
          .setSource("ut")
          .build();
      config.register(schema);
      
      schema = ConfigurationEntrySchema.newBuilder()
          .setKey("another.key")
          .setDefaultValue(0L)
          .setType(long.class)
          .setSource("ut")
          .isDynamic()
          .build();
      config.register(schema);
      assertEquals(0L, (long) config.getTyped("another.key", long.class));
      assertFalse(config.removeRuntimeOverride("another.key"));
      
      ValidationResult result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(42)
            .build());
      assertTrue(result.isValid());
      assertEquals(42L, (long) config.getTyped("another.key", long.class));
      
      assertTrue(config.removeRuntimeOverride("another.key"));
      assertEquals(0L, (long) config.getTyped("another.key", long.class));
    }
  }
  
  @Test
  public void bind() throws Exception {
    final String[] cli_args = new String[] {
        "--my.custom.key=42",
        "--" + Configuration.CONFIG_PROVIDERS_KEY 
          + "=SystemProperties,CommandLine,RuntimeOverride"
    };
    
    final long[] cb_value = new long[1];
    final String[] cb_key = new String[1];
    
    class CB implements ConfigurationCallback<Long> {
      @Override
      public void update(final String key, final Long value) {
        cb_value[0] = value;
        cb_key[0] = key;
      }
    }
    
    try (final Configuration config = new Configuration(cli_args)) {
      ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
          .setKey("another.key")
          .setDefaultValue(0)
          .setType(long.class)
          .setSource("ut")
          .isDynamic()
          .build();
      config.register(schema);
      assertEquals(0L, (long) config.getTyped("another.key", long.class));
      assertFalse(config.removeRuntimeOverride("another.key"));
      
      assertEquals(0, cb_value[0]);
      assertNull(cb_key[0]);
      
      config.bind("another.key", new CB());
      assertEquals(0, cb_value[0]);
      assertEquals("another.key", cb_key[0]);
      
      ValidationResult result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(42)
            .build());
      assertTrue(result.isValid());
      assertEquals(42L, (long) config.getTyped("another.key", long.class));
      
      assertEquals(42, cb_value[0]);
      assertEquals("another.key", cb_key[0]);
      
      assertTrue(config.removeRuntimeOverride("another.key"));
      assertEquals(0L, (long) config.getTyped("another.key", long.class));
      
      result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue("not a number")
            .build());
      assertFalse(result.isValid());
      assertEquals(0L, (long) config.getTyped("another.key", long.class));
      
      // test nulls
      schema = ConfigurationEntrySchema.newBuilder()
          .setKey("my.null.key")
          .setDefaultValue(null)
          .setType(String.class)
          .setSource("ut")
          .isNullable()
          .build();
      config.register(schema);
      assertNull(config.getTyped("my.null.key", String.class));
      
      final String[] string_value = new String[] { "Hi!" };
      assertEquals("Hi!", string_value[0]);
      assertEquals("another.key", cb_key[0]);
      
      class NullCB implements ConfigurationCallback<String> {
        @Override
        public void update(String key, String value) {
          cb_key[0] = key;
          string_value[0] = value;
        }
      }
      
      config.bind("my.null.key", new NullCB());
      assertNull(string_value[0]);
      assertEquals("my.null.key", cb_key[0]);
      
      // no such key
      try {
        config.bind("no.such.key", new NullCB());
        fail("Expected ConfigurationException");
      } catch (ConfigurationException e) { }
      
      // nulls
      try {
        config.bind(null, new NullCB());
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.bind("", new NullCB());
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.bind("another.key", null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
    }
  }
  
  @Test
  public void getTyped() throws Exception {
    try (final Configuration config = new Configuration(cli_args)) {
      ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
          .setKey("another.key")
          .setDefaultValue(42)
          .setType(String.class)
          .setSource("ut")
          .isNullable()
          .isDynamic()
          .build();
      config.register(schema);
      assertEquals(42L, (long) config.getTyped("another.key", long.class));
      assertEquals("42", config.getTyped("another.key", String.class));
      
      ValidationResult result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(null)
            .build());
      assertTrue(result.isValid());
      assertNull(config.getTyped("another.key", String.class));
      assertNull(config.getTyped("another.key", Long.class));
      try {
        config.getTyped("another.key", long.class);
        fail("Expected ConfigurationException");
      } catch (ConfigurationException e) { }
      
      final TypeReference<Map<String, List<String>>> type_ref = 
          new TypeReference<Map<String, List<String>>>() { };
      final Map<String, List<String>> mymap = Maps.newHashMap();
      mymap.put("key", Lists.newArrayList("Hi!"));
      
      schema = ConfigurationEntrySchema.newBuilder()
          .setKey("map.key")
          .setType(type_ref)
          .setSource("ut")
          .isNullable()
          .isDynamic()
          .build();
      config.register(schema);
      assertNull(config.getTyped("map.key", type_ref));
      
      result = config.addOverride("map.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(mymap)
            .build());
      assertTrue(result.isValid());
      final Map<String, List<String>> cmap = 
          config.getTyped("map.key", type_ref);
      assertEquals(1, cmap.size());
      assertEquals("Hi!", cmap.get("key").get(0));
      
      try {
        config.getTyped(null, long.class);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.getTyped("", long.class);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.getTyped("another.key", (Class<?>) null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.getTyped(null, type_ref);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.getTyped("", type_ref);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.getTyped("another.key", (TypeReference<?>) null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
    }
  }
  
  @Test
  public void getString() throws Exception {
    try (final Configuration config = new Configuration(cli_args)) {
      ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
          .setKey("another.key")
          .setDefaultValue("Kanakeredes")
          .setType(String.class)
          .setSource("ut")
          .isNullable()
          .isDynamic()
          .build();
      config.register(schema);
      assertEquals("Kanakeredes", config.getString("another.key"));
      
      ValidationResult result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(null)
            .build());
      assertTrue(result.isValid());
      assertNull(config.getString("another.key"));
      
      try {
        config.getString("no.such.key");
        fail("Expected ConfigurationException");
      } catch (ConfigurationException e) { }
      
      try {
        config.getString(null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.getString("");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
    }
  }
  
  @Test
  public void getInt() throws Exception {
    try (final Configuration config = new Configuration(cli_args)) {
      ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
          .setKey("another.key")
          .setDefaultValue("42")
          .setType(int.class)
          .setSource("ut")
          .isDynamic()
          .build();
      config.register(schema);
      assertEquals(42, config.getInt("another.key"));
      
      ValidationResult result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(24)
            .build());
      assertTrue(result.isValid());
      assertEquals(24, config.getInt("another.key"));
      
      result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(5.75) // truncates
            .build());
      assertTrue(result.isValid());
      assertEquals(5, config.getInt("another.key"));
      
      try {
        config.getInt("no.such.key");
        fail("Expected ConfigurationException");
      } catch (ConfigurationException e) { }
      
      try {
        config.getInt(null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.getInt("");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
    }
  }
  
  @Test
  public void getLong() throws Exception {
    try (final Configuration config = new Configuration(cli_args)) {
      ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
          .setKey("another.key")
          .setDefaultValue("42")
          .setType(long.class)
          .setSource("ut")
          .isDynamic()
          .build();
      config.register(schema);
      assertEquals(42, config.getLong("another.key"));
      
      ValidationResult result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(24)
            .build());
      assertTrue(result.isValid());
      assertEquals(24, config.getLong("another.key"));
      
      result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(5.75) // truncates
            .build());
      System.out.println(result.getMessage());
      assertTrue(result.isValid());
      assertEquals(5, config.getLong("another.key"));
      
      try {
        config.getLong("no.such.key");
        fail("Expected ConfigurationException");
      } catch (ConfigurationException e) { }
      
      try {
        config.getLong(null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.getLong("");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
    }
  }
  
  @Test
  public void getFloat() throws Exception {
    try (final Configuration config = new Configuration(cli_args)) {
      ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
          .setKey("another.key")
          .setDefaultValue("42")
          .setType(float.class)
          .setSource("ut")
          .isDynamic()
          .build();
      config.register(schema);
      assertEquals(42., config.getFloat("another.key"), 0.001);
      
      ValidationResult result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(-1024.99999)
            .build());
      assertTrue(result.isValid());
      assertEquals(-1024.99999, config.getFloat("another.key"), 0.001);
      
      result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(5.75)
            .build());
      assertTrue(result.isValid());
      assertEquals(5.75, config.getFloat("another.key"), 0.001);
      
      try {
        config.getFloat("no.such.key");
        fail("Expected ConfigurationException");
      } catch (ConfigurationException e) { }
      
      try {
        config.getFloat(null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.getFloat("");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
    }
  }
  
  @Test
  public void getDouble() throws Exception {
    try (final Configuration config = new Configuration(cli_args)) {
      ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
          .setKey("another.key")
          .setDefaultValue("42")
          .setType(double.class)
          .setSource("ut")
          .isDynamic()
          .build();
      config.register(schema);
      assertEquals(42., config.getDouble("another.key"), 0.001);
      
      ValidationResult result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(-1024.99999)
            .build());
      assertTrue(result.isValid());
      assertEquals(-1024.99999, config.getDouble("another.key"), 0.001);
      
      result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(5.75)
            .build());
      assertTrue(result.isValid());
      assertEquals(5.75, config.getDouble("another.key"), 0.001);
      
      try {
        config.getDouble("no.such.key");
        fail("Expected ConfigurationException");
      } catch (ConfigurationException e) { }
      
      try {
        config.getDouble(null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.getDouble("");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
    }
  }
  
  @Test
  public void getBoolean() throws Exception {
    try (final Configuration config = new Configuration(cli_args)) {
      ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
          .setKey("another.key")
          .setDefaultValue("true")
          .setType(boolean.class)
          .setSource("ut")
          .isDynamic()
          .build();
      config.register(schema);
      assertTrue(config.getBoolean("another.key"));
      
      ValidationResult result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(false)
            .build());
      assertTrue(result.isValid());
      assertFalse(config.getBoolean("another.key"));
      
      result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue(true)
            .build());
      assertTrue(result.isValid());
      assertTrue(config.getBoolean("another.key"));
      
      result = config.addOverride("another.key", 
          ConfigurationOverride.newBuilder()
            .setSource(RuntimeOverrideProvider.SOURCE)
            .setValue("")
            .build());
      assertTrue(result.isValid());
      assertFalse(config.getBoolean("another.key"));
      
      try {
        config.getBoolean("no.such.key");
        fail("Expected ConfigurationException");
      } catch (ConfigurationException e) { }
      
      try {
        config.getBoolean(null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.getBoolean("");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
    }
  }
  
  @Test
  public void hasKey() throws Exception {
    try (final Configuration config = new Configuration(cli_args)) {
      ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
          .setKey("another.key")
          .setDefaultValue("true")
          .setType(boolean.class)
          .setSource("ut")
          .isDynamic()
          .build();
      config.register(schema);
      assertTrue(config.hasProperty("another.key"));
      assertFalse(config.hasProperty("no.such.key"));
      
      try {
        config.hasProperty(null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        config.hasProperty("");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
    }
  }
  
  @Test
  public void asUnsecuredMap() throws Exception {
    final String[] cli_args = new String[] {
        "--my.custom.key=Super Secret!",
        "--another.key=50.75",
        "--" + Configuration.CONFIG_PROVIDERS_KEY 
          + "=SystemProperties,CommandLine"
    };
    
    try (final Configuration config = new Configuration(cli_args)) {
      // just the defaults before registration.
      Map<String, String> map = config.asUnsecuredMap();
      assertEquals(2, map.size());
      assertEquals("SystemProperties,CommandLine", map.get("config.providers"));
      assertEquals("300", map.get("config.reload.interval"));
      
      ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
          .setKey("tsd.conf")
          .setDefaultValue(1L)
          .setType(long.class)
          .setSource("ut")
          .build();
      config.register(schema);
      assertEquals(1L, (long) config.getTyped("tsd.conf", long.class));
      
      // register and pull from previous
      schema = ConfigurationEntrySchema.newBuilder()
          .setKey("my.custom.key")
          .setDefaultValue("My secret!")
          .setType(String.class)
          .setSource("ut")
          .isSecret()
          .build();
      config.register(schema);
      assertEquals("Super Secret!", (String) config.getTyped("my.custom.key", String.class));
      
      schema = ConfigurationEntrySchema.newBuilder()
          .setKey("another.key")
          .setDefaultValue(0L)
          .setType(double.class)
          .setSource("ut")
          .build();
      config.register(schema);
      assertEquals(50.75, (double) config.getTyped(
          "another.key", double.class), 0.001);
      
      config.register(ConfigurationEntrySchema.newBuilder()
          .setKey("null.key")
          .setDefaultValue(null)
          .setType(String.class)
          .setSource("ut")
          .isNullable());
      assertNull(config.getTyped("null.key", String.class));
      
      // now just the bits we have registered, minus the nulls.
      map = config.asUnsecuredMap();
      assertEquals(5, map.size());
      assertEquals("SystemProperties,CommandLine", map.get("config.providers"));
      assertEquals("300", map.get("config.reload.interval"));
      assertEquals("1", map.get("tsd.conf"));
      assertEquals("Super Secret!", map.get("my.custom.key"));
      assertEquals("50.75", map.get("another.key"));
    }
  }
  
  @Test
  public void close() throws Exception {
    final String[] cli_args = new String[] {
        "--" + Configuration.CONFIG_PROVIDERS_KEY 
          + "=http://yahoo.com,CommandLine",
    };
    
    final Configuration config = new Configuration(cli_args);
    boolean found_provider = false;
    for (final Provider provider : config.providers) {
      if (provider instanceof DummyHttpProvider) {
        assertFalse(((DummyHttpProvider) provider).closed);
        found_provider = true;
      }
    }
    
    boolean found_factory = false;
    for (final ProviderFactory factory : config.factories) {
      if (factory instanceof DummyHttpSourceFactory) {
        assertFalse(((DummyHttpSourceFactory) factory).closed);
        found_factory = true;
      }
    }
    
    assertTrue(found_provider);
    assertTrue(found_factory);

    config.close();
    found_provider = false;
    for (final Provider provider : config.providers) {
      if (provider instanceof DummyHttpProvider) {
        assertTrue(((DummyHttpProvider) provider).closed);
        found_provider = true;
      }
    }
    
     found_factory = false;
    for (final ProviderFactory factory : config.factories) {
      if (factory instanceof DummyHttpSourceFactory) {
        assertTrue(((DummyHttpSourceFactory) factory).closed);
        found_factory = true;
      }
    }
    
    assertTrue(found_provider);
    assertTrue(found_factory);
  }
  
  public static class DummyHttpProvider extends Provider {
    boolean closed = false;
    
    public DummyHttpProvider(ProviderFactory factory, Configuration config,
        HashedWheelTimer timer, Set<String> reload_keys) {
      super(factory, config, timer, reload_keys);
      // TODO Auto-generated constructor stub
    }

    @Override
    public ConfigurationOverride getSetting(String key) { return null; }

    @Override
    public String source() { return null; }

    @Override
    public void close() throws IOException { closed = true; }

    @Override
    public void reload() { }
  }
  
  public static class DummyHttpSourceFactory implements ProtocolProviderFactory {
    boolean closed = false;
    
    @Override
    public boolean handlesProtocol(final String uri) {
      return uri.startsWith("http");
    }

    @Override
    public String protocol() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String description() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void close() throws IOException { closed = true; }

    @Override
    public boolean isReloadable() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Provider newInstance(Configuration config, HashedWheelTimer timer,
        Set<String> reload_keys) {
      return new DummyHttpProvider(this, config, timer, reload_keys);
    }

    @Override
    public Provider newInstance(Configuration config, HashedWheelTimer timer,
        Set<String> reload_keys, String uri) {
      return new DummyHttpProvider(this, config, timer, reload_keys);
    }

    @Override
    public String simpleName() {
      return getClass().getSimpleName();
    }
    
  }

  public static class DummyNullInstanceFactory implements ProviderFactory {

    @Override
    public void close() throws IOException { }

    @Override
    public Provider newInstance(Configuration config, HashedWheelTimer timer,
        Set<String> reload_keys) {
      // this will cause an error
      return null;
    }

    @Override
    public boolean isReloadable() { return false; }

    @Override
    public String description() { return null; }

    @Override
    public String simpleName() {
      return getClass().getSimpleName();
    }
    
  }
  
  public static class DummyExceptionOnInstanceFactory implements ProviderFactory {

    @Override
    public void close() throws IOException { }

    @Override
    public Provider newInstance(Configuration config, HashedWheelTimer timer,
        Set<String> reload_keys) {
      // this will cause an error
      throw new RuntimeException("OOPSS! Couldn't instantiate me!");
    }

    @Override
    public boolean isReloadable() { return false; }

    @Override
    public String description() { return null; }

    @Override
    public String simpleName() {
      return getClass().getSimpleName();
    }
    
  }
  
}
