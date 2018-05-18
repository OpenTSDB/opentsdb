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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.ConfigurationValueValidator.ValidationResult;
import net.opentsdb.configuration.provider.CommandLineProvider;
import net.opentsdb.configuration.provider.CommandLineProvider.CommandLine;
import net.opentsdb.configuration.provider.EnvironmentProvider;
import net.opentsdb.configuration.provider.ProtocolProviderFactory;
import net.opentsdb.configuration.provider.Provider;
import net.opentsdb.configuration.provider.ProviderFactory;
import net.opentsdb.configuration.provider.RuntimeOverrideProvider;
import net.opentsdb.configuration.provider.RuntimeOverrideProvider.RuntimeOverride;
import net.opentsdb.configuration.provider.SystemPropertiesProvider;
import net.opentsdb.utils.ArgP;
import net.opentsdb.utils.PluginLoader;
import net.opentsdb.utils.StringUtils;
import net.opentsdb.utils.Threads;

/**
 * A configuration framework that provides overriding and flattening of
 * configurations from various sources as well as variable binding, 
 * conversion and reloading. This class is a key => value map of simple
 * string keys to primitive or complex objects. 
 * <p>
 * To start the configuration, at least one parameter must be supplied
 * via either command line, environment or system properties. That parameter
 * is the 'config.providers' defined in {@link #CONFIG_PROVIDERS_KEY}.
 * It is a list of configuration sources to parse for data in an ordered
 * manner. Each entry is either a {@link ProviderFactory} simple name,
 * a {@link ProviderFactory} canonical class name or a URI with a schema
 * that can be loaded by a {@link ProtocolProviderFactory}.
 * <p>
 * For example 'PropertiesFile,Environment,SystemProperties,CommandLine,RuntimeOverride'
 * means load the old style OpenTSDB default files via the 
 * {@link PropertiesFileFactory} factory, then parse the environment
 * variables and override any duplicates from the properties, then parse
 * the system properties and command line in that order. The final
 * {@link RuntimeOverrideProvider} allows the application to insert
 * overrides during runtime (if a schema is dynamic). Runtime overrides
 * override command line values, which override system property values
 * and so on.
 * <p>
 * To use the class, simply call {@link #Configuration()} or 
 * {@link #Configuration(String[])} (passing in command line args).
 * Next, register a {@link ConfigurationEntrySchema} with the configuration
 * so that it knows the type, default value and a useful description to
 * give users.
 * <b>NOTE:</b> Without this schema, exceptions are thrown when trying
 * to read or override a key.
 * After registering a schema, the providers are checked, in order, to
 * see if they had any values for the registered key. If so, they're 
 * loaded into the config map and calls to a read function will return
 * the properly overridden value.
 * To override the value at runtime, call 
 * {@link #addOverride(String, ConfigurationOverride)} with a source of
 * {@link RuntimeOverrideProvider#SOURCE}. 
 * <p>
 * For schemas marked {@link ConfigurationEntrySchema#isDynamic()}, a 
 * callback can be registered with the configuration to be notified of
 * updates. Simply call {@link Configuration#bind(String, ConfigurationCallback)}
 * and any time an override is registered or a variable is updated, the
 * function will be called with the proper override value. (Note that 
 * the function may still be called if a source is updated downstream
 * from the most current override, but the proper override will be given).
 * <p>
 * <b>NOTE:</b> Remember to close the configuration when shutting down
 * your application. A timer thread runs within the config any time a
 * provider is given that is reloadable.
 * <p>
 * <b>NOTE:</b> When implementing providers, validators or variables, 
 * there is a flag, {@link ConfigurationEntrySchema#isSecret()} that will
 * obfuscate secret values when logging or accessing the {@link #getView()}
 * method. Be careful to check for that when modifying the code.
 * 
 * @since 3.0
 */
public class Configuration implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);
  
  /**
   * Jackson de/serializer initialized, configured and shared in order
   * to use the converter for handling type conversion.
   */
  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static {
    // allows parsing NAN and such without throwing an exception. This is
    // important
    // for incoming data points with multiple points per put so that we can
    // toss only the bad ones but keep the good
    OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
  }
  
  /** Key, default and a validator for the provider configuration. */
  public static final String CONFIG_PROVIDERS_KEY = "config.providers";
  public static final String CONFIG_PROVIDERS_DEFAULT = 
      "PropertiesFile,Environment,SystemProperties,CommandLine,RuntimeOverride";
  protected static final Pattern CONFIG_PROVIDERS_REX = 
      Pattern.compile("^[\\w+\\d+,\\/\\.:\\|\\\\\\- ]+$");
  
  /** Key and validator for the plugin path. */
  public static final String PLUGIN_DIRECTORY_KEY = "config.plugin.path";
  public static final Pattern PLUGIN_DIRECTORY_REX = 
      Pattern.compile("^[\\w+\\d+\\/\\.\\\\:\\-]+$");
  
  /** Key for the reload interval. */
  public static final String CONFIG_RELOAD_INTERVAL_KEY = "config.reload.interval";
  
  /** The main configuration. Everything works off this.*/
  protected final Map<String, ConfigurationEntry> merged_config;
  
  /**
   * The list of config sources instantiated in order from most significant
   * to least. (Meaning idx 0 overrides idx 1, etc).
   */
  protected final List<Provider> providers;
  
  /** The parsed provider config string. */
  protected String provider_config;
  
  /** The parsed plugin path string. */
  protected String plugin_path;
  
  /** A list of the factories that we've found. */
  protected List<ProviderFactory> factories;
  
  /** A list of keys that should be updated by providers when they reload. */
  protected final Set<String> reload_keys;
  
  /** The timer that executes provider reloads. */
  protected HashedWheelTimer timer;
  
  /**
   * The default ctor that searches the environment and system properties
   * for the 'config.providers' list.
   * 
   * @throws IllegalArgumentException if some required parameter was invalid.
   * @throws ConfigurationException if the configuration could not be 
   * loaded.
   */
  public Configuration() {
    this(new String[0]);
  }
  
  /**
   * The ctor used when running as tool that needs to parse command line
   * parameters.
   * 
   * @param cli_args A non-null list of zero or more command line parameters.
   * @throws IllegalArgumentException if the cli args were null or some 
   * required parameter was invalid.
   * @throws ConfigurationException if the configuration could not be 
   * loaded.
   */
  public Configuration(final String[] cli_args) {
    if (cli_args == null) {
      throw new IllegalArgumentException("CLI arguments cannot be null.");
    }
    
    merged_config = Maps.newConcurrentMap();
    
    // register locals first, can't use {@code register} as it would fail
    ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey(CONFIG_PROVIDERS_KEY)
        .setDefaultValue(CONFIG_PROVIDERS_DEFAULT)
        .setType(String.class)
        .notNullable()
        .setSource(getClass().toString())
        .setDescription("An ordered list of configuration providers to "
            + "load. The list must be comma separated and can include: "
            + "The shortname of a configuration provider factory, "
            + "the canonical name of a provider factory, or a URI "
            + "with schema referring to a configuration loadable "
            + "and parseable by a factory. E.g. "
            + "'file:///opt/opentsdb/opentsdb.conf,Environment,CommandLine'. "
            + "Configurations are processed in least significant to most "
            + "significant order. In the example given, the 'opentsdb.conf' "
            + "file would be loaded first, next Environment variables would "
            + "override those in the file and finally command line values "
            + "would override the previous values.")
        .build();
    ConfigurationEntry entry = new ConfigurationEntry(this);
    entry.setSchema(schema);
    merged_config.putIfAbsent(schema.key, entry);
    
    schema = ConfigurationEntrySchema.newBuilder()
        .setKey(PLUGIN_DIRECTORY_KEY)
        .setType(String.class)
        .isNullable()
        .setSource(getClass().toString())
        .setDescription("The path to a directory containing .JAR files "
            + "that should be loaded during initialization of the "
            + "application to search for configuration provider "
            + "implementations.")
        .build();
    entry = new ConfigurationEntry(this);
    entry.setSchema(schema);
    merged_config.putIfAbsent(schema.key, entry);
    
    reload_keys = Sets.newConcurrentHashSet();
    
    // set this up first so we can schedule
    timer = new HashedWheelTimer();
    
    providers = Lists.newArrayList();
    loadInitialConfig(cli_args);
    loadPlugins();
    loadProviders(cli_args);
    configureReloads();
  }

  /**
   * Helper to register a schema builder. 
   * See {@link #register(ConfigurationEntrySchema)}
   * @param builder A non-null builder.
   */
  public void register(final ConfigurationEntrySchema.Builder builder) {
    if (builder == null) {
      throw new IllegalArgumentException("Builder cannot be null.");
    }
    register(builder.build());
  }
  
  /**
   * Registers the config schema with the configuration. This should be
   * called by the TSD and plugins to associate actual values with a
   * type and validator.
   * <p>
   * If the schema is marked as {@link ConfigurationEntrySchema#isDynamic()}
   * then the key is also added to the reloadable keys for the providers
   * to override on reloads.
   * 
   * @param schema A non-null schema fully configured.
   * @throws IllegalArgumentException if the given schema was null.
   * @throws ConfigurationException if the schema was already registered.
   */
  public void register(final ConfigurationEntrySchema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("Schema cannot be null.");
    }
    
    final ConfigurationEntry entry = new ConfigurationEntry(this);
    entry.setSchema(schema);
    final ConfigurationEntry extant = merged_config.putIfAbsent(schema.key, entry);
    if (extant != null) {
      throw new ConfigurationException("Schema already exists for "
          + "key: " + schema.key);
    }
    
    // pull from previous sources in order
    for (int i = providers.size() - 1; i >= 0; i--) {
      if (schema.isDynamic()) {
        reload_keys.add(schema.getKey());
      }
      
      final ConfigurationOverride setting = providers.get(i)
          .getSetting(schema.key);
      if (setting != null) {
        entry.addOverride(setting);
      }
    }
  }

  /**
   * Registers a configuration schema with the type {@link String} with
   * the source set to the class name of the caller and the schema
   * marked as nullable.
   * 
   * @param key A non-null and non-empty key.
   * @param default_value A default value, may be null.
   * @param is_dynamic Whether or not the value can be overridden.
   * @param description A non-null and non-empty description.
   * @throws IllegalArgumentException if the key or description was 
   * null or empty.
   * @throws ConfigurationException if the key was already registered. 
   */
  public void register(final String key, 
                       final String default_value, 
                       final boolean is_dynamic,
                       final String description) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    if (Strings.isNullOrEmpty(description)) {
      throw new IllegalArgumentException("Description cannot be null or "
          + "empty. Help the users!");
    }
    final ConfigurationEntrySchema.Builder builder = 
        ConfigurationEntrySchema.newBuilder()
        .setKey(key)
        .setDefaultValue(default_value)
        .setType(String.class)
        .setSource(Threads.getCallerCallerClassName())
        .isNullable()
        .setDescription(description);
    if (is_dynamic) {
      builder.isDynamic();
    }
    
    register(builder.build());
  }
  
  /**
   * Registers a configuration schema with the type {@link int} with
   * the source set to the class name of the caller and the schema
   * marked as not-nullable.
   * 
   * @param key A non-null and non-empty key.
   * @param default_value A default value.
   * @param is_dynamic Whether or not the value can be overridden.
   * @param description A non-null and non-empty description.
   * @throws IllegalArgumentException if the key or description was 
   * null or empty.
   * @throws ConfigurationException if the key was already registered. 
   */
  public void register(final String key, 
                       final int default_value, 
                       final boolean is_dynamic,
                       final String description) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    if (Strings.isNullOrEmpty(description)) {
      throw new IllegalArgumentException("Description cannot be null or "
          + "empty. Help the users!");
    }
    final ConfigurationEntrySchema.Builder builder = 
        ConfigurationEntrySchema.newBuilder()
        .setKey(key)
        .setDefaultValue(default_value)
        .setType(int.class)
        .setSource(Threads.getCallerCallerClassName())
        .setDescription(description);
    if (is_dynamic) {
      builder.isDynamic();
    }
    
    register(builder.build());
  }
  
  /**
   * Registers a configuration schema with the type {@link long} with
   * the source set to the class name of the caller and the schema
   * marked as not-nullable.
   * 
   * @param key A non-null and non-empty key.
   * @param default_value A default value.
   * @param is_dynamic Whether or not the value can be overridden.
   * @param description A non-null and non-empty description.
   * @throws IllegalArgumentException if the key or description was 
   * null or empty.
   * @throws ConfigurationException if the key was already registered. 
   */
  public void register(final String key, 
                       final long default_value, 
                       final boolean is_dynamic,
                       final String description) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    if (Strings.isNullOrEmpty(description)) {
      throw new IllegalArgumentException("Description cannot be null or "
          + "empty. Help the users!");
    }
    final ConfigurationEntrySchema.Builder builder = 
        ConfigurationEntrySchema.newBuilder()
        .setKey(key)
        .setDefaultValue(default_value)
        .setType(long.class)
        .setSource(Threads.getCallerCallerClassName())
        .setDescription(description);
    if (is_dynamic) {
      builder.isDynamic();
    }
    
    register(builder.build());
  }
  
  /**
   * Registers a configuration schema with the type {@link double} with
   * the source set to the class name of the caller and the schema
   * marked as not-nullable.
   * 
   * @param key A non-null and non-empty key.
   * @param default_value A default value.
   * @param is_dynamic Whether or not the value can be overridden.
   * @param description A non-null and non-empty description.
   * @throws IllegalArgumentException if the key or description was 
   * null or empty.
   * @throws ConfigurationException if the key was already registered. 
   */
  public void register(final String key, 
                       final double default_value, 
                       final boolean is_dynamic,
                       final String description) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    if (Strings.isNullOrEmpty(description)) {
      throw new IllegalArgumentException("Description cannot be null or "
          + "empty. Help the users!");
    }
    final ConfigurationEntrySchema.Builder builder = 
        ConfigurationEntrySchema.newBuilder()
        .setKey(key)
        .setDefaultValue(default_value)
        .setType(double.class)
        .setSource(Threads.getCallerCallerClassName())
        .setDescription(description);
    if (is_dynamic) {
      builder.isDynamic();
    }
    
    register(builder.build());
  }
  
  /**
   * Registers a configuration schema with the type {@link boolean} with
   * the source set to the class name of the caller and the schema
   * marked as not-nullable.
   * 
   * @param key A non-null and non-empty key.
   * @param default_value A default value.
   * @param is_dynamic Whether or not the value can be overridden.
   * @param description A non-null and non-empty description.
   * @throws IllegalArgumentException if the key or description was 
   * null or empty.
   * @throws ConfigurationException if the key was already registered. 
   */
  public void register(final String key, 
                       final boolean default_value, 
                       final boolean is_dynamic,
                       final String description) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    if (Strings.isNullOrEmpty(description)) {
      throw new IllegalArgumentException("Description cannot be null or "
          + "empty. Help the users!");
    }
    final ConfigurationEntrySchema.Builder builder = 
        ConfigurationEntrySchema.newBuilder()
        .setKey(key)
        .setDefaultValue(default_value)
        .setType(boolean.class)
        .setSource(Threads.getCallerCallerClassName())
        .setDescription(description);
    if (is_dynamic) {
      builder.isDynamic();
    }
    
    register(builder.build());
  }
  
  /**
   * Helper to register an override builder. See 
   * {@link #addOverride(String, ConfigurationOverride)}.
   * 
   * @param keyA non-null and non-empty key.
   * @param builder A non-null builder.
   * @return A non-null validation result.
   * @throws IllegalArgumentException if the key was null or empty, or
   * the setting was null.
   */
  public ValidationResult addOverride(final String key, 
                                      final ConfigurationOverride.Builder builder) {
    if (builder == null) {
      throw new IllegalArgumentException("Builder cannot be null.");
    }
    return addOverride(key, builder.build());
  }
  
  /**
   * Registers an override with the config. The response includes whether
   * or not the override passed validation or failed. On failure, the
   * override is NOT saved in the config. See 
   * {@link ValidationResult#isValid()}.
   * <b>Note:</b> This should only be used by {@link Provider} 
   * implementations on reloads. For runtime overrides, use 
   * {@link #addOverride(String, Object)}.
   * 
   * @param key A non-null and non-empty key.
   * @param override A non-null setting.
   * @return A non-null validation result.
   * @throws IllegalArgumentException if the key was null or empty, or
   * the setting was null.
   * @throws ConfigurationException if no schema was present for the 
   * given key.
   */
  public ValidationResult addOverride(final String key, 
                                      final ConfigurationOverride override) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    if (override == null) {
      throw new IllegalArgumentException("Override cannot be null.");
    }
    final ConfigurationEntry entry = merged_config.get(key);
    if (entry == null) {
      throw new ConfigurationException("Cannot store a override before "
          + "storing a schema.");
    }
    return entry.addOverride(override);
  }
  
  /**
   * Registers an override with the config as a {@link RuntimeOverrideProvider#SOURCE}.
   * The response includes whether or not the override passed validation 
   * or failed. On failure, the override is NOT saved in the config. See 
   * {@link ValidationResult#isValid()}.
   * 
   * @param key A non-null and non-empty key.
   * @param value The override. May be null when appropriate.
   * IllegalArgumentException if the key was null or empty, or
   * the setting was null.
   * @throws ConfigurationException if no schema was present for the 
   * given key.
   */
  public ValidationResult addOverride(final String key, 
                                      final Object value) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    final ConfigurationEntry entry = merged_config.get(key);
    if (entry == null) {
      throw new ConfigurationException("Cannot store a override before "
          + "storing a schema.");
    }
    return entry.addOverride(ConfigurationOverride.newBuilder()
        .setSource(RuntimeOverrideProvider.SOURCE)
        .setValue(value)
        .build());
  }
  
  /**
   * Removes the given runtime override from the settings.
   * 
   * @param key A non-null and non-empty key.
   * @return True if the setting was found and removed, false if an 
   * override was not found.
   */
  public boolean removeRuntimeOverride(final String key) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    final ConfigurationEntry entry = merged_config.get(key);
    if (entry == null) {
      return false;
    }
    return entry.removeRuntimeOverride();
  }
  
  /**
   * Attaches the given callback to the key so that the caller can 
   * receive updates any time the flattened value has changed to a new
   * value. Note that callback processing order is indeterminate.
   * 
   * @param key A non-null and non-empty config key entry.
   * @param callback A non-null callback.
   * @throws IllegalArgumentException if the key was null or empty or the
   * callback was null.
   * @throws ConfigurationException if the key did nost exist.
   */
  public void bind(final String key, 
                   final ConfigurationCallback<?> callback) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    if (callback == null) {
      throw new IllegalArgumentException("Callback cannot be null.");
    }
    
    final ConfigurationEntry entry = merged_config.get(key);
    if (entry == null) {
      throw new ConfigurationException("No registration found for key: " + key);
    }
    entry.addCallback(callback);
  }
  
  /**
   * Returns the given config value cast to the given type (if possible).
   * Use this method for complex objects like maps or POJOs. Uses 
   * Jackson's {@link ObjectMapper} to handle conversion. Thus it's not
   * ideal to call this incredibly frequently. Cache the results when
   * possible.
   * 
   * @param key The non-null and non-empty config key entry.
   * @param type A non-null class to cast to.
   * @return The value found, may be null if set to null for non-primitive
   * types.
   * @throws IllegalArgumentException if the key was null or empty or
   * the data types were incompatible. Also if a primitive type was
   * given and the value was null.
   * @throws ConfigurationException if the key was not registered.
   */
  @SuppressWarnings("unchecked")
  public <T> T getTyped(final String key, final Class<?> type) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    
    final ConfigurationEntry entry = merged_config.get(key);
    if (entry == null) {
      throw new ConfigurationException("No registration found for key: " + key);
    }
    
    final Object value = entry.getValue();
    if (value == null) {
      if (type.isPrimitive()) {
        throw new ConfigurationException("Cannot cast null to a "
            + "primitive type: " + type);
      }
      return null;
    }
    
    if (!value.getClass().equals(type)) {
      return (T) OBJECT_MAPPER.convertValue(value, type);
    }
    return (T) entry.getValue();
  }
  
  /**
   * Returns the given config value cast to the given type (if possible).
   * Use this method for complex objects like maps or POJOs. Uses 
   * Jackson's {@link ObjectMapper} to handle conversion. Thus it's not
   * ideal to call this incredibly frequently. Cache the results when
   * possible.
   * 
   * @param key The non-null and non-empty config key entry.
   * @param type A non-null class to cast to.
   * @return The value found, may be null if set to null for non-primitive
   * types.
   * @throws IllegalArgumentException if the key was null or empty or
   * the data types were incompatible. Also if a primitive type was
   * given and the value was null.
   * @throws ConfigurationException if the key was not registered.
   */
  @SuppressWarnings("unchecked")
  public <T> T getTyped(final String key, final TypeReference<?> type) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    
    final ConfigurationEntry entry = merged_config.get(key);
    if (entry == null) {
      throw new ConfigurationException("No registration found for key: " + key);
    }
    
    final Object value = entry.getValue();
    if (value == null) {
      return null;
    }
    
    return (T) OBJECT_MAPPER.convertValue(value, type);
  }
  
  /**
   * Returns the value, if found, as a string. Numbers are cast to strings
   * and complex objects will throw an IllegalArgumentException. If the 
   * value was null, a null is returned.
   * 
   * @param key The non-null and non-empty config key entry.
   * @return A String if the entry had a value, null if it was set to null.
   * @throws IllegalArgumentException if the key was null or empty.
   * @throws ConfigurationException if the key did not exist in the
   * config.
   */
  public String getString(final String key) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    return getTyped(key, String.class);
  }
  
  /**
   * Returns the value as an integer when possible.
   * 
   * @param key A non-null and non-empty key.
   * @return An integer value.
   * @throws IllegalArgumentException if the key was null or empty.
   * @throws ConfigurationException if the key did not exist in the
   * config.
   */
  public int getInt(final String key) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    return (int) getTyped(key, int.class);
  }
  
  /**
   * Returns the value as a long integer when possible.
   * 
   * @param key A non-null and non-empty key.
   * @return A long integer value.
   * @throws IllegalArgumentException if the key was null or empty.
   * @throws ConfigurationException if the key did not exist in the
   * config.
   */
  public long getLong(final String key) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    return (long) getTyped(key, long.class);
  }
  
  /**
   * Returns the value as a float (single precision) when possible.
   * 
   * @param key A non-null and non-empty key.
   * @return A float value.
   * @throws IllegalArgumentException if the key was null or empty.
   * @throws ConfigurationException if the key did not exist in the
   * config.
   */
  public float getFloat(final String key) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    return (float) getTyped(key, float.class);
  }
  
  /**
   * Returns the value as a double (double precision) when possible.
   * 
   * @param key A non-null and non-empty key.
   * @return A double value.
   * @throws IllegalArgumentException if the key was null or empty.
   * @throws ConfigurationException if the key did not exist in the
   * config.
   */
  public double getDouble(final String key) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    return (double) getTyped(key, double.class);
  }
  
  /**
   * Checks to see if the value of the key is true or false. Nulls count
   * as false and only the values in the set [true, 1, yes] count as 
   * true (cast to lower case in string form).
   * 
   * @param key A non-null and non-empty key.
   * @return A boolean value.
   * @throws IllegalArgumentException if the key was null or empty.
   * @throws ConfigurationException if the key did not exist in the
   * config.
   */
  public boolean getBoolean(final String key) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String bool = getTyped(key, String.class);
    if (Strings.isNullOrEmpty(bool)) {
      return false;
    }
    bool = bool.toLowerCase().trim();
    if (bool.equals("true") ||
        bool.equals("1") ||
        bool.equals("yes")) {
      return true;
    }
    return false;
  }
  
  /**
   * Determines if the given key has been registered.
   * 
   * @param key A non-null and no-empty key.
   * @return True if the key was registered, false if not and calls
   * to read methods would throw an exception.
   * @throws IllegalArgumentException if the key was null or empty.
   */
  public boolean hasProperty(final String key) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    final ConfigurationEntry entry = merged_config.get(key);
    if (entry == null) {
      return false;
    }
    return entry.schema() != null;
  }
  
  /**
   * Returns a read-only view of the current state of the config. Creates
   * a snapshot with duplicate values so if the config is really large
   * use this with care.
   * 
   * @return A non-null configuration view to read through.
   */
  public ConfigurationView getView() {
    return new ConfigurationView(this);
  }
  
  /** @return <b>WARN</b> A non-obfuscated map with non-null settings
   * cast to strings. */
  public Map<String, String> asUnsecuredMap() {
    final Map<String, String> map = 
        Maps.newHashMapWithExpectedSize(merged_config.size());
    for (final Entry<String, ConfigurationEntry> entry : 
        merged_config.entrySet()) {
      final Object value = entry.getValue().getValue();
      if (value != null) {
        map.put(entry.getKey(), value.toString());        
      }
    }
    return Collections.unmodifiableMap(map);
  }
  
  @Override
  public void close() throws IOException {
    if (timer != null) {
      timer.stop();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stopped the timer.");
      }
    }
    
    for (final Provider provider : providers) {
      try {
        provider.close();
      } catch (Exception e) {
        LOG.error("Failed to close provider: " + provider);
      }
    }
    
    for (final ProviderFactory factory : factories) {
      try {
        factory.close();
      } catch (Exception e) {
        LOG.error("Failed to close provider: " + factory);
      }
    }
    LOG.info("Completed shutdown of config providers, factories and timer.");
  }
  
  /**
   * Helper method that attempts to load the initial providers key and
   * plugin path from the:
   * <ol><li>Environment</li>
   * <li>System properties (-D settings passed to the JVM)</li>
   * <li>Command line arguments</li></ol>
   * 
   * @param cli_args A non-null array of zero or more command line arguments.
   * @throws IllegalArgumentException if the args were null or if a value
   * read for one of the initial properties faild validation.
   * @throws ConfigurationException if the default providers list failed
   * to match the regular expression check.
   */
  private void loadInitialConfig(final String[] cli_args) {
    if (cli_args == null) {
      throw new IllegalArgumentException("CLI Args cannot be null.");
    }
    // Start with validating the defaults.
    provider_config = CONFIG_PROVIDERS_DEFAULT;
    if (!CONFIG_PROVIDERS_REX.matcher(provider_config).matches()) {
      throw new ConfigurationException("'" + CONFIG_PROVIDERS_KEY 
          + "' default value failed to pass the regex.");
    }
    
    // next read the environment variables
    String temp = System.getenv(CONFIG_PROVIDERS_KEY);
    if (!Strings.isNullOrEmpty(temp)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Read '" + CONFIG_PROVIDERS_DEFAULT + 
            "' from environment. Value: " + temp);
      }
      if (!CONFIG_PROVIDERS_REX.matcher(temp).matches()) {
        throw new IllegalArgumentException("'" + CONFIG_PROVIDERS_KEY 
            + "' environment value failed to pass the regex.");
      }
      provider_config = temp;
      addOverride(CONFIG_PROVIDERS_KEY, ConfigurationOverride.newBuilder()
          .setSource(EnvironmentProvider.SOURCE)
          .setValue(provider_config)
          .build());
    }
    
    temp = System.getenv(PLUGIN_DIRECTORY_KEY);
    if (!Strings.isNullOrEmpty(temp)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Read '" + PLUGIN_DIRECTORY_KEY + 
            "' from environment. Value: " + temp);
      }
      if (!PLUGIN_DIRECTORY_REX.matcher(temp).matches()) {
        throw new IllegalArgumentException("'" + PLUGIN_DIRECTORY_KEY 
            + "' environment value failed to pass the regex.");
      }
      plugin_path = temp;
      addOverride(PLUGIN_DIRECTORY_KEY, ConfigurationOverride.newBuilder()
          .setSource(EnvironmentProvider.SOURCE)
          .setValue(plugin_path)
          .build());
    }
    
    // Next check system properties
    temp = System.getProperty(CONFIG_PROVIDERS_KEY);
    if (!Strings.isNullOrEmpty(temp)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Read '" + CONFIG_PROVIDERS_KEY + 
            "' from JVM system propreties. Value: " + temp);
      }
      if (!CONFIG_PROVIDERS_REX.matcher(temp).matches()) {
        throw new IllegalArgumentException("'" + CONFIG_PROVIDERS_KEY 
            + "' JVM system value failed to pass the regex.");
      }
      provider_config = temp;
      addOverride(CONFIG_PROVIDERS_KEY, ConfigurationOverride.newBuilder()
          .setSource(SystemPropertiesProvider.SOURCE)
          .setValue(provider_config)
          .build());
    }
    
    temp = System.getProperty(PLUGIN_DIRECTORY_KEY);
    if (!Strings.isNullOrEmpty(temp)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Read '" + PLUGIN_DIRECTORY_KEY + 
            "' from JVM system properties. Value: " + temp);
      }
      if (!PLUGIN_DIRECTORY_REX.matcher(temp).matches()) {
        throw new IllegalArgumentException("'" + PLUGIN_DIRECTORY_KEY 
            + "' JVM system value failed to pass the regex.");
      }
      plugin_path = temp;
      addOverride(PLUGIN_DIRECTORY_KEY, ConfigurationOverride.newBuilder()
          .setSource(SystemPropertiesProvider.SOURCE)
          .setValue(plugin_path)
          .build());
    }
    
    // Finally we'll parse out the command line args. 
    final ArgP argp = new ArgP(false);
    argp.addOption("--" + CONFIG_PROVIDERS_KEY, "The comma separated list "
        + "of config sources in order from defaults to overrides.");
    argp.addOption("--" + PLUGIN_DIRECTORY_KEY, "A path to load plugins "
        + "from.");
    argp.parse(cli_args);
    
    if (argp.has("--" + CONFIG_PROVIDERS_KEY)) {
      temp = argp.get("--" + CONFIG_PROVIDERS_KEY);
      if (!Strings.isNullOrEmpty(temp)) {
        if (temp.contains("\"")) {
          temp = temp.replaceAll("\"", "");
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Read '" + CONFIG_PROVIDERS_KEY + 
              "' from command line arguments. Value: " + temp);
        }
        if (!CONFIG_PROVIDERS_REX.matcher(temp).matches()) {
          throw new IllegalArgumentException("'" + CONFIG_PROVIDERS_KEY 
              + "' command line value failed to pass the regex.");
        }
        provider_config = temp;
        addOverride(CONFIG_PROVIDERS_KEY, ConfigurationOverride.newBuilder()
            .setSource(CommandLineProvider.SOURCE)
            .setValue(provider_config)
            .build());
      }
    }
    
    if (argp.has("--" + PLUGIN_DIRECTORY_KEY)) {
      temp = argp.get("--" + PLUGIN_DIRECTORY_KEY);
      if (!Strings.isNullOrEmpty(temp)) {
        if (temp.contains("\"")) {
          temp = temp.replaceAll("\"", "");
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Read '" + PLUGIN_DIRECTORY_KEY + 
              "' from command line arguments. Value: " + temp);
        }
        if (!PLUGIN_DIRECTORY_REX.matcher(temp).matches()) {
          throw new IllegalArgumentException("'" + PLUGIN_DIRECTORY_KEY 
              + "' command line value failed to pass the regex.");
        }
        plugin_path = temp;
        addOverride(PLUGIN_DIRECTORY_KEY, ConfigurationOverride.newBuilder()
            .setSource(CommandLineProvider.SOURCE)
            .setValue(plugin_path)
            .build());
      }
    }
  }

  /**
   * Loads all of the factories from the class path under "net.opentsdb"
   * as well as plugins that implement the interface.
   * 
   * @throws ConfigurationException if loading failed, most likely due
   * to a bad constructor somewhere.
   */
  private void loadPlugins() {
    if (!Strings.isNullOrEmpty(plugin_path)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Loading plugins from directory: " + plugin_path);
      }
      try {
        PluginLoader.loadJARs(plugin_path);
      } catch (Exception e) {
        LOG.error("Failed to load plugins from path: " + plugin_path, e);
        throw new ConfigurationException("Unable to load plugins from path: " 
            + plugin_path, e);
      }
      
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Plugin path was empty, not loading external JARs.");
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Loading instances of '" 
          + ProtocolProviderFactory.class.getCanonicalName() + "'");
    }
    
    try {
      factories = PluginLoader.loadPlugins(ProviderFactory.class);
    } catch (Exception e) {
      throw new ConfigurationException("A plugin was found that threw "
          + "an exception on instantiation", e);
    }
    if (factories != null) {
      LOG.info("Loaded " + factories.size() + " provider factories.");
    } else {
      LOG.info("Loaded 0 provider factories.");
    }
  }
  
  /**
   * Dedupes the given provider key string and attempts to load providers
   * for each source.
   * 
   * @param cli_args A non-null list of cli arguments to pass to the 
   * command line provider if requested.
   */
  private void loadProviders(final String[] cli_args) {
    final String[] raw_sources = StringUtils.splitString(provider_config, ',');
    if (raw_sources.length < 1) {
      throw new IllegalArgumentException("No sources found!");
    }
    
    final Set<String> de_dupes = Sets.newHashSet();
    final List<String> sources = Lists.newArrayList();
    for (final String raw : raw_sources) {
      if (raw == null || raw.trim().isEmpty()) {
        continue;
      }
      if (de_dupes.contains(raw.trim())) {
        LOG.warn("Skipping duplicate source entry: " + raw.trim());
      } else {
        de_dupes.add(raw.trim());
        sources.add(raw.trim());
      }
    }
    
    // init in reverse order so remote sources can pull settings from 
    // local sources like the command line or environment
    for (int i = sources.size() - 1; i >= 0; i--) {
      final String source = sources.get(i);
      LOG.info("Attempting to load config source [" + source + "]");
      
      if (source.contains("://")) {
        if (factories == null) {
          throw new IllegalArgumentException("Unable to find a plugin "
              + "factory for source: " + source);
        }
        
        // it's a protocol source, so fetch it using the right class
        boolean matched = false;
        for (final ProviderFactory factory : factories) {
          if (factory instanceof ProtocolProviderFactory) {
            if (((ProtocolProviderFactory) factory).handlesProtocol(source)) {
              final Provider provider = ((ProtocolProviderFactory) factory)
                  .newInstance(this, timer, reload_keys, source);
              if (provider == null) {
                throw new ConfigurationException("Factory [" 
                    + factory + "] returned a null instance.");
              }
              providers.add(provider);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Instantiated the [" 
                    + provider.getClass().getSimpleName() 
                    + "] provider with source: " + source);
              }
              matched = true;
              break;
            }
          }
        }
        
        if (!matched) {
          throw new ConfigurationException("Unable to find a plugin "
              + "factory for source: " + source);
        }
        
      } else {
        if (CommandLine.class.getName().endsWith(source)) {
          // this is an outlier as it needs the cli args.
          providers.add(new CommandLineProvider(new CommandLine(), 
                                                this, 
                                                timer, 
                                                reload_keys, 
                                                cli_args));
          if (LOG.isDebugEnabled()) {
            LOG.debug("Instantiated the [CommandLine] provider.");
          }
        } else if (RuntimeOverride.class.getName().endsWith(source)) {
          // another outlier as unit tests that use Mockito or PowerMockito
          // may fail as it will prevent the implementations from loading
          // properly. Here we force instantiation.
          providers.add(new RuntimeOverrideProvider(new CommandLine(), 
                                                    this, 
                                                    timer, 
                                                    reload_keys));
          if (LOG.isDebugEnabled()) {
            LOG.debug("Instantiated the [CommandLine] provider.");
          }
        } else if (source.equals("UnitTest")) {
          // another outlier for unit tests.
          providers.add(new UnitTestConfiguration.UnitTest()
              .newInstance(this, timer, reload_keys));
          if (LOG.isDebugEnabled()) {
            LOG.debug("Instantiated the [UnitTest] provider.");
          }
        } else if (factories != null && !factories.isEmpty()) {
          boolean matched = false;
          // plugin source so find the plugin
          for (final ProviderFactory factory : factories) {
            if (factory.simpleName().equals(source) || 
                factory.getClass().getCanonicalName().equals(source)) {
              final Provider plugin = factory
                  .newInstance(this, timer, reload_keys);
              if (plugin == null) {
                throw new ConfigurationException("Factory [" 
                    + factory + "] returned a null instance.");
              }
              providers.add(plugin);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Instantiated the [" 
                    + plugin.getClass().getSimpleName() + "] provider.");
              }
              matched = true;
              break;
            }
          }
          
          if (!matched) {
            throw new ConfigurationException("Unable to find a plugin "
                + "factory for source: " + source);
          }
        } else {
          throw new ConfigurationException("Unable to find a plugin "
              + "factory for source: " + source);
        }
      }
    }
  }

  /**
   * Adds the {@link #CONFIG_RELOAD_INTERVAL_KEY} setting and for each
   * provider that returns true when {@link ProviderFactory#isReloadable()}
   * is called, schedules a timeout task. If no reloadable providers are
   * found then the timer is not started. But if one or more are found
   * then of course we start the timer.
   */
  private void configureReloads() {
    // we set this schema here as we can now load settings from providers.
    final ConfigurationEntrySchema schema = ConfigurationEntrySchema.newBuilder()
        .setKey(CONFIG_RELOAD_INTERVAL_KEY)
        .setType(long.class)
        .setDefaultValue(300)
        .setSource(getClass().toString())
        .setDescription("How often to refresh reloadable configuration "
            + "providers in seconds.")
        .build();
    register(schema);
    
    // start reloads for providers so configured.
    final long reload_interval = getTyped(CONFIG_RELOAD_INTERVAL_KEY, long.class);
    boolean start_timer = false;
    for (final Provider provider : providers) {
      if (provider.factory().isReloadable()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scheduling reload of provider [" + provider + "] in " 
              + reload_interval + " seconds.");
        }
        timer.newTimeout(provider, reload_interval, TimeUnit.SECONDS);
        start_timer = true;
      }
    }
    
    if (start_timer) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Starting the reload timer.");
      }
      timer.start();
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not starting the reload timer as no providers "
            + "were given that allow for reloads.");
      }
    }
  }
  
  @VisibleForTesting
  List<Provider> sources() {
    return providers;
  }

  @VisibleForTesting
  HashedWheelTimer timer() {
    return timer;
  }

  @VisibleForTesting
  Set<String> reloadKeys() {
    return this.reloadKeys();
  }
  
  @VisibleForTesting
  List<ProviderFactory> factories() {
    return factories;
  }

  @VisibleForTesting
  List<Provider> providers() {
    return providers;
  }
  
  @VisibleForTesting
  ConfigurationEntry getEntry(final String key) {
    return merged_config.get(key);
  }
}
