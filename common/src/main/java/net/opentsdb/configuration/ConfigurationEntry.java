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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import net.opentsdb.configuration.ConfigurationValueValidator.ValidationResult;
import net.opentsdb.configuration.provider.Provider;
import net.opentsdb.configuration.provider.RuntimeOverrideProvider;
import net.opentsdb.utils.DateTime;

/**
 * A package private class meant to be used as a container in the main
 * hash map to contain the schema, settings and callbacks for a config
 * entry.
 * <p>
 * <b>NOTE:</b> The schema MUST be set before an override can be given
 * in order to perform validation.
 * 
 * @since 3.0
 */
@SuppressWarnings("rawtypes")
class ConfigurationEntry {
  private static final Logger LOG = LoggerFactory.getLogger(
      ConfigurationEntry.class);
  
  /** Reference to the config object. */
  private final Configuration config;
  
  /** The schema when set. May actually be null for a while depending 
   * on load order. */
  private ConfigurationEntrySchema schema;
  
  /** The settings loaded in order with overrides at the top. Would use
   * a stack, but we need to be able to insert into arbitrary locations. */
  private List<ConfigurationOverride> settings;
  
  /** A set of callbacks, lazily initialized. */
  private Set<ConfigurationCallback> callbacks;
  
  /**
   * Package private ctor for the {@link Configuration} class to call. 
   * @param config A non-null config.
   */
  protected ConfigurationEntry(final Configuration config) {
    this.config = config;
  }
  
  /**
   * Determines the value to respond with based on the schema and 
   * settings.
   * @return A value, may be null.
   */
  public Object getValue() {
    if (settings != null && !settings.isEmpty()) {
      return settings.get(0).getValue();
    }
    if (schema != null) {
      return schema.getDefaultValue();
    }
    return null;
  }
  
  /**
   * Sets the schema in this entry.
   * @param schema A non-null schema.
   * @throws IllegalArgumentException if the schema was null
   * @throws ConfigurationException if a race condition was lost and 
   * another thread tried to write the schema.
   */
  public ValidationResult setSchema(final ConfigurationEntrySchema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("Schema cannot be null.");
    }
    
    // double locking.
    if (this.schema == null) {
      synchronized(this) {
        if (this.schema == null) {
          this.schema = schema;
        } else {
          throw new ConfigurationException("[" + schema.getKey() 
              + "] Failed to set the schema "
              + "as another source [" + this.schema.getSource() 
              + "] already wrote a value before [" 
              + schema.getSource() + "]");
        }
      }
    } else {
      throw new ConfigurationException("[" + schema.getKey() 
          + "] Failed to set the schema "
          + "as another source [" + this.schema.getSource() 
          + "] already wrote a value before [" 
          + schema.getSource() + "]");
    }
    return ConfigurationValueValidator.OK;
  }
  
  /**
   * Adds the given setting to the deque on top. If it's a new setting
   * or the value has changed then the {@link #callbacks} are executed if
   * any are present at the time of the update. If the setting is already
   * present and the value is the same, no updates occur. If a schema is
   * already present, and the setting is present and dynamic updating is 
   * disabled, then nothing is updated regardless of value.
   * 
   * The {@link ConfigurationOverride#getSource()} is used as the comparator.
   *  
   * @param override A non-null setting.
   * @throws IllegalArgumentException if the setting was null or the
   * source was null.
   * @throws ConfigurationException if the schema wasn't already set.
   */
  @SuppressWarnings("unchecked")
  public ValidationResult addOverride(final ConfigurationOverride override) {
    if (override == null) {
      throw new IllegalArgumentException("The setting cannot be null.");
    }
    if (Strings.isNullOrEmpty(override.getSource())) {
      throw new IllegalArgumentException("The setting's source cannot "
          + "be null or empty.");
    }
    if (schema == null) {
      throw new ConfigurationException("Cannot store a setting before "
          + "storing a schema.");
    }
    
    // TODO - may need to worry about secrets here...
    final ValidationResult result = schema.validate(override.getValue());
    if (!result.isValid()) {
      return result;
    }
    
    // make sure to hide important bits during debugging.
    if (schema.secret) {
      override.secret = true;
    }
    
    // two special keys only dealt with on initialization
    if (schema.getKey().equals(Configuration.CONFIG_PROVIDERS_KEY) || 
        schema.getKey().equals(Configuration.PLUGIN_DIRECTORY_KEY)) {
      if (settings == null) {
        settings = new CopyOnWriteArrayList<ConfigurationOverride>();
      }
      settings.add(0, override);
      return ConfigurationValueValidator.OK;
    }
    
    final int source_index = sourceIndex(override.source);
    if (source_index < 0) {
      throw new IllegalArgumentException("Settings from source [" 
          + override.getSource() + "] were not allowed in this config.");
    }
    
    // double locking
    if (settings == null) {
      synchronized(this) {
        if (settings == null) {
          settings = new CopyOnWriteArrayList<ConfigurationOverride>();
        }
      }
    }
    
    // maintain insert order by syncing here. Reads will continue safely.
    synchronized(this) {
      boolean matched = false;
      // TODO - check actual source or FORCE every source to store an empty value?
      for (final ConfigurationOverride extant : settings) {
        if (extant.getSource().equals(override.getSource())) {
          if (extant.getValue().equals(override.getValue())) {
            // same value, no need to update
            return ConfigurationValueValidator.OK;
          }
          
          if (!schema.isDynamic()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("[" + schema.getKey() + "] Skipping update of "
                  + "override as it is not set to be dynamic.");
            }
            return ValidationResult.newBuilder()
                .notValid()
                .setMessage("Schema was marked as not dynamic. "
                    + "Updates not allowed.")
                .build();
          }
          
          // existing so update it!
          extant.value = override.getValue();
          extant.last_change = DateTime.currentTimeMillis();
          if (LOG.isDebugEnabled()) {
            LOG.debug("[" + schema.getKey() + "] Updated dynamic setting: " 
                + extant);
          }
          matched = true;
          break;
        }
      }
      
      if (!matched) {
        if (override.getSource().equals(RuntimeOverrideProvider.SOURCE)) {
          if (!schema.isDynamic()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("[" + schema.getKey() + "] Skipping update of "
                  + "override as it is not set to be dynamic.");
            }
            return ValidationResult.newBuilder()
                .notValid()
                .setMessage("Schema was marked as not dynamic. "
                    + "Updates not allowed.")
                .build();
          }
          
          // Runtimes always go at the front.
          settings.add(0, override);
          if (LOG.isDebugEnabled()) {
            LOG.debug("[" + schema.getKey() + "] Added runtime "
                + "override: " + override);
          }
        } else {
          // we need to look at the insert order.
          boolean found = false;
          for (int x = 0; x < settings.size(); x++) {
            for (int y = source_index - 1; y >= 0; y--) {
              if (config.providers().get(y).source()
                    .equals(settings().get(x).getSource())) {
                // if a match was made we found the next entry that 
                // comes BEFORE the current setting, so insert this setting
                // at the index x.
                found = true;
                break;
              }
            }
            if (found) {
              settings.add(x, override);
              if (LOG.isDebugEnabled()) {
                LOG.debug("[" + schema.getKey() + "] Added runtime "
                    + "override: " + override);
              }
              break;
            }
          }
          
          if (!found) {
            settings.add(override);
            if (LOG.isDebugEnabled()) {
              LOG.debug("[" + schema.getKey() + "] Added runtime "
                  + "override: " + override);
            }
          }
        }
      }
    }
    
    // if we made it here then we need to update our callers
    executeCallbacks();
    return ConfigurationValueValidator.OK;
  }

  /**
   * Attempts to remove the runtime overrid setting as named by 
   * {@link Provider#SOURCE}.
   * @return True if the entry was found and removed, false if an entry
   * was not found.
   */
  public boolean removeRuntimeOverride() {
    synchronized(this) {
      if (settings == null || settings.isEmpty()) {
        return false;
      }
      int idx = -1;
      for (int i = 0; i < settings.size(); i++) {
        if (settings.get(i).getSource().equals(RuntimeOverrideProvider.SOURCE)) {
          idx = i;
          break;
        }
      }
      if (idx >= 0) {
        settings.remove(idx);
        if (LOG.isDebugEnabled()) {
          LOG.debug("[" + schema.getKey() + "] Removed runtime override.");
        }
        
        // execute callbacks to show the update
        executeCallbacks();
        return true;
      }
    }
    return false;
  }
  
  /**
   * Adds a callback to the callback set. Before the callback is added
   * it's executed with the current value of the setting.
   * @param callback A non-null callback.
   * @throws IllegalArgumentException if the callback was null.
   */
  @SuppressWarnings("unchecked")
  public void addCallback(final ConfigurationCallback callback) {
    if (callback == null) {
      throw new IllegalArgumentException("Callback cannot be null.");
    }
    
    if (callbacks == null) {
      synchronized(this) {
        if (callbacks == null) {
          callbacks = Sets.newConcurrentHashSet();
        }
      }
    }
    
    try {
      if (schema.getTypeReference() != null) {
        callback.update(schema.getKey(),
            Configuration.OBJECT_MAPPER.convertValue(getValue(), schema.getTypeReference()));
      } else {
        callback.update(schema.getKey(),
            Configuration.OBJECT_MAPPER.convertValue(getValue(), schema.getType()));
      }
    } catch (Throwable t) {
      LOG.error("Failed to execute config callback: " + callback, t);
    }
    
    callbacks.add(callback);
  }
  
  @VisibleForTesting
  protected ConfigurationEntrySchema schema() {
    return schema;
  }
  
  @VisibleForTesting
  protected List<ConfigurationOverride> settings() {
    return settings;
  }
  
  @VisibleForTesting
  public Set<ConfigurationCallback> callbacks() {
    return callbacks;
  }
  
  /**
   * Helper to find the index of the given source in the config source
   * array name list.
   * @param source A non-null and non-empty source.
   * @return -1 if not found in the config source list or a zero
   * based index if found.
   */
  private int sourceIndex(final String source) {
    if (Strings.isNullOrEmpty(source)) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (config.providers() == null) {
      throw new IllegalStateException("Shouldn't be calling this if the "
          + "sources list is null, meaning the config wasn't initialized"
          + "properly!");
    }
    for (int i = 0; i < config.providers().size(); i++) {
      if (config.providers().get(i).source().equals(source)) {
        return i;
      }
    }
    return -1;
  }

  private void executeCallbacks() {
    if (callbacks == null || callbacks.isEmpty()) {
      return;
    }
    
    for (final ConfigurationCallback callback : callbacks) {
      try {
        if (schema.getTypeReference() != null) {
          callback.update(schema.getKey(),
              Configuration.OBJECT_MAPPER.convertValue(getValue(), schema.getTypeReference()));
        } else {
          callback.update(schema.getKey(), 
              Configuration.OBJECT_MAPPER.convertValue(getValue(), schema.getType()));
        }
      } catch (Exception e) {
        LOG.error("Failed to update callback: " + callback 
            + " with key [" + schema.getKey() + "]", e);
      }
    }
  }
}
