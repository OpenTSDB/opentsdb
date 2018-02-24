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

import java.util.Deque;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import net.opentsdb.config.ConfigValueValidator.ValidationResult;
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
class ConfigEntry {
  private static final Logger LOG = LoggerFactory.getLogger(
      ConfigEntry.class);
  
  /** The schema when set. May actually be null for a while depending 
   * on load order. */
  private ConfigEntrySchema schema;
  
  /** The settings loaded in order with overrides at the top. */
  private Deque<ConfigSetting> settings;
  
  /** A set of callbacks, lazily initialized. */
  private Set<ConfigCallback> callbacks;
  
  /**
   * Determines the value to respond with based on the schema and 
   * settings.
   * @return A value, may be null.
   */
  public Object getValue() {
    if (settings != null) {
      return settings.peek().getValue();
    }
    if (schema != null) {
      return schema.getDefaultValue();
    }
    return null;
  }
  
  /**
   * Sets the schema in this entry.
   * @param schema A non-null schema.
   * @throws IllegalArgumentException if the schema was null or an existing
   * schema was found.
   */
  public ValidationResult setSchema(final ConfigEntrySchema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("Schema cannot be null.");
    }
    
    // yes, we're validating the schema.
    if (schema.getValidator() != null) {
      final ValidationResult result = schema.getValidator().validate(schema);
      if (!result.isValid()) {
        LOG.warn("Unable to register schema: " + schema + " due to: " 
            + result.getMessage());
        return result;
      }
    }
    if (!schema.isNullable() && schema.getDefaultValue() == null) {
      LOG.warn("Unable to register schema: " + schema + " due to: " 
          + ConfigValueValidator.NULL);
      return ConfigValueValidator.NULL;
    }
      
    // double locking.
    if (this.schema == null) {
      synchronized(this) {
        if (this.schema == null) {
          this.schema = schema;
        } else {
          throw new IllegalArgumentException("Failed to set the schema "
              + "as another source [" + this.schema.getSource() 
              + "] already wrote a value before [" 
              + schema.getSource() + "]");
        }
      }
    } else {
      throw new IllegalArgumentException("Failed to set the schema "
          + "as another source [" + this.schema.getSource() 
          + "] already wrote a value before [" 
          + schema.getSource() + "]");
    }
    return ConfigValueValidator.OK;
  }
  
  /**
   * Adds the given setting to the deque on top. If it's a new setting
   * or the value has changed then the {@link #callbacks} are executed if
   * any are present at the time of the update. If the setting is already
   * present and the value is the same, no updates occur. If a schema is
   * already present, and the setting is present and dynamic updating is 
   * disabled, then nothing is updated regardless of value.
   * 
   * The {@link ConfigSetting#getSource()} is used as the comparator.
   *  
   * @param setting A non-null setting.
   * @throws IllegalArgumentException if the setting was null or the
   * source was null or empty or the schema wasn't set.
   */
  @SuppressWarnings("unchecked")
  public ValidationResult setSetting(final ConfigSetting setting) {
    if (setting == null) {
      throw new IllegalArgumentException("The setting cannot be null.");
    }
    if (Strings.isNullOrEmpty(setting.getSource())) {
      throw new IllegalArgumentException("The setting's source cannot "
          + "be null or empty.");
    }
    if (schema == null) {
      throw new IllegalArgumentException("Cannot store a setting before "
          + "storing a schema.");
    }
    if (schema.getValidator() != null) {
      final ValidationResult result = schema.getValidator().validate(setting);
      if (!result.isValid()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping invalid setting: " + setting + " due to: " 
              + result.getMessage());
        }
        return result;
      }
    }
    if (!schema.isNullable() && setting.getValue() == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping invalid setting: " + setting + " due to: " 
            + ConfigValueValidator.NULL);
      }
      return ConfigValueValidator.NULL;
    }
    
    // double locking
    if (settings == null) {
      synchronized(this) {
        if (settings == null) {
          settings = new ConcurrentLinkedDeque<ConfigSetting>();
        }
      }
    }
    
    // maintain call order by syncing
    synchronized(this) {
      boolean matched = false;
      for (final ConfigSetting extant : settings) {
        if (extant.getSource().equals(setting.getSource())) {
          if (extant.getValue().equals(setting.getValue())) {
            // same value, no need to update
            return ConfigValueValidator.OK;
          }
          
          if (!schema.isDynamic()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Skipping update of setting [" + schema.getKey() 
                + "] as it is not set to be dynamic.");
            }
            return ValidationResult.newBuilder()
                .notValid()
                .setMessage("Schema was marked as not dynamic. "
                    + "Updates not allowed.")
                .build();
          }
          
          // existing so update it!
          extant.value = setting.getValue();
          extant.last_change = DateTime.currentTimeMillis();
          matched = true;
          break;
        }
      }
      
      if (!matched) {
        // not found so push it
        settings.push(setting);
      }
    }
    
    // if we made it here then we need to update our callers
    if (callbacks != null) {
      final Iterator<ConfigCallback> iterator = callbacks.iterator();
      while (iterator.hasNext()) {
        final ConfigCallback callback = iterator.next();
        try {
          callback.update(schema.getKey(), getValue());
        } catch (Throwable t) {
          LOG.error("Failed to execute config callback: " + callback, t);
        }
      }
    }
    return ConfigValueValidator.OK;
  }
  
  /**
   * Adds a callback to the callback set. Before the callback is added
   * it's executed with the current value of the setting.
   * @param callback A non-null callback.
   * @throws IllegalArgumentException if the callback was null.
   */
  @SuppressWarnings("unchecked")
  public void addCallback(final ConfigCallback callback) {
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
      callback.update(schema.getKey(), getValue());
    } catch (Throwable t) {
      LOG.error("Failed to execute config callback: " + callback, t);
    }
    
    callbacks.add(callback);
  }
  
  @VisibleForTesting
  protected ConfigEntrySchema schema() {
    return schema;
  }
  
  @VisibleForTesting
  protected Deque<ConfigSetting> settings() {
    return settings;
  }
  
  @VisibleForTesting
  public Set<ConfigCallback> callbacks() {
    return callbacks;
  }
}
