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

import java.lang.reflect.Type;

import com.google.common.base.Strings;

/**
 * A configuration entry schema describing the value, it's type and 
 * how to validate if it's a proper value. 
 * 
 * @since 3.0
 */
public class ConfigEntrySchema {

  /** Whether or not to obfuscate the value. */
  protected boolean obfuscate;
  
  /** The configuration key */
  protected final String key;
  
  /** The type of data used to parse the config. */
  protected final Type type;
  
  /** The source of this registration, i.e. the class name. */
  protected final String source;
  
  /** A description of the configuration item. */
  protected final String description;
  
  /** A validator for the config entry. */
  protected final ConfigValueValidator validator;
  
  /** The default value for this config entry. */
  protected final Object default_value;
  
  /** Whether or not the value can be dynamically refreshed or overridden. */
  protected final boolean dynamic;
  
  /** Whether or not nulls are allowed. */
  protected final boolean nullable;
  
  /** The command line help level at which this item will be displayed 
   * ('default' or 'extended') */
  protected final String help_level;
  
  /** The meta symbol representing the type of value expected for a 
   * parameterized command line arg. */
  protected final String meta;
  
  /**
   * Protected ctor for the builder.
   * @param builder A non-null builder.
   */
  protected ConfigEntrySchema(final Builder builder) {
    key = builder.key;
    type = builder.type;
    source = builder.source;
    description = builder.description;
    validator = builder.validator;
    default_value = builder.default_value;
    dynamic = builder.dynamic;
    nullable = builder.nullable;
    help_level = builder.help_level;
    meta = builder.meta;
  }
  
  /** @return The non-null and non-empty key for this config entry. */
  public String getKey() {
    return key;
  }

  /** @return The non-null data type of this config entry. */
  public Type getType() {
    return type;
  }
  
  /** @return The non-null and non-empty source of this meta. */
  public String getSource() {
    return source;
  }
  
  /** @return The description of this config entry. May be null. */
  public String getDescription() {
    return description;
  }
  
  /** @return The optional validator of this config entry. May be null. */
  public ConfigValueValidator getValidator() {
    return validator;
  }
  
  /** @return The default value of this config entry. May be null. */
  public Object getDefaultValue() {
    return default_value;
  }
  
  /** @return Whether or not this entry can be dynamically re-read. */
  public boolean isDynamic() {
    return dynamic;
  }
  
  /** @return Whether or not values may be null. */
  public boolean isNullable() {
    return nullable;
  }
  
  /** @return ? */
  public String getHelpLevel() {
    return help_level;
  }
  
  /** @return ? */
  public String getMeta() {
    return meta;
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("key=")
        .append(key)
        .append(", type=")
        .append(type)
        .append(", source=")
        .append(source)
        .append(", description=")
        .append(description)
        .append(", validator=")
        .append(validator)
        .append(", defaultValue=")
        .append(default_value == null ? "null" :
          (obfuscate ? "********" : default_value))
        .append(", isDynamic=")
        .append(dynamic)
        .append(", isNullable=")
        .append(nullable)
        .append(", helpLevel=")
        .append(help_level)
        .append(", meta=")
        .append(meta)
        .toString();
  }
  
  /** @return A new builder to construct the schema with. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    protected String key;
    protected Type type;
    protected String source;
    protected String description;
    protected ConfigValueValidator validator;
    protected Object default_value;
    protected boolean dynamic;
    protected boolean nullable;
    protected String help_level;
    protected String meta;
    
    /**
     * Sets the config entry key.
     * @param key A non-null and non-empty string.
     * @return The builder.
     * @throws IllegalArgumentException if the key was null or empty.
     */
    public Builder setKey(final String key) {
      if (Strings.isNullOrEmpty(key)) {
        throw new IllegalArgumentException("The key cannot be null or empty.");
      }
      this.key = key;
      return this;
    }
    
    /**
     * Sets the type of the data for this config.
     * @param type A non-null type.
     * @return The builder.
     * @throws IllegalArgumentException if the type was null.
     */
    public Builder setType(final Type type) {
      if (type == null) {
        throw new IllegalArgumentException("Type cannot be null.");
      }
      this.type = type;
      return this;
    }
    
    /**
     * Sets the type of the data for this config.
     * @param type A non-null type reference.
     * @return The builder.
     * @throws IllegalArgumentException if the type was null.
     */
    public Builder setType(final ConfigTypeRef<?> type) {
      if (type == null) {
        throw new IllegalArgumentException("Type reference cannot be null.");
      }
      this.type = type.type();
      return this;
    }
    
    /**
     * Sets the source of the default value, typically the class where 
     * config was registered.
     * @param source A non-null and non-empty source name.
     * @return The builder.
     * @throws IllegalArgumentException if the source was null or empty.
     */
    public Builder setSource(final String source) {
      if (Strings.isNullOrEmpty(source)) {
        throw new IllegalArgumentException("The source cannot be null or empty.");
      }
      this.source = source;
      return this;
    }
    
    /**
     * Sets the description for this config.
     * @param description A non-null and non-empty description string.
     * @return The builder.
     * @throws IllegalArgumentException if the description was null or empty.
     */
    public Builder setDescription(final String description) {
      if (Strings.isNullOrEmpty(description)) {
        throw new IllegalArgumentException("The description cannot be null or empty.");
      }
      this.description = description;
      return this;
    }
    
    /**
     * Sets an optional validator for the config. If not set, we just rely
     * on casting.
     * @param validator An optional validator. May be null.
     * @return The builder.
     */
    public Builder setValidator(final ConfigValueValidator validator) {
      this.validator = validator;
      return this;
    }
    
    /**
     * Sets the default value for this config. The default may be null.
     * <b>WARNING</b> When passing in a complex object (such as a 
     * collection) make sure to give this an immutable copy. Otherwise
     * you'll see some strange behavior if the underlying object is 
     * mutated.
     * @param default_value A default value, may be null.
     * @return The builder.
     */
    public Builder setDefaultValue(final Object default_value) {
      this.default_value = default_value;
      return this;
    }
    
    /**
     * Sets the dynamic flag indicating this config can be dynamically
     * reloaded during runtime.
     * @return The builder.
     */
    public Builder isDynamic() {
      dynamic = true;
      return this;
    }
    
    /**
     * Unsets the dynamic flag indicating this config cannot be dynamically
     * reloaded during runtime.
     * @return The builder.
     */
    public Builder notDynamic() {
      dynamic = false;
      return this;
    }
    
    /**
     * Sets the nullable flag indicating this config can be null.
     * @return The builder.
     */
    public Builder isNullable() {
      nullable = true;
      return this;
    }
    
    /**
     * Unsets the nullable flag indicating this config cannot be null.
     * @return The builder.
     */
    public Builder notNullable() {
      nullable = false;
      return this;
    }
    public Builder setHelpLevel(final String help_level) {
      this.help_level = help_level;
      return this;
    }
    
    public Builder setMeta(final String meta) {
      this.meta = meta;
      return this;
    }
    
    /** @return The constructed {@link ConfigEntrySchema} */
    public ConfigEntrySchema build() {
      return new ConfigEntrySchema(this);
    }
  }
}
