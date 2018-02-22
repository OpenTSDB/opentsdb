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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;

import net.opentsdb.configuration.ConfigurationValueValidator.ValidationResult;

/**
 * A configuration entry schema describing the value, it's type and 
 * how to validate if it's a proper value. 
 * 
 * @since 3.0
 */
public class ConfigurationEntrySchema {

  /** The configuration key */
  protected final String key;
  
  /** The type of data used to parse the config. */
  protected final Class<?> type;
  
  /** An optional type referenc for the complex object. */
  protected final TypeReference<?> type_reference;
  
  /** The source of this registration, i.e. the class name. */
  protected final String source;
  
  /** A description of the configuration item. */
  protected final String description;
  
  /** A validator for the config entry. */
  protected final ConfigurationValueValidator validator;
  
  /** The default value for this config entry. */
  protected final Object default_value;
  
  /** Whether or not the value can be dynamically refreshed or overridden. */
  protected final boolean dynamic;
  
  /** Whether or not nulls are allowed. */
  protected final boolean nullable;
  
  /** Whether or not the setting is a secret. */
  protected boolean secret;
  
  /** The command line help level at which this item will be displayed 
   * ('default' or 'extended') */
  protected final String help_level;
  
  /** The meta symbol representing the type of value expected for a 
   * parameterized command line arg. */
  protected final String meta;
  
  protected Object black_hole;
  
  /**
   * Protected ctor for the builder.
   * @param builder A non-null builder.
   */
  protected ConfigurationEntrySchema(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    if (builder.type == null && builder.type_reference == null) {
      throw new IllegalArgumentException("Must supply either a type or "
          + "a type reference.");
    }
    if (builder.type != null && builder.type_reference != null) {
      throw new IllegalArgumentException("Must set either type or type "
          + "reference but not both.");
    }
    if (Strings.isNullOrEmpty(builder.source)) {
      throw new IllegalArgumentException("The source cannot be null or "
          + "empty.");
    }
    key = builder.key;
    type = builder.type;
    type_reference = builder.type_reference;
    source = builder.source;
    description = builder.description;
    validator = builder.validator;
    default_value = builder.default_value;
    dynamic = builder.dynamic;
    nullable = builder.nullable;
    secret = builder.secret;
    help_level = builder.help_level;
    meta = builder.meta;
    
    // validation checks
    final ValidationResult result = validate(default_value);
    if (!result.isValid()) {
      throw new IllegalArgumentException(result.getMessage());
    }
  }
  
  /** @return The non-null and non-empty key for this config entry. */
  public String getKey() {
    return key;
  }

  /** @return The type of the data for this config entry. If null
   * {@link #getTypeReference()} must return a non-null value. */
  public Class<?> getType() {
    return type;
  }
  
  /** @return The optional type reference of this config entry. May be 
   * null but then {@link #getType()} should return a non-null value.
   */
  public TypeReference<?> getTypeReference() {
    return type_reference;
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
  public ConfigurationValueValidator getValidator() {
    return validator;
  }
  
  /** @return The default value of this config entry. May be null. */
  public Object getDefaultValue() {
    return default_value;
  }
  
  /** @return Whether or not the setting is a secret. */
  public boolean isSecret() {
    return secret;
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
  
  /**
   * Determines if the given value is valid or not based on the schema
   * type and value.
   * @param value A value that may be null if nullables are allowed.
   * @return A non-null validation result.
   */
  public ValidationResult validate(final Object value) {
    if (!nullable && value == null) {
      return ConfigurationValueValidator.NULL;
    }
    
    if (type != null &&
        type.isPrimitive() &&
        value == null) {
      return ValidationResult.newBuilder()
          .notValid()
          .setMessage("Null values cannot be assigned to primitive types.")
          .build();
    }
    
    if (value == null && nullable) {
      return ConfigurationValueValidator.OK;
    }
    
    try {
      if (type_reference != null) {
        black_hole = value;
        black_hole = Configuration.OBJECT_MAPPER.convertValue(value, 
            type_reference);
      } else {
        black_hole = value;
        black_hole = Configuration.OBJECT_MAPPER.convertValue(value, type);
      }
    } catch (Exception e) {
      return ValidationResult.newBuilder()
          .notValid()
          .setMessage(e.getMessage())
          .build();
    }
    
    if (validator != null) {
      return validator.validate(this, value);
    }
    
    return ConfigurationValueValidator.OK;
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("key=")
        .append(key)
        .append(", type=")
        .append(type)
        .append(", typeReference=")
        .append(type_reference)
        .append(", source=")
        .append(source)
        .append(", description=")
        .append(description)
        .append(", validator=")
        .append(validator)
        .append(", defaultValue=")
        .append(default_value == null ? "null" :
          (secret ? "********" : default_value))
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
    protected Class<?> type;
    protected TypeReference<?> type_reference;
    protected String source;
    protected String description;
    protected ConfigurationValueValidator validator;
    protected Object default_value;
    protected boolean dynamic;
    protected boolean nullable;
    protected boolean secret;
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
    public Builder setType(final Class<?> type) {
      if (type == null) {
        throw new IllegalArgumentException("Type cannot be null.");
      }
      this.type = type;
      return this;
    }
    
    /**
     * Sets the type of the data for this config.
     * @param type_reference A non-null type reference.
     * @return The builder.
     * @throws IllegalArgumentException if the type was null.
     */
    public Builder setType(final TypeReference<?> type_reference) {
      if (type_reference == null) {
        throw new IllegalArgumentException("Type reference cannot be null.");
      }
      this.type_reference = type_reference;
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
    public Builder setValidator(final ConfigurationValueValidator validator) {
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
    
    /**
     * Sets the secret flag to hide the value in debug calls.
     * @return The builder.
     */
    public Builder isSecret() {
      secret = true;
      return this;
    }
    
    /**
     * Unsets the secret flag to print out the value in debug calls.
     * @return The builder.
     */
    public Builder notSecret() {
      secret = false;
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
    
    /** @return The constructed {@link ConfigurationEntrySchema} 
     * @throws IllegalArgumentException if a required parameter is 
     * missing or the default value does not satisfy the config. */
    public ConfigurationEntrySchema build() {
      return new ConfigurationEntrySchema(this);
    }
  }
}
