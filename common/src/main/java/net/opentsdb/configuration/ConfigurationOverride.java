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

import com.google.common.base.Strings;

import net.opentsdb.configuration.ConfigurationValueValidator.ValidationResult;

/**
 * An override for a particular setting. This must be associated with
 * a {@link ConfigurationEntrySchema} to determine the key and type.
 * 
 * @since 3.0
 */
public class ConfigurationOverride {

  /** Whether or not the config is a secret. */
  protected boolean secret;
  
  /** The source of the override. */
  protected final String source;
  
  /** The value of the override. */
  protected Object value;
  
  /** The time of the last load for this setting, in ms. */
  protected long last_change;
  
  /**
   * Protected builder.
   * @param builder A non-null builder to load from.
   */
  protected ConfigurationOverride(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.source)) {
      throw new IllegalArgumentException("Source cannot be null or emtpy.");
    }
    source = builder.source;
    value = builder.value;
  }
  
  /** @return The non-null and non-empty source of this setting override. */
  public String getSource() {
    return source;
  }
  
  /** @return The value of this setting entry. May be null. */
  public Object getValue() {
    return value;
  }
  
  /** @return The timestamp this setting was last loaded, in ms. */
  public long getLastChange() {
    return last_change;
  }
  
  /**
   * Validates that the value is acceptable as per the schema.
   * @param schema The non-null schema to use to compare against.
   */
  public void validate(final ConfigurationEntrySchema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("Schema cannot be null.");
    }
    
    if (!schema.isNullable() && value == null) {
      throw new IllegalArgumentException("The schema was marked as "
          + "not-nullable yet the default value was null.");
    }
    
    if (schema.getType() != null && 
        schema.getType().isPrimitive() && 
        value == null) {
      throw new IllegalArgumentException("The type of this schema was a "
          + "primitive value yet the default value was null.");
    }
    
    if (value != null) {
      if (schema.getTypeReference() != null) {
        Configuration.OBJECT_MAPPER.convertValue(value, 
            schema.getTypeReference());
      } else {
        Configuration.OBJECT_MAPPER.convertValue(value, schema.getType());
      }
    }
    
    if (schema.getValidator() != null) {
      final ValidationResult result = schema.getValidator().validate(this);
      if (!result.isValid()) {
        throw new IllegalArgumentException("Validation failed: " 
            + result.getMessage());
      }
    }
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("{ value=")
        .append(value == null ? "null" : (secret ? "********" : value))
        .append(", source=")
        .append(source)
        .append(", lastChange=")
        .append(last_change)
        .append(" }")
        .toString();
  }
  
  /** @return A new builder to construct the schema with. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    protected String source;
    protected Object value;
    
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
     * Sets the value for this config. The value may be null.
     * <b>WARNING</b> When passing in a complex object (such as a 
     * collection) make sure to give this an immutable copy. Otherwise
     * you'll see some strange behavior if the underlying object is 
     * mutated.
     * @param value A value, may be null.
     * @return The builder.
     */
    public Builder setValue(final Object value) {
      this.value = value;
      return this;
    }
    
    
    /** @return The constructed {@link ConfigurationOverride} */
    public ConfigurationOverride build() {
      return new ConfigurationOverride(this);
    }
  }
}
