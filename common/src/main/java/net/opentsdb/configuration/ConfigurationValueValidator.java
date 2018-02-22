// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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

/**
 * The interface for a value validator that takes a configuration schema,
 * override or bare value and can implement various rules on it
 * 
 * @since 2.4
 */
public interface ConfigurationValueValidator {
  
  /** Singleton telling folks the result was valid. */
  public static final ValidationResult OK = ValidationResult.newBuilder()
      .isValid()
      .build();
  
  /** Singleton for null values set on a non-nullable schema. */
  public static final ValidationResult NULL = ValidationResult.newBuilder()
      .notValid()
      .setMessage("Null values are not allowed")
      .build();
  
  /**
   * Validates the passed configuration schema.
   * @param schema The schema to validate.
   * @return A non-null validation result.
   */
  public ValidationResult validate(final ConfigurationEntrySchema schema);
  
  /**
   * Validates a config override.
   * @param override The override to validate.
   * @return A non-null validation result.
   */
  public ValidationResult validate(final ConfigurationOverride override);
  
  /**
   * Validates a raw value against the rules in this implementation.
   * @param schema A non-null schema to pull settings from for the value.
   * @param value A value to parse.
   * @return A non-null validation result.
   */
  public ValidationResult validate(final ConfigurationEntrySchema schema, 
                                   final Object value);
  
  /**
   * A result returned from a validation of a config setting. We avoid
   * exceptions here to avoid stack tracing overhead since we don't need
   * it.
   */
  public static class ValidationResult {
    private final boolean valid;
    private final String message;
    
    /**
     * Protected ctor.
     * @param builder A non-null builder.
     */
    protected ValidationResult(final Builder builder) {
      valid = builder.valid;
      message = builder.message;
    }
    
    /** @return Whether or not the validation passed. */
    public boolean isValid() {
      return valid;
    }
    
    /** @return An optional message if validation failed. May be null. */
    public String getMessage() {
      return message;
    }
    
    /** @return A builder. */
    public static Builder newBuilder() {
      return new Builder();
    }
    
    public static class Builder {
      private boolean valid;
      private String message;
      
      private Builder() { }
      
      /**
       * Sets the validation result to true.
       * @return The builder.
       */
      public Builder isValid() {
        valid = true;
        return this;
      }
      
      /**
       * Sets the validation result to false.
       * @return The builder.
       */
      public Builder notValid() {
        valid = false;
        return this;
      }
      
      /**
       * Sets an error message on validation failure.
       * @param message A non-null and non-empty message.
       * @return The builder.
       * @throws IllegalArgumentException if the message was null or empty.
       */
      public Builder setMessage(final String message) {
        if (Strings.isNullOrEmpty(message)) {
          throw new IllegalArgumentException("Message cannot be null "
              + "or empty.");
        }
        this.message = message;
        return this;
      }
      
      /** @return The constructed validation result instance. */
      public ValidationResult build() {
        return new ValidationResult(this);
      }
    }
  }
}
