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
package net.opentsdb.core;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.configuration.ConfigurationOverride;
import net.opentsdb.configuration.ConfigurationValueValidator;

/**
 * Simple config validation class that allows the plugin config to be 
 * either a path to a file or the actual YAML/JSON config.
 * 
 * @since 3.0
 */
public class PluginConfigValidator implements ConfigurationValueValidator {

  /** Used for validation to make sure the jvm doesn't skip the operation. */
  private Object black_hole;
  
  @Override
  public ValidationResult validate(final ConfigurationEntrySchema schema) {
    return validate(schema, null);
  }

  @Override
  public ValidationResult validate(final ConfigurationOverride override) {
    if (override.getValue() == null) {
      return ConfigurationValueValidator.NULL;
    }
    
    if (override.getValue() instanceof String) {
      return ConfigurationValueValidator.OK;
    }
    
    if (override.getValue() instanceof JsonNode) {
      try {
        black_hole = Configuration.OBJECT_MAPPER.convertValue(
            override.getValue(), 
            PluginsConfig.class);
        return ConfigurationValueValidator.OK;        
      } catch (Exception e) {
        return ValidationResult.newBuilder()
            .notValid()
            .setMessage("Parsing failed: " + e.getMessage())
            .build();
      } finally {
        black_hole = null;
      }
    }
    
    return ValidationResult.newBuilder()
        .notValid()
        .setMessage("Unrecognized value type: " 
            + override.getValue().getClass().toString())
        .build();
  }

  @Override
  public ValidationResult validate(final ConfigurationEntrySchema schema,
                                   final Object value) {
    if (schema.getDefaultValue() == null) {
      return ConfigurationValueValidator.OK;
    }
    
    if (schema.getDefaultValue() instanceof String) {
      return ConfigurationValueValidator.OK;
    }
    return ConfigurationValueValidator.OK;
  }

}
