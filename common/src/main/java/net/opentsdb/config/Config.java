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

/**
 * TODO - doc
 * A new config class for OpenTSDB that supports -
 * - merging multiple sources
 * - registering configs for easy debugging
 * - updatable values (callbacks)
 * - complex types
 * 
 * TODO - questions
 * - do we kick values that don't have a schema out?
 * @since 3.0
 */
public interface Config {

  /**
   * Whether or not the key should be obfuscated when debugging.
   * 
   * @param key A non-null and non-empty config entry key.
   * @return True if the value should be obfuscated, false if not.
   */
  public boolean obfuscate(final String key);
  
  /**
   * Registers the config schema with the configuration. This should be
   * called by the TSD and plugins to associate actual values with a
   * type and validator.
   * 
   * @param item A non-null schema fully configured.
   * @throws IllegalArgumentException if a schema for the same key has
   * already been registered.
   */
  public void register(final ConfigEntrySchema item);
 
  /**
   * Returns the parameter as a string. Simply calls {@code toString} on
   * non-string objects.
   * 
   * @param key The non-null and non-empty config key entry.
   * @return Null if the entry was not found, a String if found.
   */
  public String getString(final String key);
  
  /**
   * Returns the given config value cast to the given type (if possible).
   * 
   * @param key The non-null and non-empty config key entry.
   * @param type A non-null type to cast to.
   * @return Null if the entry was not found, a typed object if found.
   * @throws ???
   */
  public <T> T getTyped(final String key, final Type type);
  
  /**
   * Attaches the given callback to the key so that the caller can 
   * receive updates any time the flattened value has changed to a new
   * value. Note that callback processing order is indeterminate.
   * 
   * @param key A non-null and non-empty config key entry.
   * @param callback A non-null callback.
   */
  public void bind(final String key, final ConfigCallback<?> callback);
  
}
