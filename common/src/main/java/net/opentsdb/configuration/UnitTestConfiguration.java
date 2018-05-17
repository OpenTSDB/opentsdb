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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.ConfigurationValueValidator.ValidationResult;
import net.opentsdb.configuration.provider.Provider;
import net.opentsdb.configuration.provider.ProviderFactory;
import net.opentsdb.configuration.provider.RuntimeOverrideProvider;

/**
 * A helper for use with Unit Testing Configuration consumers. The full
 * Configuration class is instantiated but only one or two providers are
 * given. So use this in a BeforeClass call.
 * <p>
 * To instantiate with default settings, use the {@link #getConfiguration(Map)}
 * by providing the reference to a map.
 * 
 * @since 3.0
 */
public class UnitTestConfiguration extends Configuration {

  public UnitTestConfiguration(final String[] providers) {
    super(providers);
    register("tsd.maintenance.frequency", 60000, true, "UT");
  }
  
  /**
   * Helper function that returns a Configuration instance with a dead
   * timer and only the {@link RuntimeOverrideProvider} given.
   * 
   * @return A non-null config.
   */
  public static Configuration getConfiguration() {
    final String[] providers = new String[] {
        "--" + Configuration.CONFIG_PROVIDERS_KEY + "=RuntimeOverride"
    };
    return new UnitTestConfiguration(providers);
  }
  
  /**
   * Helper function that returns a Configuration instance with a dead
   * timer and only the {@link RuntimeOverrideProvider} given. The map
   * given must be a mutable reference.
   * 
   * @param settings A map of key values to load.
   * @return A non-null config.
   */
  public static UnitTestConfiguration getConfiguration(
      final Map<String, String> settings) {
    final String[] providers = new String[] {
        "--" + Configuration.CONFIG_PROVIDERS_KEY 
        + "=UnitTest,RuntimeOverride"
    };
    final UnitTestConfiguration config = 
        new UnitTestConfiguration(providers);
    for (final Provider provider : config.providers()) {
      if (provider instanceof UnitTestProvider) {
        ((UnitTestProvider) provider).kvs = settings;
      }
    }
    return config;
  }

  /**
   * Allows a UnitTest to inject a value into the config as either the
   * default value in the schema or an override. It still requires that
   * a config key be registered.
   * 
   * @param key A non-null and non-empty key.
   * @param value A value to inject.
   * @throws IllegalArgumentException if the value failed validation.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void override(final String key, final Object value) {
    final ConfigurationEntry entry = merged_config.get(key);
    if (entry == null) {
      throw new IllegalArgumentException("Register this config first!");
    }
    final ValidationResult result = entry.schema().validate(value);
    if (!result.isValid()) {
      throw new IllegalArgumentException("Bad value: " + result.getMessage());
    }
    if (entry.settings() == null || entry.settings().isEmpty()) {
      entry.schema().default_value = value;
    } else {
      final ConfigurationOverride override = entry.settings().get(0);
      override.value = value;
    }
    if (entry.callbacks() != null) {
      for (final ConfigurationCallback cb : entry.callbacks()) {
        cb.update(key, value);
      }
    }
  }
  
  public static class UnitTestProvider extends Provider {
    private Map<String, String> kvs;
    
    public UnitTestProvider(final ProviderFactory factory, 
                            final Configuration config,
                            final HashedWheelTimer timer, 
                            final Set<String> reload_keys) {
      super(factory, config, timer, reload_keys);
    }

    @Override
    public void close() throws IOException {
      // no-op
    }

    @Override
    public ConfigurationOverride getSetting(final String key) {
      if (kvs != null && kvs.containsKey(key)) {
        return ConfigurationOverride.newBuilder()
            .setSource(source())
            .setValue(kvs.get(key))
            .build();
      }
      return null;
    }

    @Override
    public String source() {
      return getClass().getSimpleName();
    }

    @Override
    public void reload() {
      // no-op
    }
    
  }
  
  public static class UnitTest implements ProviderFactory {

    @Override
    public void close() throws IOException {
      // no-op
    }

    @Override
    public Provider newInstance(final Configuration config, 
                                final HashedWheelTimer timer,
                                final Set<String> reload_keys) {
      return new UnitTestProvider(this, config, timer, reload_keys);
    }

    @Override
    public boolean isReloadable() {
      return false;
    }

    @Override
    public String description() {
      return "Only used for Unit Tests";
    }

    @Override
    public String simpleName() {
      return getClass().getSimpleName();
    }
    
  }
}
