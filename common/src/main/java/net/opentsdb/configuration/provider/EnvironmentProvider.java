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
package net.opentsdb.configuration.provider;

import java.io.IOException;

import io.netty.util.HashedWheelTimer;
import io.netty.util.TimerTask;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationOverride;

/**
 * Pulls values from the environment. As the user can change flags at
 * runtime, we'll reload periodically.
 * 
 * @since 3.0
 */
public class EnvironmentProvider extends BaseProvider implements TimerTask {
  public static final String SOURCE = EnvironmentProvider.class.getSimpleName();

  /**
   * Default ctor.
   * @param factory A non-null provider factory.
   * @param config A non-null config object we belong to.
   * @param timer A non-null timer object.
   * @throws IllegalArgumentException if a required parameter is missing.
   */
  public EnvironmentProvider(final ProviderFactory factory, 
                     final Configuration config, 
                     final HashedWheelTimer timer) {
    super(factory, config, timer);
  }
  
  @Override
  public ConfigurationOverride getSetting(final String key) {
    final String value = System.getenv(key);
    if (value == null) {
      return null;
    }
    return ConfigurationOverride.newBuilder()
        .setSource(SOURCE)
        .setValue(value)
        .build();
  }

  @Override
  public String source() {
    return SOURCE;
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
  
  @Override
  public void reload() {
    for (final String key : config.reloadableKeys()) {
      final String value = System.getenv(key);
      if (value == null) {
        continue;
      }
      config.addOverride(key, ConfigurationOverride.newBuilder()
          .setSource(SOURCE)
          .setValue(value)
          .build());
    }
  }

  public static class Environment implements ProviderFactory {
  
    @Override
    public Provider newInstance(final Configuration config, 
                                final HashedWheelTimer timer) {
      return new EnvironmentProvider(this, config, timer);
    }
    
    @Override
    public String description() {
      return "Pulls settings from the environment under which the JVM was "
          + "launched.";
    }
  
    @Override
    public boolean isReloadable() {
      return true;
    }

    @Override
    public String simpleName() {
      return getClass().getSimpleName();
    }
    
    @Override
    public void close() throws IOException {
      // no-op
    }
    
  }
}
