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
import java.util.Set;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationOverride;

/**
 * Loads values from the system properties passed as JVM options.
 * 
 * @since 3.0
 */
public class SystemPropertiesProvider extends BaseProvider {
  public static final String SOURCE = SystemPropertiesProvider.class.getSimpleName();
  
  /**
   * Default ctor.
   * @param factory A non-null provider factory.
   * @param config A non-null config object we belong to.
   * @param timer A non-null timer object.
   * @param reload_keys A non-null (possibly empty) set of keys to reload.
   * @throws IllegalArgumentException if a required parameter is missing.
   */
  public SystemPropertiesProvider(final ProviderFactory factory, 
                          final Configuration config, 
                          final HashedWheelTimer timer,
                          final Set<String> reload_keys) {
    super(factory, config, timer, reload_keys);
  }
  
  @Override
  public ConfigurationOverride getSetting(final String key) {
    final String value = System.getProperty(key);
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
    // no-op
  }
  
  public static class SystemProperties implements ProviderFactory {
    
    @Override
    public Provider newInstance(final Configuration config, 
                                final HashedWheelTimer timer,
                                final Set<String> reload_keys) {
      return new SystemPropertiesProvider(this, config, timer, reload_keys);
    }
    
    @Override
    public String description() {
      return "Pulls settings from the JVM flags in the format "
          + "'-Dkey=value -Dkey2=value2'.";
    }
  
    @Override
    public boolean isReloadable() {
      return false;
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
