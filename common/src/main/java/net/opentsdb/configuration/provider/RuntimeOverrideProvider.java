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
 * A simple instance that doesn't do anything other than allow the 
 * {@link Configuration} class to accept runtime overrides from the 
 * application.
 * 
 * @since 3.0
 */
public class RuntimeOverrideProvider extends Provider {
  public static final String SOURCE = RuntimeOverrideProvider.class.getSimpleName();
  
  /**
   * Factory constructor
   * @param factory A non-null provider factory.
   * @param config A non-null config object we belong to.
   * @param timer A non-null timer object.
   * @param reload_keys A non-null (possibly empty) set of keys to reload.
   * @throws IllegalArgumentException if a required parameter is missing.
   */
  public RuntimeOverrideProvider(final ProviderFactory factory, 
                         final Configuration config, 
                         final HashedWheelTimer timer,
                         final Set<String> reload_keys) {
    super(factory, config, timer, reload_keys);
  }
  
  @Override
  public ConfigurationOverride getSetting(final String key) {
    // no-op
    return null;
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
  
  public static class RuntimeOverride implements ProviderFactory {
  
    @Override
    public String description() {
      return "A provider that allows for the application to call "
          + "Configuration#addOverride(final String key, final "
          + "ConfigurationOverride override) during the application's "
          + "execution. The override may be removed by calling "
          + "Configuration#removeRuntimeOverride(final String key).";
    }
  
    @Override
    public Provider newInstance(final Configuration config, 
                                final HashedWheelTimer timer,
                                final Set<String> reload_keys) {
      return new RuntimeOverrideProvider(this, config, timer, reload_keys);
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
      // TODO no-op
    }
    
  }
}
