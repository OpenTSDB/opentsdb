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

import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationOverride;

import java.io.Closeable;

import io.netty.util.TimerTask;

/**
 * The base class for a {@link Configuration} provider. It maintains a reference
 * to the config object, a timer and a set of dynamic keys to reload if
 * the provider supports reloading.
 * <p>
 * <b>NOTE:</b> Do NOT modify the reload keys set. This is shared between
 * all providers.
 * <b>NOTE:</b> Do NOT stop the timer as it's also shared.
 * 
 * @since 3.0
 */
public interface Provider extends Closeable, TimerTask {
  
  /**
   * Called by the {@link Configuration} class to load the current value for the
   * given key when a schema is registered via 
   * {@link Configuration#register(net.opentsdb.configuration.ConfigurationEntrySchema)}.
   * @param key A non-null and non-empty key.
   * @return A configuration override if the provider had data for the 
   * key (even if it was null) or null if the provider did not have any
   * data.
   */
  public ConfigurationOverride getSetting(final String key);
  
  /**
   * The name of this provider.
   * @return A non-null string.
   */
  public String source();

  /**
   * Called by the {@link Configuration} timer to refresh it's values.
   * Only called if {@link ProviderFactory#isReloadable()} returned true.
   */
  public void reload();
  
  /**
   * A millisecond Unix epoch timestamp when the config was last reloaded.
   * Starts at 0 and is 0 always for non-reloadable providers.
   * @return An integer from 0 to max int.
   */
  public long lastReload();
  
  /**
   * The factory this provider was instantiated from.
   * @return A non-null factory.
   */
  public ProviderFactory factory();
  
}
