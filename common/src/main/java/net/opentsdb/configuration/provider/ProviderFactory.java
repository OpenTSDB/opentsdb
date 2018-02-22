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

import java.io.Closeable;
import java.util.Set;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.Configuration;

/**
 * The interface to implement for provider factories, used to instantiate
 * a provider for the configuration framework.
 * 
 * @since 3.0
 */
public interface ProviderFactory extends Closeable {
  
  /**
   * Return a new instance of the underlying provider. This is only
   * called by the Configuration class in it's constructor.
   * 
   * @param config A non-null configuration class that the provider
   * belongs to.
   * @param timer A non-null timer the config uses for triggering 
   * {@link Provider#reload()}s.
   * @param reload_keys A non-null set of keys to keep an eye on during
   * reloads.
   * @return A non-null provider instance.
   */
  public Provider newInstance(final Configuration config,
                              final HashedWheelTimer timer,
                              final Set<String> reload_keys);
  
  /** @return Whether or not the provider is reloadable. */
  public boolean isReloadable();
  
  /** @return A description of the provider, what it reads from, 
   * what it parses, etc. */
  public String description();
  
  /** @return The simpe name used in 'config.providers' to load this
   * factory. */
  public String simpleName();
}
