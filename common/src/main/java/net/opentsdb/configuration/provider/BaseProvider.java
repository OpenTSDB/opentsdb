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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationOverride;
import net.opentsdb.utils.DateTime;

/**
 * Base class for the provider implementation.
 * 
 * @since 3.0
 */
public abstract class BaseProvider implements Provider {
  private static final Logger LOG = LoggerFactory.getLogger(Provider.class);
  
  /** The factory that instantiated this provider. */
  protected final ProviderFactory factory;
  
  /** The configuration object this provider is associated with. */
  protected final Configuration config;
  
  /** A timer from the config used to reload the config. */
  protected final HashedWheelTimer timer;
  
  /** The SHARED set of reload keys to update on refreshes. */
  protected final Set<String> reload_keys;
  
  /** The last reload time in ms. May be 0 for non-reloadable providers. */
  protected long last_reload;
  
  /**
   * Default ctor.
   * @param factory A non-null provider factory.
   * @param config A non-null config object we belong to.
   * @param timer A non-null timer object.
   * @param reload_keys A non-null (possibly empty) set of keys to reload.
   * @throws IllegalArgumentException if a required parameter is missing.
   */
  public BaseProvider(final ProviderFactory factory, 
                  final Configuration config, 
                  final HashedWheelTimer timer,
                  final Set<String> reload_keys) {
    if (factory == null) {
      throw new IllegalArgumentException("Factory cannot be null.");
    }
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    if (timer == null) {
      throw new IllegalArgumentException("Timer cannot be null.");
    }
    if (reload_keys == null) {
      throw new IllegalArgumentException("Reload keys cannot be null.");
    }
    this.factory = factory;
    this.config = config;
    this.timer = timer;
    this.reload_keys = reload_keys;
  }
  
  /**
   * Called by the {@link Configuration} class to load the current value for the
   * given key when a schema is registered via 
   * {@link Configuration#register(net.opentsdb.configuration.ConfigurationEntrySchema)}.
   * @param key A non-null and non-empty key.
   * @return A configuration override if the provider had data for the 
   * key (even if it was null) or null if the provider did not have any
   * data.
   */
  public abstract ConfigurationOverride getSetting(final String key);
  
  /**
   * The name of this provider.
   * @return A non-null string.
   */
  public abstract String source();

  /**
   * Called by the {@link Configuration} timer to refresh it's values.
   * Only called if {@link ProviderFactory#isReloadable()} returned true.
   */
  public abstract void reload();
  
  /**
   * A millisecond Unix epoch timestamp when the config was last reloaded.
   * Starts at 0 and is 0 always for non-reloadable providers.
   * @return An integer from 0 to max int.
   */
  public long lastReload() {
    return last_reload;
  }
  
  /**
   * The factory this provider was instantiated from.
   * @return A non-null factory.
   */
  public ProviderFactory factory() {
    return factory;
  }
  
  /**
   * The code that runs in the timer to execute {@link #reload()}.
   */
  @Override
  public void run(final Timeout ignored) throws Exception {
    last_reload = DateTime.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting config reload for provider: " + this);
    }
    try {
      reload();
    } catch (Exception e) {
      LOG.error("Failed to run the reload task.", e);
    }
    
    final long interval = config.getTyped(
        Configuration.CONFIG_RELOAD_INTERVAL_KEY, long.class);
    final long next_run = interval - 
        ((DateTime.currentTimeMillis() - last_reload) / 1000);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reload completed in " 
          + (((double) DateTime.currentTimeMillis() - 
              (double)last_reload) / (double) 1000) 
          + "ms. Scheduling reload for provider [" + this + "] in " 
          + next_run + " seconds");
    }
    timer.newTimeout(this, next_run, TimeUnit.SECONDS);
  }
  
}
