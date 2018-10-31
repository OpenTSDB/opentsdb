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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationOverride;

/**
 * Dummy class to test secret providers. This should NOT be loaded in
 * production environments as it simply reads the plain text config.
 * 
 * @since 3.0
 */
public class PlainTextSecretProvider extends BaseSecretProvider  {
  private static final Logger LOG = LoggerFactory.getLogger(
      PlainTextSecretProvider.class);
  
  public PlainTextSecretProvider() {
    super();
  }

  @Override
  public void initialize(final ProviderFactory factory, 
      final Configuration config, 
      final HashedWheelTimer timer,
      final String id) {
    if (factory == null) {
      throw new IllegalArgumentException("Factory cannot be null.");
    }
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    if (timer == null) {
      throw new IllegalArgumentException("Timer cannot be null.");
    }
    this.factory = factory;
    this.config = config;
    this.timer = timer;
    this.id = id;
    
    LOG.warn("-----------------------------------------------------------");
    LOG.warn("- WARNING: Instantiated Plain Text Secret plugin!         -");
    LOG.warn("- This is not a secure way to handle secrets. Please use  -");
    LOG.warn("- a plugin with access to a secure storage location.      -");
    LOG.warn("-----------------------------------------------------------");
  }
  
  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public String getSecretString(final String key) {
    return config.getString(key);
  }

  @Override
  public byte[] getSecretBytes(final String key) {
    return config.getString(key).getBytes(Const.UTF8_CHARSET);
  }

  @Override
  public ConfigurationOverride getSetting(final String key) {
    // Always returns null from this source.
    return null;
  }
  
  @Override
  public void reload() {
    // no-op
  }

}
