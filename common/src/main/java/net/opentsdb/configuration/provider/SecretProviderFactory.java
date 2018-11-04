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
import net.opentsdb.configuration.Configuration;
import net.opentsdb.utils.PluginLoader;

/**
 * The secret provider factory that handles loading various secret 
 * source plugins (e.g. from Google Cloud KMS or AWS Secrets Manager).
 * 
 * @since 3.0
 */
public class SecretProviderFactory implements ProtocolProviderFactory, ProviderFactory {
  public static final String PROTOCOL = "secrets://";
  
  @Override
  public Provider newInstance(final Configuration config, 
                              final HashedWheelTimer timer) {
    throw new UnsupportedOperationException("Requires a URI");
  }
  
  @Override
  public Provider newInstance(final Configuration config, 
                              final HashedWheelTimer timer,
                              final String uri) {
    final String class_and_id = uri.trim().substring(PROTOCOL.length()).trim();
    if (class_and_id.contains(":")) {
      final String[] parts = class_and_id.split(":");
      final SecretProvider provider = getProvider(parts[0]);
      provider.initialize(this, config, timer, parts[1]);
      return provider;
    }
    final SecretProvider provider = getProvider(class_and_id);
    provider.initialize(this, config, timer, null);
    return provider;
  }

  @Override
  public boolean isReloadable() {
    return true;
  }

  @Override
  public String description() {
    return "A factory that instantiates secrets source plugins, i.e. "
        + "plugins that call out to encrypted data stores for objects "
        + "such as passwords or certificates.";
  }

  @Override
  public String simpleName() {
    return "Secrets";
  }

  @Override
  public String protocol() {
    return PROTOCOL;
  }

  @Override
  public boolean handlesProtocol(final String uri) {
    if (!uri.toLowerCase().startsWith(PROTOCOL)) {
      return false;
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }
  
  /**
   * Helper that searches the class path then plugins for the given
   * class name. 
   * @param class_name A non-empty and fully qualified class name.
   * @return The plugin if found and instantiated, an exception if
   * not.
   */
  private SecretProvider getProvider(final String class_name) {
    try {
      final Class<?> clazz = Class.forName(class_name);
      final Object instance = clazz.newInstance();
      if (!(instance instanceof SecretProvider)) {
        throw new IllegalArgumentException("Class: " + class_name 
            + " was not an instance of SecretProvider.");
      }
      return (SecretProvider) instance;
    } catch (ClassNotFoundException e) { 
     final SecretProvider provider = 
         PluginLoader.loadSpecificPlugin(class_name, SecretProvider.class);
      if (provider == null) {
        throw new IllegalArgumentException("No class found for: " 
            + class_name);
      }
      return provider;
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Unable to instantiate class: " 
          + class_name, e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Not allowed to instantiate class: " 
          + class_name, e);
    }
  }
}
