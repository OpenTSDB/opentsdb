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

/**
 * The factory for parsing files. Uses the suffix to determine the type
 * of provider to return.
 * 
 * Returns a key = value properties config if the suffix is .conf, 
 * .config, .tx, .properties.
 * Returns a JSON/YAML parser if the suffix is .yaml, .yml, .json, .jsn. 
 * 
 * @since 3.0
 */
public class FileFactory implements ProtocolProviderFactory, ProviderFactory {
  public static final String PROTOCOL = "file://";
  
  @Override
  public boolean handlesProtocol(String uri) {
    uri = uri.toLowerCase();
    if (!uri.startsWith(PROTOCOL)) {
      return false;
    }
    if (uri.endsWith(".conf") ||
        uri.endsWith(".config") ||
        uri.endsWith(".txt") ||
        uri.endsWith(".properties") ||
        uri.endsWith(".yaml") || 
        uri.endsWith(".yml") ||
        uri.endsWith(".json") ||
        uri.endsWith(".jsn")) {
      return true;
    }
    return false;
  }

  @Override
  public Provider newInstance(final Configuration config, 
                              final HashedWheelTimer timer,
                              final Set<String> reload_keys) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public Provider newInstance(final Configuration config, 
                              final HashedWheelTimer timer,
                              final Set<String> reload_keys,
                              String uri) {
    uri = uri.toLowerCase();
    if (uri.endsWith(".conf") ||
        uri.endsWith(".config") ||
        uri.endsWith(".txt") ||
        uri.endsWith(".properties")) {
      return new PropertiesFileProvider(this, config, timer, reload_keys, uri);
    } else if (uri.endsWith(".yaml") || 
               uri.endsWith(".yml") ||
               uri.endsWith(".json") ||
               uri.endsWith(".jsn")) {
      return new YamlJsonFileProvider(this, config, timer, reload_keys, uri);
    } else {
      throw new UnsupportedOperationException("File type not recognized.");
    }
  }
  
  @Override
  public String protocol() {
    return PROTOCOL;
  }

  @Override
  public String description() {
    return "Returns a PropertiesFile provider to parse local files "
        + "ending with one of [.conf, .config, .txt, .properties].";
  }
  
  @Override
  public boolean isReloadable() {
    return true;
  }
  
  @Override
  public String simpleName() {
    return "PropertiesFile";
  }
  
  @Override
  public void close() throws IOException {
    // no-op
  }
  
}
