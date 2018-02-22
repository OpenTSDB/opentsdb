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
 * The factory for parsing properties files, basic key = value files.
 * 
 * @since 3.0
 */
public class PropertiesFileFactory implements ProtocolProviderFactory, ProviderFactory {
  public static final String PROTOCOL = "file://";
  
  @Override
  public boolean handlesProtocol(final String uri) {
    if (!uri.toLowerCase().startsWith(PROTOCOL)) {
      return false;
    }
    if (uri.toLowerCase().endsWith(".conf") ||
        uri.toLowerCase().endsWith(".config") ||
        uri.toLowerCase().endsWith(".txt") ||
        uri.toLowerCase().endsWith(".properties")) {
      return true;
    }
    return false;
  }

  @Override
  public Provider newInstance(final Configuration config, 
                              final HashedWheelTimer timer,
                              final Set<String> reload_keys) {
    return new PropertiesFileProvider(this, config, timer, reload_keys);
  }
  
  @Override
  public Provider newInstance(final Configuration config, 
                              final HashedWheelTimer timer,
                              final Set<String> reload_keys,
                              final String uri) {
    return new PropertiesFileProvider(this, config, timer, reload_keys, uri);
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
