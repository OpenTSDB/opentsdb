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

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.Configuration;

/**
 * An interface that can be used by various configuration providers to
 * match a specific protocol, e.g. `file://` or `https://` and return
 * a new {@link Provider} instance to parse and handle that file.
 * 
 * @since 3.0
 */
public interface ProtocolProviderFactory extends ProviderFactory, Closeable {

  /**
   * The protocol specifier this factory handles.
   * @return A non-null and non-empty string.
   */
  public String protocol();
  
  /**
   * Parses the URI to determine if it can be handled by this Provider.
   * @param uri A non-null and non-empty URI string.
   * @return True if the Provider can parse it, false if not.
   */
  public boolean handlesProtocol(final String uri);
  
  /**
   * Return a new instance of the Provider configured with the given
   * URI. Same URI as sent to {@link #handlesProtocol(String)}.
   * @param config The non-null config this provider will belong to.
   * @param timer The non-null config timer.
   * @param uri A non-null and non-empty URI string.
   * @return The non-null provider instance.
   */
  public Provider newInstance(final Configuration config, 
                              final HashedWheelTimer timer,
                              final String uri);
  
}
