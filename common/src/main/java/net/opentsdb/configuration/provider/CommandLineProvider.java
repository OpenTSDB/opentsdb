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

import com.google.common.base.Strings;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationOverride;
import net.opentsdb.utils.ArgP;

/**
 * Handles parsing the command line arguments sent to the JVM. Since 
 * args are sent only once, this implementation will not be reloading.
 * 
 * @since 3.0
 */
public class CommandLineProvider extends BaseProvider {
  public static final String SOURCE = CommandLineProvider.class.getSimpleName();
  
  /** The arguments. */
  private final String[] args;
  
  /**
   * Default ctor.
   * @param factory A non-null provider factory.
   * @param config A non-null config object we belong to.
   * @param timer A non-null timer object.
   * @param reload_keys A non-null (possibly empty) set of keys to reload.
   * @param args A non-null array of CLI arguments, may be empty.
   * @throws IllegalArgumentException if a required parameter is missing.
   */
  public CommandLineProvider(final ProviderFactory factory, 
                  final Configuration config, 
                  final HashedWheelTimer timer,
                  final Set<String> reload_keys,
                  final String[] args) {
    super(factory, config, timer, reload_keys);
    if (args == null) {
      throw new IllegalArgumentException("Args cannot be null.");
    }
    this.args = args;
  }
  
  @Override
  public ConfigurationOverride getSetting(final String key) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    final String dashed_key = "--" + key;
    final ArgP argp = new ArgP(false);
    argp.addOption(dashed_key, "ignored");
    argp.parse(args);
    
    if (!argp.has(dashed_key)) {
      return null;
    }
    return ConfigurationOverride.newBuilder()
        .setSource(SOURCE)
        .setValue(argp.get(dashed_key))
        .build();
  }

  @Override
  public String source() {
    return SOURCE;
  }

  @Override
  public void close() throws IOException {
    // No-op
    }

  @Override
  public void reload() {
    // no-op
  }
  
  public static class CommandLine implements ProviderFactory {

    @Override
    public Provider newInstance(final Configuration config, 
                                final HashedWheelTimer timer,
                                final Set<String> reload_keys) {
      throw new UnsupportedOperationException("Cannot instantiate a "
          + "CommandLine instance this way.");
    }
    
    @Override
    public boolean isReloadable() {
      return false;
    }

    @Override
    public String description() {
      // TODO Auto-generated method stub
      return "Parses settings from the commandline in the format "
          + "'--key=value --key2=value2'";
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
