// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;

/**
 * Handles parsing of JSON or YAML formatted files. Supports dotted paths
 * as long as each sub-path is an object. The root of the path can be
 * any object.
 * Use this class for complex configs that require maps, lists and possibly
 * POJOs in the source.
 * 
 * @since 3.0
 */
public class YamlJsonFileProvider extends YamlJsonBaseProvider {
  private static final Logger LOG = LoggerFactory.getLogger(
      YamlJsonFileProvider.class);
  
  /** The file name. */
  protected final String file_name;
  
  /**
   * Default ctor.
   * @param factory A non-null provider factory.
   * @param config A non-null config object we belong to.
   * @param timer A non-null timer object.
   * @param uri The URI to parse out. A file in this case.
   * @throws IllegalArgumentException if a required parameter is missing.
   */
  public YamlJsonFileProvider(final ProviderFactory factory, 
                              final Configuration config,
                              final HashedWheelTimer timer, 
                              final String uri) {
    super(factory, config, timer, uri);
    
    int idx = uri.toLowerCase().indexOf(FileFactory.PROTOCOL);
    if (idx < 0) {
      throw new IllegalArgumentException("File name did not start "
          + "with `" + FileFactory.PROTOCOL + "`: " + uri);
    }
    this.file_name = uri.substring(FileFactory.PROTOCOL.length());
    
    try {
      reload();
    } catch (Throwable t) {
      LOG.error("Failed to load config file: " + this.file_name, t);
    }
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
  
  @Override
  public void reload() {
    final File file = new File(file_name);
    if (!file.exists()) {
      LOG.warn("No file found at: " + file_name);
      return;
    }
    
    try {
      long hash = Files.asByteSource(file).hash(
          Const.HASH_FUNCTION()).asLong();
      // NOTE: Tiny possibility the initial file hash could hash to 0...
      // if that happens lemme know.
      if (hash == last_hash) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No changes to file: " + file_name);
        }
        return;
      }
      last_hash = hash;
    } catch (IOException e) {
      LOG.error("Failed to read the file at: " + file_name, e);
      return;
    }
    
    InputStream stream = null;
    try {
      stream = Files.asByteSource(file).openStream();
      parse(stream);
    } catch (IOException e) {
      LOG.error("Failed to open file: " + uri);
    } finally {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException e) {
          LOG.warn("Failed to close stream: " + uri);
        }
      }
    }
  }
  
}
