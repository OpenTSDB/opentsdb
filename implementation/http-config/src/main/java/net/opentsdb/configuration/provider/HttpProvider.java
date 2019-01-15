// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.io.Files;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;

/**
 * A provider that fetches configs from a web server given the full URI.
 * 
 * @since 3.0
 */
public class HttpProvider extends YamlJsonBaseProvider {
  private static final Logger LOG = LoggerFactory.getLogger(
      YamlJsonBaseProvider.class);
  
  private final static String CACHE_DIR_KEY = "config.http.cache.directory";
  private final static String CACHE_REPLACE = "[\\:/\\?]";
  
  private volatile boolean initialized;
  
  private final String cache_dir;
  
  public HttpProvider(final ProviderFactory factory, 
                      final Configuration config,
                      final HashedWheelTimer timer,
                      final String uri) {
    super(factory, config, timer, uri);
    
    cache_dir = System.getProperty(CACHE_DIR_KEY);
    try {
      reload();
    } catch (Throwable t) {
      LOG.error("Failed to load config: " + uri, t);
    }
  }

  @Override
  public void close() throws IOException {
    // no-op here.
  }
  
  @Override
  public void reload() {
    final HttpGet get = new HttpGet(uri);
    
    class ResponseCallback implements FutureCallback<HttpResponse> {

      @Override
      public void completed(final HttpResponse result) {
        if (result.getStatusLine().getStatusCode() != 200) {
          try {
            LOG.error("Error retrieving URI: " + uri + " => " 
                + result.getStatusLine().getStatusCode() + "\n" 
                + EntityUtils.toString(result.getEntity()));
          } catch (IOException e) {
            LOG.error("Failed to parse data for URI: " + uri, e);
          }
          if (!initialized && !Strings.isNullOrEmpty(cache_dir)) {
            loadFromCache();
          }
        } else {
          try {
            final String data = EntityUtils.toString(result.getEntity());
            long hash = Const.HASH_FUNCTION().hashString(data, Const.UTF8_CHARSET).asLong();
            if (hash == last_hash) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("No changes to URI: " + uri);
              }
              return;
            }
            last_hash = hash;
            
            parse(new ByteArrayInputStream(data.getBytes(Const.UTF8_CHARSET)));
            
            if (!Strings.isNullOrEmpty(cache_dir)) {
              final String file = cache_dir.endsWith("/") ? 
                  cache_dir + getFileName() : 
                    cache_dir + "/" + getFileName();
              try {
                Files.write(data.getBytes(Const.UTF8_CHARSET), new File(file));
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Successfully wrote cache file: " + file);
                }
              } catch (Exception e) {
                LOG.error("Failed to write cache file: " + file, e);
              }
            }
            
            if (LOG.isDebugEnabled()) {
              LOG.debug("Succesfully loaded: " + uri);
            }
          } catch (IOException e) {
            LOG.error("Failed to parse data for URI: " + uri, e);
            if (!initialized && !Strings.isNullOrEmpty(cache_dir)) {
              loadFromCache();
            }
          }
        }
      }

      @Override
      public void failed(final Exception ex) {
        if (!initialized && !Strings.isNullOrEmpty(cache_dir)) {
          loadFromCache();
        }
        LOG.error("Failed to retrieve URI: " + uri, ex);
      }

      @Override
      public void cancelled() {
        LOG.error("Request was cancellend when retrieving URI: " + uri);
      }
      
    }
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("Requesting URI: " + uri);
    }
    
    if (!initialized) {
      try {
        final Future<HttpResponse> future = 
            ((HttpProviderFactory) factory).client().execute(get, null);
        new ResponseCallback().completed(future.get());
      } catch (Exception e) {
        LOG.error("Failed to fetch URI: " + uri, e);
        if (!Strings.isNullOrEmpty(cache_dir)) {
          loadFromCache();
        }
      }
      initialized = true;
    } else {
      ((HttpProviderFactory) factory).client().execute(get, new ResponseCallback());
    }
  }

  String getFileName() {
    return uri.replaceAll(CACHE_REPLACE, "_");
  }
  
  void loadFromCache() {
    final String path = cache_dir.endsWith("/") ? 
        cache_dir + getFileName() : 
          cache_dir + "/" + getFileName();
    LOG.warn("Attempting to bootstrap from cache: " + path);
    final File file = new File(path);
    if (!file.exists()) {
      throw new IllegalStateException("Failed to read from URI: " + uri 
          + " and no cache file at: " + path);
    }
    
    InputStream stream = null;
    try {
      stream = Files.asByteSource(file).openStream();
      parse(stream);
      LOG.warn("Successfully loaded cache config: " + path);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read from URI: " + uri 
          + " and failed to read file at: " + path);
    } finally {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException e) {
          LOG.warn("Failed to close stream: " + path);
        }
      }
    }
  }
}
