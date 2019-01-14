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
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  
  private volatile boolean initialized;
  
  public HttpProvider(final ProviderFactory factory, 
                      final Configuration config,
                      final HashedWheelTimer timer,
                      final String uri) {
    super(factory, config, timer, uri);
    
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
          } catch (ParseException e) {
            LOG.error("Failed to parse data for URI: " + uri, e);
          } catch (IOException e) {
            LOG.error("Failed to parse data for URI: " + uri, e);
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
            
            if (LOG.isDebugEnabled()) {
              LOG.debug("Succesfully loaded: " + uri);
            }
          } catch (ParseException e) {
            LOG.error("Failed to parse data for URI: " + uri, e);
          } catch (IOException e) {
            LOG.error("Failed to parse data for URI: " + uri, e);
          }
        }
      }

      @Override
      public void failed(final Exception ex) {
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
    
    final Future<HttpResponse> future = 
        ((HttpProviderFactory) factory).client().execute(get, new ResponseCallback());
    if (!initialized) {
      initialized = true;
      try {
        new ResponseCallback().completed(future.get());
      } catch (InterruptedException e) {
        LOG.error("Failed to parse data for URI: " + uri, e);
      } catch (ExecutionException e) {
        LOG.error("Failed to parse data for URI: " + uri, e);
      }
    }
  }

}
