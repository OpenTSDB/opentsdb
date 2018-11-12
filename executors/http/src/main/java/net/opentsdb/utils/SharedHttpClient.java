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
package net.opentsdb.utils;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.entity.DeflateDecompressingEntity;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.exceptions.RemoteQueryExecutionException;

/**
 * A shared asynchronous HTTP client for making remote calls to various
 * services.
 * 
 * @since 3.0
 * @author clarsen
 *
 */
public class SharedHttpClient extends BaseTSDBPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(
      SharedHttpClient.class);
  
  public static final String TYPE = "SharedHttpClient";
  
  /** The client. */
  protected CloseableHttpAsyncClient client;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;
    
    // TODO - configs!
    client = HttpAsyncClients.custom()
      .setDefaultIOReactorConfig(IOReactorConfig.custom()
          .setIoThreadCount(8).build())
      .setMaxConnTotal(200)
      .setMaxConnPerRoute(25)
      .build();
    client.start();
    LOG.info("Initialized shared HTTP client.");
    return Deferred.fromResult(null);
  }
  
  @Override
  public Deferred<Object> shutdown() {
    if (client != null) {
      try {
        client.close();
      } catch (IOException e) {
        LOG.error("Failed to close HTTPClient", e);
      }
    }
    return Deferred.fromResult(null);
  }
  
  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  /**
   * NOTE: Do not close it.
   * @return The non-null client.
   */
  public CloseableHttpAsyncClient getClient() {
    return client;
  }
  
  /**
   * Helper that handles decompressing the result and parses the entity
   * to a string. If the entity is an exception then we through a 
   * {@link RemoteQueryExecutionException}.
   * @param response The non-null response to parse.
   * @param order The order of the result.
   * @param remote_host The remote host name.
   * @return A string if successful.
   */
  public static String parseResponse(final HttpResponse response,  
                                     final int order, 
                                     final String remote_host) {
    final String content;
    if (response.getEntity() == null) {
      throw new RemoteQueryExecutionException("Content for http response "
          + "was null: " + response, remote_host, order, 500);
    }
    
    try {
      final String encoding = (response.getEntity().getContentEncoding() != null &&
          response.getEntity().getContentEncoding().getValue() != null ?
              response.getEntity().getContentEncoding().getValue().toLowerCase() :
                "");
      if (encoding.equals("gzip") || encoding.equals("x-gzip")) {
        content = EntityUtils.toString(
            new GzipDecompressingEntity(response.getEntity()));
      } else if (encoding.equals("deflate")) {
        content = EntityUtils.toString(
            new DeflateDecompressingEntity(response.getEntity()));
      } else if (encoding.equals("")) {
        content = EntityUtils.toString(response.getEntity());
      } else {
        throw new RemoteQueryExecutionException("Unhandled content encoding [" 
            + encoding + "] : " + response, remote_host, 500, order);
      }
    } catch (ParseException e) {
      LOG.error("Failed to parse content from HTTP response: " + response, e);
      throw new RemoteQueryExecutionException("Content parsing failure for: " 
          + response, remote_host, 500, order, e);
    } catch (IOException e) {
      LOG.error("Failed to parse content from HTTP response: " + response, e);
      throw new RemoteQueryExecutionException("Content parsing failure for: " 
          + response, remote_host, 500, order, e);
    }
  
    if (response.getStatusLine().getStatusCode() == 200) {
      return content;
    }
    
    // NOTE assuming TSDB style exceptions
    if (content.startsWith("{")) {
      try {
        final JsonNode root = JSON.getMapper().readTree(content);
        JsonNode node = root.get("error");
        if (node != null && !node.isNull()) {
          final JsonNode message = node.get("message");
          if (message != null && !message.isNull()) {
            throw new RemoteQueryExecutionException(message.asText(), remote_host, 
                response.getStatusLine().getStatusCode(), order);
          }
        }
      } catch (IOException e) {
        LOG.warn("Failed to parse the JSON exception: " + content, e);
        // fall through
      }
    }
    // TODO - parse out the exception
    throw new RemoteQueryExecutionException(content, remote_host, 
        response.getStatusLine().getStatusCode(), order);
  }
}
