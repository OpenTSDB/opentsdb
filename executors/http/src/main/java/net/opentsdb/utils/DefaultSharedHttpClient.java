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
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

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
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.exceptions.RemoteQueryExecutionException;
import net.opentsdb.utils.RefreshingSSLContext.RefreshCallback;
import net.opentsdb.utils.RefreshingSSLContext.SourceType;

/**
 * A shared asynchronous HTTP client for making remote calls to various
 * services.
 * 
 * @since 3.0
 *
 */
public class DefaultSharedHttpClient extends BaseTSDBPlugin 
    implements SharedHttpClient, RefreshCallback {
  private static final Logger LOG = LoggerFactory.getLogger(
      DefaultSharedHttpClient.class);
  
  public static final String KEY_PREFIX = "httpclient.";
  public static final String TYPE_KEY = "tls.type";
  public static final String KEYSTORE_KEY = "tls.keystore";
  public static final String KEYSTORE_PASS_KEY = "tls.keystore.password";
  public static final String CERT_KEY = "tls.cert";
  public static final String KEY_KEY = "tls.key";
  public static final String CA_KEY = "tls.ca";
  public static final String SECRET_CERT_KEY = "tls.secret.cert";
  public static final String SECRET_KEY_KEY = "tls.secret.key";
  public static final String INTERVAL_KEY = "tls.interval";
  
  public static final String TYPE = "SharedHttpClient";
  
  /** The client. */
  protected volatile CloseableHttpAsyncClient client;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;
    registerConfigs(tsdb);
    if (!Strings.isNullOrEmpty(tsdb.getConfig().getString(getConfigKey(TYPE_KEY)))) {
      RefreshingSSLContext ctx = RefreshingSSLContext.newBuilder()
          .setType(SourceType.valueOf(tsdb.getConfig().getString(getConfigKey(TYPE_KEY))))
          .setKeystore(tsdb.getConfig().getString(getConfigKey(KEYSTORE_KEY)))
          .setKeystorePass(tsdb.getConfig().getString(getConfigKey(KEYSTORE_PASS_KEY)))
          .setCert(tsdb.getConfig().getString(getConfigKey(CERT_KEY)))
          .setKey(tsdb.getConfig().getString(getConfigKey(KEY_KEY)))
          .setCa(tsdb.getConfig().getString(getConfigKey(CA_KEY)))
          .setSecret_cert(tsdb.getConfig().getString(getConfigKey(SECRET_CERT_KEY)))
          .setSecret_key(tsdb.getConfig().getString(getConfigKey(SECRET_KEY_KEY)))
          .setInterval(tsdb.getConfig().getInt(getConfigKey(INTERVAL_KEY)))
          .setTsdb(tsdb)
          .setCallback(this)
          .build();
      client = HttpAsyncClients.custom()
          .setSSLContext(ctx.context())
          .setDefaultIOReactorConfig(IOReactorConfig.custom()
              .setIoThreadCount(8).build())
          .setMaxConnTotal(200)
          .setMaxConnPerRoute(25)
          .build();
    } else {
      // TODO - configs!
      client = HttpAsyncClients.custom()
        .setDefaultIOReactorConfig(IOReactorConfig.custom()
            .setIoThreadCount(8).build())
        .setMaxConnTotal(200)
        .setMaxConnPerRoute(25)
        .build();
    }
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
  public synchronized CloseableHttpAsyncClient getClient() {
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

  String getConfigKey(final String key) {
    return KEY_PREFIX + (Strings.isNullOrEmpty(id) ? "" : id + ".") + key;
  }
  
  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(getConfigKey(TYPE_KEY))) {
      tsdb.getConfig().register(getConfigKey(TYPE_KEY), null, false, 
          "The source of the certificate we're refreshing.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(KEYSTORE_KEY))) {
      tsdb.getConfig().register(getConfigKey(KEYSTORE_KEY), null, false, 
          "The full path to a keystore file.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(KEYSTORE_PASS_KEY))) {
      tsdb.getConfig().register(getConfigKey(KEYSTORE_PASS_KEY), null, false, 
          "The password for the keystore file.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(CERT_KEY))) {
      tsdb.getConfig().register(getConfigKey(CERT_KEY), null, false, 
          "The full path to a public certificate PEM file. May include "
          + "the RSA key.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(KEY_KEY))) {
      tsdb.getConfig().register(getConfigKey(KEY_KEY), null, false, 
          "The full path to an RSA key in PEM format.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(CA_KEY))) {
      tsdb.getConfig().register(getConfigKey(CA_KEY), null, false, 
          "The path to an optioanl Certificate Authority PEM file with "
          + "required CA certs.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(SECRET_CERT_KEY))) {
      tsdb.getConfig().register(getConfigKey(SECRET_CERT_KEY), null, false, 
          "The ID of a secret provider and key to use to fetch the public "
          + "certificate PEM formatted data. It may include the RSA key.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(SECRET_KEY_KEY))) {
      tsdb.getConfig().register(getConfigKey(SECRET_KEY_KEY), null, false, 
          "The ID of a secret provider and the key to use to fetch the private "
          + "key in PEM format.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(INTERVAL_KEY))) {
      tsdb.getConfig().register(getConfigKey(INTERVAL_KEY), 300000, false, 
          "The number of milliseconds to wait between refreshes.");
    }
  }

  @Override
  public void refresh(final SSLContext context) {
    final CloseableHttpAsyncClient existing = client;
    final CloseableHttpAsyncClient new_client = HttpAsyncClients.custom()
        .setSSLContext(context)
        .setDefaultIOReactorConfig(IOReactorConfig.custom()
            .setIoThreadCount(8).build())
        .setMaxConnTotal(200)
        .setMaxConnPerRoute(25)
        .build();
    new_client.start();
    synchronized (this) {
      client = new_client;
    }
    
    // we delay the closing to give existing calls time to finish.
    class Release implements TimerTask {
      @Override
      public void run(final Timeout ignored) throws Exception {
        existing.close();
      }
    }
    
    tsdb.getMaintenanceTimer().newTimeout(new Release(), 2, TimeUnit.MINUTES);
  }
}
