// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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

import com.google.common.base.Strings;
import com.oath.auth.KeyRefresher;
import com.oath.auth.Utils;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A shared Http client implementation that uses Athenz certificates for
 * MTLS authentication.
 *
 * TODO - setup configurations for the http client params.
 *
 * @since 3.0
 */
public class AthenzMTLSHttpClient extends BaseTSDBPlugin implements SharedHttpClient {
  private static final Logger LOG = LoggerFactory.getLogger(AthenzMTLSHttpClient.class);

  public static final String TYPE = "AthenzMTLSHttpClient";

  public static final String KEY_PREFIX = "athenz.httpclient.";
  public static final String TRUSTSORE_KEY = "tls.truststore";
  public static final String TRUSTSORE_PASS_KEY = "tls.truststore.pass";
  public static final String CA_KEY = "tls.ca";
  public static final String ATHENZ_KEY_KEY = "tls.key";
  public static final String ATHENZ_CERT_KEY = "tls.cert";

  /** The client. */
  protected CloseableHttpAsyncClient client;
  protected KeyRefresher key_refresher;

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;
    registerConfigs(tsdb);

    try {
      final String ca = tsdb.getConfig().getString(getConfigKey(CA_KEY));
      if (!Strings.isNullOrEmpty(ca)) {
        key_refresher = Utils
                .generateKeyRefresherFromCaCert(
                        ca,
                        tsdb.getConfig().getString(getConfigKey(ATHENZ_CERT_KEY)),
                        tsdb.getConfig().getString(getConfigKey(ATHENZ_KEY_KEY)));
      } else {
        key_refresher = Utils
                .generateKeyRefresher(
                        tsdb.getConfig().getString(getConfigKey(TRUSTSORE_KEY)),
                        tsdb.getConfig().getString(getConfigKey(TRUSTSORE_PASS_KEY)),
                        tsdb.getConfig().getString(getConfigKey(ATHENZ_CERT_KEY)),
                        tsdb.getConfig().getString(getConfigKey(ATHENZ_KEY_KEY)));
      }
      key_refresher.startup();

      client = HttpAsyncClients.custom()
              .setDefaultIOReactorConfig(IOReactorConfig.custom()
                      .setSoReuseAddress(true)
                      .setSoKeepAlive(true)
                      .setTcpNoDelay(true)
                      .setIoThreadCount(Runtime.getRuntime().availableProcessors() * 2)
                      .build())
              .setMaxConnTotal(200)
              .setMaxConnPerRoute(25)
              .setSSLContext(Utils.buildSSLContext(
                      key_refresher.getKeyManagerProxy(),
                      key_refresher.getTrustManagerProxy()))
              .build();
      client.start();
    } catch (Exception e) {
      LOG.error("WTF? Couldn't start the refresher?", e);
      return Deferred.fromError(e);
    }
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

    if (key_refresher != null) {
      try {
        key_refresher.shutdown();
      } catch (Throwable t) {
        LOG.error("Failed to shutdown key refresher", t);
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

  @Override
  public CloseableHttpAsyncClient getClient() {
    return client;
  }

  String getConfigKey(final String key) {
    return KEY_PREFIX + (Strings.isNullOrEmpty(id) ? "" : id + ".") + key;
  }

  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(getConfigKey(TRUSTSORE_KEY))) {
      tsdb.getConfig().register(getConfigKey(TRUSTSORE_KEY), null, false,
              "The truststore for Athenz.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(TRUSTSORE_PASS_KEY))) {
      tsdb.getConfig().register(getConfigKey(TRUSTSORE_PASS_KEY), null, false,
              "The password for the truststore.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(ATHENZ_KEY_KEY))) {
      tsdb.getConfig().register(getConfigKey(ATHENZ_KEY_KEY), null, false,
              "The path to the Athenz key that is refreshed by SIA.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(ATHENZ_CERT_KEY))) {
      tsdb.getConfig().register(getConfigKey(ATHENZ_CERT_KEY), null, false,
              "The path to the Athenz cert that is refreshed by SIA.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(CA_KEY))) {
      tsdb.getConfig().register(getConfigKey(CA_KEY), null, false,
              "The path to a CA cert to use instead of the trust store.");
    }
  }

}
