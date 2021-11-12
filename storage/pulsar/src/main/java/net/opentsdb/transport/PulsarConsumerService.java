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
package net.opentsdb.transport;

import net.opentsdb.service.TSDBService;
import net.opentsdb.storage.TimeSeriesDataConsumer;
import net.opentsdb.storage.TimeSeriesDataConsumerFactory;
import net.opentsdb.storage.TimeSeriesDataConverter;
import net.opentsdb.storage.TimeSeriesDataConverterFactory;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

public class PulsarConsumerService extends BaseTSDBPlugin implements
        TSDBService {
  protected static final Logger LOG = LoggerFactory.getLogger(PulsarConsumerService.class);
  
  public static final String TYPE = "PulsarConsumerService";

  public static final String KEY_PREFIX = "pulsar.";
  public static final String DESTINATION_KEY = "destination.id";
  public static final String CONVERTER_KEY = "converter.id";
  public static final String THREADS_KEY = "consumer.threads";

  public static final String SERVICE_URL_KEY = "service.url";
  public static final String TOPIC_KEY = "topic";
  public static final String EARLIEST_KEY = "read.earliest";
  public static final String TLS_KEY_KEY = "tls.path.key";
  public static final String TLS_CERT_KEY = "tls.path.cert";
  public static final String TLS_CA_KEY = "tls.path.ca";
  public static final String TLS_ENABLE_KEY = "tls.enable";

  protected TimeSeriesDataConsumer destination;
  protected TimeSeriesDataConverter converter;
  protected ClientConfigurationData clientConfig;
  protected PulsarConsumerThread[] threads;

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;

    this.tsdb = tsdb;
    this.id = id == null ? getClass().getSimpleName() : id;

    registerConfigs(tsdb);

    final int threads = tsdb.getConfig().getInt(getConfigKey(THREADS_KEY));
    if (threads < 1) {
      LOG.warn(getConfigKey(THREADS_KEY) + " was set to " + threads
              + " so no consumers will start.");
      return Deferred.fromResult(null);
    }

    String writerId = tsdb.getConfig().getString(getConfigKey(DESTINATION_KEY));
    final TimeSeriesDataConsumerFactory destinationFactory;
    if (Strings.isNullOrEmpty(writerId)) {
      destinationFactory = tsdb.getRegistry().getDefaultPlugin(TimeSeriesDataConsumerFactory.class);
    } else {
      destinationFactory = tsdb.getRegistry().getPlugin(TimeSeriesDataConsumerFactory.class, writerId);
    }
    if (destinationFactory == null) {
      return Deferred.fromError(new IllegalArgumentException(
              "No time series data consumer factory found for ID " +
                      (writerId == null ? "Default" : writerId)));
    }
    destination = destinationFactory.consumer();
    if (destination == null) {
      return Deferred.fromError(new IllegalArgumentException(
              "No time series data consumer returned for the factory with ID " +
                      (writerId == null ? "Default" : writerId)));
    }

    String converterId = tsdb.getConfig().getString(getConfigKey(CONVERTER_KEY));
    final TimeSeriesDataConverterFactory converterFactory;
    if (Strings.isNullOrEmpty(converterId)) {
      converterFactory = tsdb.getRegistry().getDefaultPlugin(TimeSeriesDataConverterFactory.class);
    } else {
      converterFactory = tsdb.getRegistry().getPlugin(TimeSeriesDataConverterFactory.class, converterId);
    }
    if (converterFactory == null) {
      return Deferred.fromError(new IllegalArgumentException(
              "No time series data converter factory found for ID " +
                      (converterId == null ? "Default" : converterId)));
    }
    converter = converterFactory.newInstance();
    if (converter == null) {
      return Deferred.fromError(new IllegalArgumentException(
              "No time series data converter returned for the factory with ID " +
                      (converterId == null ? "Default" : converterId)));
    }

    String serviceUrl = tsdb.getConfig().getString(getConfigKey(SERVICE_URL_KEY));
    if (Strings.isNullOrEmpty(serviceUrl)) {
      return Deferred.fromError(new IllegalArgumentException("Project URL "
              + getConfigKey(SERVICE_URL_KEY) + " was empty."));
    }

    if (Strings.isNullOrEmpty(tsdb.getConfig().getString(getConfigKey(TOPIC_KEY)))) {
      return Deferred.fromError(new IllegalArgumentException("Topic "
              + getConfigKey(TOPIC_KEY) + " was empty."));
    }

    clientConfig = new ClientConfigurationData();
    clientConfig.setServiceUrl(serviceUrl);

    if (tsdb.getConfig().getBoolean(getConfigKey(TLS_ENABLE_KEY))) {
      String keyPath = tsdb.getConfig().getString(getConfigKey(TLS_KEY_KEY));
      if (Strings.isNullOrEmpty(keyPath)) {
        return Deferred.fromError(new IllegalArgumentException("TLS was enabled but "
                + getConfigKey(TLS_KEY_KEY) + " was empty."));
      }

      String certPath = tsdb.getConfig().getString(getConfigKey(TLS_CERT_KEY));
      if (Strings.isNullOrEmpty(certPath)) {
        return Deferred.fromError(new IllegalArgumentException("TLS was enabled but "
                + getConfigKey(TLS_CERT_KEY) + " was empty."));
      }

      clientConfig.setAuthentication(new AuthenticationTls(certPath, keyPath));
      clientConfig.setTlsAllowInsecureConnection(false);
      clientConfig.setTlsHostnameVerificationEnable(true);
      clientConfig.setTlsTrustCertsFilePath(tsdb.getConfig().getString(getConfigKey(TLS_CA_KEY)));
      clientConfig.setUseTls(true);
    }

    this.threads = new PulsarConsumerThread[threads];
    for (int i = 0; i < threads; i++) {
      this.threads[i] = new PulsarConsumerThread(this, i);
      this.threads[i].start();
    }
    return Deferred.fromResult(null);
  }

  public ClientConfigurationData clientConfig() {
    return clientConfig;
  }

  public String topic() {
    return tsdb.getConfig().getString(getConfigKey(TOPIC_KEY));
  }

  public boolean earliest() {
    return tsdb.getConfig().getBoolean(getConfigKey(EARLIEST_KEY));
  }

  @Override
  public Deferred<Object> shutdown() {
    for (int i = 0; i < threads.length; i++) {
      threads[i].shutdown();
    }
    return Deferred.fromResult(null);
  }
  
  @Override
  public String type() {
    return TYPE;
  }
  
  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(getConfigKey(DESTINATION_KEY))) {
      tsdb.getConfig().register(getConfigKey(DESTINATION_KEY), null, false,
              "The ID of a TimeSeriesDataConsumer plugin.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(CONVERTER_KEY))) {
      tsdb.getConfig().register(getConfigKey(CONVERTER_KEY), null, false,
              "The ID of a time series data converter plugin.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(THREADS_KEY))) {
      tsdb.getConfig().register(getConfigKey(THREADS_KEY), 1, false,
              "The number of threads to spin up as consumers.");
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(SERVICE_URL_KEY))) {
      tsdb.getConfig().register(getConfigKey(SERVICE_URL_KEY), null, false,
          "The full service URL");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(TOPIC_KEY))) {
      tsdb.getConfig().register(getConfigKey(TOPIC_KEY), null, false,
              "The Topic name");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(EARLIEST_KEY))) {
      tsdb.getConfig().register(getConfigKey(EARLIEST_KEY), true, false,
              "Whether to read the earliest offset (true) or latest (false).");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(TLS_KEY_KEY))) {
      tsdb.getConfig().register(getConfigKey(TLS_KEY_KEY), null, false,
              "The path to an MTLS key.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(TLS_CERT_KEY))) {
      tsdb.getConfig().register(getConfigKey(TLS_CERT_KEY), null, false,
              "The path to an MTLS certificate.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(TLS_CA_KEY))) {
      tsdb.getConfig().register(getConfigKey(TLS_CA_KEY), null, false,
              "The path to an MTLS CA cert file.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(TLS_ENABLE_KEY))) {
      tsdb.getConfig().register(getConfigKey(TLS_ENABLE_KEY), false, false,
              "Whether or not MTLS is enabled.");
    }

  }

  String getConfigKey(final String suffix) {
    return KEY_PREFIX + (Strings.isNullOrEmpty(id) || id.equals(TYPE) ?
            "" : id + ".")
            + suffix;
  }

}
