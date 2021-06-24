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
package net.opentsdb.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.service.TSDBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * A service that uses the high-level Kafka consumer to read from a single
 * topic. It will startup the configured number of threads to consume data
 * from the given topic.
 */
public class SimpleKafkaConsumerService extends BaseTSDBPlugin implements TSDBService {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaConsumerService.class);

  public static final String TYPE = SimpleKafkaConsumerService.class.getSimpleName();
  public static final TypeReference<Map<String, String>> MAP_TYPE_REFERENCE =
          new TypeReference<Map<String, String>>() {};
  public static final String KEY_PREFIX = "kafka.";
  public static final String DESTINATION_KEY = "destination.id";
  public static final String CONVERTER_KEY = "converter.id";
  public static final String ZOOKEEPER_KEY = "consumer.zookeeper";
  public static final String THREADS_KEY = "consumer.threads";
  public static final String TOPIC_KEY = "consumer.topic";
  public static final String GROUP_KEY = "consumer.group";
  public static final String ID_PREFIX_KEY = "consumer.id.prefix";
  public static final String USE_HOSTNAME_KEY = "consumer.id.use_host";
  public static final String PROPERTIES_KEY = "properties";

  protected TimeSeriesDataConsumer destination;
  protected TimeSeriesDataConverter converter;
  protected Properties properties;
  protected SimpleKafkaConsumerThread[] threads;

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
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

    properties = new Properties();
    final Map<String, String> configProperties = tsdb.getConfig().getTyped(
            getConfigKey(PROPERTIES_KEY), MAP_TYPE_REFERENCE);
    if (configProperties != null) {
      for (Entry<String, String> entry : configProperties.entrySet()) {
        properties.put(entry.getKey(), entry.getValue());
      }
    }

    final String zookeeper = tsdb.getConfig().getString(getConfigKey(ZOOKEEPER_KEY));
    if (Strings.isNullOrEmpty(ZOOKEEPER_KEY)) {
      return Deferred.fromError(new IllegalArgumentException(
              getConfigKey(ZOOKEEPER_KEY) + " cannot be null or empty."));
    }
    properties.put("zookeeper.connect", zookeeper);

    final String topic = tsdb.getConfig().getString(getConfigKey(TOPIC_KEY));
    if (Strings.isNullOrEmpty(topic)) {
      return Deferred.fromError(new IllegalArgumentException(
              getConfigKey(TOPIC_KEY) + " cannot be null or empty."));
    }

    final String group = tsdb.getConfig().getString(getConfigKey(GROUP_KEY));
    if (Strings.isNullOrEmpty(group)) {
      return Deferred.fromError(new IllegalArgumentException(
              getConfigKey(GROUP_KEY) + " cannot be null or empty."));
    }
    properties.put("group.id", group);

    String prefix = tsdb.getConfig().getString(getConfigKey(ID_PREFIX_KEY));
    final boolean useHostname = tsdb.getConfig().getBoolean(getConfigKey(USE_HOSTNAME_KEY));

    String hostname = null;
    if (useHostname) {
      try {
        hostname = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        return Deferred.fromError(new RuntimeException("Unable to get the hostname?", e));
      }
    }

    String consumerIdBase = prefix == null ? "" : prefix;
    if (useHostname) {
      consumerIdBase += hostname + "_";
    } else if (!Strings.isNullOrEmpty(consumerIdBase)) {
      consumerIdBase += "_";
    }

    this.threads = new SimpleKafkaConsumerThread[threads];
    for (int i = 0; i < threads; i++) {
      this.threads[i] = new SimpleKafkaConsumerThread(topic, consumerIdBase + i, this);
      this.threads[i].start();
    }
    return Deferred.fromResult(null);
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
    if (!tsdb.getConfig().hasProperty(getConfigKey(PROPERTIES_KEY))) {
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
              .setKey(getConfigKey(PROPERTIES_KEY))
              .setType(MAP_TYPE_REFERENCE)
              .setSource(TYPE)
              .setDescription("An optional map of kafka properties to pass to the consumer.")
              .isNullable()
              .build());
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(ZOOKEEPER_KEY))) {
      tsdb.getConfig().register(getConfigKey(ZOOKEEPER_KEY), null, false,
              "The list of one or more Zookeeper servers to connect to.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(THREADS_KEY))) {
      tsdb.getConfig().register(getConfigKey(THREADS_KEY), 1, false,
              "The number of threads to spin up as consumers.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(TOPIC_KEY))) {
      tsdb.getConfig().register(getConfigKey(TOPIC_KEY), null, false,
              "The single topic to consume from.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(GROUP_KEY))) {
      tsdb.getConfig().register(getConfigKey(GROUP_KEY), null, false,
              "The name of a consumer group to consume with.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(ID_PREFIX_KEY))) {
      tsdb.getConfig().register(getConfigKey(ID_PREFIX_KEY), null, false,
              "An optional prefix for the consumer ID. If set then the " +
                      "consumer ID would be <prefix>_<hostname>_<thread#> (with the" +
                      "hostname being optional)." +
                      "If null then it will just be <hostname>_<thread#> or " +
                      "just <thread#> if kafka.consumer.id.use_host is false.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(USE_HOSTNAME_KEY))) {
      tsdb.getConfig().register(getConfigKey(USE_HOSTNAME_KEY), true, false,
              "Whether or not to include the hostname in the consumer ID.");
    }
  }

  String getConfigKey(final String suffix) {
    return KEY_PREFIX + (Strings.isNullOrEmpty(id) || id.equals(TYPE) ?
            "" : id + ".")
            + suffix;
  }
}
