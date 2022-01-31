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

import com.google.common.collect.Maps;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.opentsdb.data.LowLevelTimeSeriesData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * A consumer thread that will route the data through the converter to the
 * consumer.
 */
public class SimpleKafkaConsumerThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaConsumerThread.class);

  private ConsumerConnector consumerConnector;
  private Map<String, Integer> topicMap;
  private String topic;
  private Properties properties;
  private final TimeSeriesDataConsumer destination;
  private final TimeSeriesDataConverter converter;

  public SimpleKafkaConsumerThread(final String topic,
                                   final String consumerId,
                                   final SimpleKafkaConsumerService service) {
    this.properties = (Properties) service.properties.clone();
    this.properties.put("consumer.id", consumerId);
    this.destination = service.destination;
    this.converter = service.converter;
    setName("KafkaConsumer_" + service.id() + "_" + consumerId);
    this.topic = topic;
    topicMap = Maps.newHashMap();
    topicMap.put(topic, 1);
  }

  @Override
  public void run() {
    try {
      LOG.info("Kafka consumer properties: {}", properties);
      consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(
              new ConsumerConfig(properties));
      final KafkaStream<byte[], byte[]> kafkaStream =
              consumerConnector.createMessageStreams(topicMap).get(topic).get(0);
      for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : kafkaStream) {
        try {
          final LowLevelTimeSeriesData data =
                  converter.convertLowLevelData(messageAndMetadata.message());
          destination.write(null, data, null);
        } catch (Exception e) {
          LOG.error("Failed to parse a message", e);
        }
      }
    } catch (Throwable t) {
      LOG.error("Unexpected exception consuming from Kafka. Shutting down " +
              "consumer " + properties.get("consumer.id"), t);
    }
    shutdown();
  }

  void shutdown() {
    if (consumerConnector != null) {
      consumerConnector.shutdown();
      consumerConnector = null;
    }
  }
}
