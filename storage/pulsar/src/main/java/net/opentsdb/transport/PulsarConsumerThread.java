/*
 * This file is part of OpenTSDB.
 * Copyright (C) 2021  The OpenTSDB Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.opentsdb.transport;

import net.opentsdb.data.LowLevelTimeSeriesData;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarConsumerThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(PulsarConsumerThread.class);

  private final PulsarConsumerService service;
  private final int index;
  private PulsarClient pulsarClient;

  public PulsarConsumerThread(final PulsarConsumerService service, final int index) {
    this.service = service;
    this.index = index;
    try {
      pulsarClient = new PulsarClientImpl(service.clientConfig());
    } catch (PulsarClientException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void run() {
    try {
      final Reader<byte[]> reader = pulsarClient.newReader()
              .topic(service.topic())
              .readerName(service.topic()) // TODO - does it need to be unique?
              .startMessageId(service.earliest() ?
                      MessageId.earliest : MessageId.latest)
              .create();

      while (true) {
        try {
          final Message<byte[]> message = reader.readNext();
          final LowLevelTimeSeriesData data =
                  service.converter.convertLowLevelData(message.getData());
          service.destination.write(null, data, null);
        } catch (Exception e) {
          LOG.error("Failed to parse a message", e);
        }
      }
    } catch (Throwable t) {
      LOG.error("Unexpected exception consuming from Kafka. Shutting down " +
            "consumer", t);
    }
    shutdown();
  }

  void shutdown() {
    if (pulsarClient != null) {
      try {
        pulsarClient.shutdown();
      } catch (PulsarClientException e) {
        LOG.error("Failed to close pulsar client", e);
      }
      pulsarClient = null;
    }
  }
}
