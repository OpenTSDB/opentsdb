// This file is part of OpenTSDB.
// Copyright (C) 2012018  The OpenTSDB Authors.
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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.api.client.util.Lists;
import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.query.serdes.TimeSeriesDataSerdes;
import net.opentsdb.storage.WriteStatus.WriteState;
import net.opentsdb.utils.Deferreds;

public class PubSubConsumer extends BaseTSDBPlugin implements 
    TimeSeriesDataConsumer, MessageReceiver {
  protected static final Logger LOG = LoggerFactory.getLogger(
      PubSubConsumer.class);
  
  public static final String TYPE = "GooglePubSubConsumer";
  
  /** Configuration keys. */
  public static final String PROJECT_NAME_KEY = "google.pubsub.publisher.project.id";
  public static final String TOPIC_KEY = "google.pubsub.publisher.topic";
  public static final String JSON_KEYFILE_KEY = "google.pubsub.publisher.auth.json.keyfile";
  public static final String SUBSCRIPTION_KEY = "google.pubsub.subscription.id";
  public static final String CONSUMER_COUNT_KEY = "google.pubsub.consumers.count";
  public static final String ACK_IMMEDIATELY_KEY = "google.pubsub.ack.immediately";
  public static final String SHARED_DATA_KEY = "google.pubsub.data.shared";

  /** The serdes implementation to use. */
  protected TimeSeriesDataSerdes serdes;
  
  /** The subscription name. */
  protected ProjectSubscriptionName subscription;
  
  /** The list of subscribers. */
  protected List<Subscriber> subscribers;
  
  /** The data store to write to. */
  protected WritableTimeSeriesDataStore data_store;
  
  /** Whether or not to ack messages immediately or wait for the store. */
  protected boolean ack_immediately;
  
  /** Whether or not we're consuming shared objects or stand-alone datum. */
  protected boolean shared_data;
  
  /**
   * Default ctor for the plugin.
   */
  public PubSubConsumer() {
    
  }
  
  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    registerConfigs(tsdb);
    final String project_id = tsdb.getConfig().getString(PROJECT_NAME_KEY);
    if (Strings.isNullOrEmpty(project_id)) {
      return Deferred.fromError(new IllegalArgumentException(
          "Project ID cannot be null or empty."));
    }
    final String topic_id = tsdb.getConfig().getString(TOPIC_KEY);
    if (Strings.isNullOrEmpty(topic_id)) {
      return Deferred.fromError(new IllegalArgumentException(
          "Topic ID cannot be null or empty."));
    }
    final String json_key = tsdb.getConfig().getString(JSON_KEYFILE_KEY);
    if (Strings.isNullOrEmpty(json_key)) {
      return Deferred.fromError(new IllegalArgumentException(
          "Json key path cannot be null or empty."));
    }
    final String subcription_id = tsdb.getConfig().getString(SUBSCRIPTION_KEY);
    if (Strings.isNullOrEmpty(subcription_id)) {
      return Deferred.fromError(new IllegalArgumentException(
          "Subscription id path cannot be null or empty."));
    }
    ack_immediately = tsdb.getConfig().getBoolean(ACK_IMMEDIATELY_KEY);
    final int consumers = tsdb.getConfig().getInt(CONSUMER_COUNT_KEY);
    shared_data = tsdb.getConfig().getBoolean(SHARED_DATA_KEY);
    
    serdes = tsdb.getRegistry().getDefaultPlugin(TimeSeriesDataSerdes.class);
    if (serdes == null) {
      return Deferred.fromError(new IllegalArgumentException(
          "Serdes can't be null."));
    }
    
    data_store = ((DefaultRegistry) tsdb.getRegistry()).getDefaultWriteStore();
    if (data_store == null) {
      return Deferred.fromError(new IllegalArgumentException(
          "No default data store found."));
    }
    
    subscription = ProjectSubscriptionName.of(project_id, subcription_id);
    
    try {
      final GoogleCredentials credentials = GoogleCredentials.fromStream(
          new FileInputStream(json_key));
      
      subscribers = Lists.newArrayListWithCapacity(consumers);
      for (int i = 0; i < consumers; i++) {
        Subscriber subscriber = Subscriber.newBuilder(subscription, this)
            .setCredentialsProvider(
                FixedCredentialsProvider.create(credentials))
            .build();
        subscribers.add(subscriber);subscriber.startAsync().addListener(new Listener() {
          
          public void failed(State from, Throwable failure) {
            LOG.error("Failed to initialize the subscriber", failure);
            failure.printStackTrace();
          }
          
          public void running() {
            
          }
          
          @Override
          public void terminated(final State from) {
            
          }
        }, MoreExecutors.directExecutor());
      }
      LOG.info("Initialized " + consumers + " PubSub consumers");
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return Deferred.fromError(e);
    } catch (IOException e) {
      e.printStackTrace();
      return Deferred.fromError(e);
    } catch (Throwable e) {
      e.printStackTrace();
    }
    
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    final List<Deferred<Object>> deferreds = 
        Lists.newArrayListWithCapacity(subscribers.size()); 
    for (final Subscriber subscriber : subscribers) {
      final Deferred<Object> deferred = new Deferred<Object>();
      deferreds.add(deferred);
      subscriber.stopAsync().addListener(new Listener() {
        @Override
        public void terminated(final State from) {
          deferred.callback(null);
        }
      }, MoreExecutors.directExecutor());
      
    }
    return Deferred.group(deferreds).addCallback(Deferreds.NULL_GROUP_CB);
  }

  @Override
  public String version() {
    return "3.0.0";
  }
  
  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(PROJECT_NAME_KEY)) {
      tsdb.getConfig().register(PROJECT_NAME_KEY, null, false, 
          "The project ID for the pubsub publisher.");
    }
    if (!tsdb.getConfig().hasProperty(TOPIC_KEY)) {
      tsdb.getConfig().register(TOPIC_KEY, null, false, 
          "The topic ID to publish to.");
    }
    if (!tsdb.getConfig().hasProperty(JSON_KEYFILE_KEY)) {
      tsdb.getConfig().register(JSON_KEYFILE_KEY, null, false, 
          "The full path to a JSON service key file.");
    }
    if (!tsdb.getConfig().hasProperty(SUBSCRIPTION_KEY)) {
      tsdb.getConfig().register(SUBSCRIPTION_KEY, null, false, 
          "The subscription key to consume with.");
    }
    if (!tsdb.getConfig().hasProperty(CONSUMER_COUNT_KEY)) {
      tsdb.getConfig().register(CONSUMER_COUNT_KEY, 
          Runtime.getRuntime().availableProcessors(), false, 
          "The number of different consumers to start up for the same"
          + " subscription. Parallelizes the consumption. Defaults to "
          + "the number of processors found.");
    }
    if (!tsdb.getConfig().hasProperty(ACK_IMMEDIATELY_KEY)) {
      tsdb.getConfig().register(ACK_IMMEDIATELY_KEY, false, false, 
          "Whether or not to acknowledge the message immediately or to "
          + "wait for the data to be persisted before acking.");
    }
    if (!tsdb.getConfig().hasProperty(SHARED_DATA_KEY)) {
      tsdb.getConfig().register(SHARED_DATA_KEY, false, false, 
          "Whether or not this topic is receiving and parsing shared "
          + "data or stand-alone datum.");
    }
  }

  @Override
  public void receiveMessage(final PubsubMessage message, 
                             final AckReplyConsumer consumer) {
    try {
      if (shared_data) {
        final TimeSeriesSharedTagsAndTimeData data = 
            serdes.deserializeShared(null, 
                message.getData().newInput(), null);
        class ResultCB implements Callback<Object, List<WriteStatus>> {
          @Override
          public Object call(final List<WriteStatus> results) throws Exception {
            // TODO - requeue
            if (results.get(0).state() != WriteState.OK) {
              LOG.error("FAILED TO WRITE: " + results.get(0).message());
            }
            consumer.ack();
            return null;
          }
        }
        
        data_store.write(null, data, null).addCallback(new ResultCB())
          .addErrback(new Callback<Object, Exception>() {

          @Override
          public Object call(Exception arg) throws Exception {
            LOG.error("WTF??!?!?!", arg);
            return null;
          }
          
        });
        
        if (ack_immediately) {
          consumer.ack();
        }
      } else {
        final TimeSeriesDatum datum = serdes.deserializeDatum(null, 
            message.getData().newInput(), null);
        
        class ResultCB implements Callback<Object, WriteStatus> {
          @Override
          public Object call(final WriteStatus results) throws Exception {
            // TODO - requeue
            if (results.state() != WriteState.OK) {
              LOG.error("FAILED TO WRITE: " + results.message());
            }
            consumer.ack();
            return null;
          }
        }
        
        data_store.write(null, datum, null).addCallback(new ResultCB())
        .addErrback(new Callback<Object, Exception>() {

          @Override
          public Object call(Exception arg) throws Exception {
            LOG.error("WTF??!?!?!", arg);
            return null;
          }
          
        });
        if (ack_immediately) {
          consumer.ack();
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to process message", e);
    } catch (Throwable e) {
      LOG.error("Failed to process message", e);
    }
  }

}