// This file is part of OpenTSDB.
// Copyright (C) 2015-2018  The OpenTSDB Authors.
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

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.query.serdes.TimeSeriesDataSerdes;
import net.opentsdb.stats.Span;

/**
 * A simple publisher for a Google PubSub topic for data points. Stub.
 * 
 * TODO - support for different credentials fetching methods
 * TODO - tuning
 * 
 * @since 3.0
 */
public class PubSubWriter extends BaseTSDBPlugin implements 
    WritableTimeSeriesDataStore {
  protected static final Logger LOG = LoggerFactory.getLogger(PubSubWriter.class);
  
  public static final String TYPE = "GooglePubSubWriter";
  
  /** Configuration keys. */
  public static final String PROJECT_NAME_KEY = "google.pubsub.publisher.project.id";
  public static final String TOPIC_KEY = "google.pubsub.publisher.topic";
  public static final String JSON_KEYFILE_KEY = "google.pubsub.publisher.auth.json.keyfile";
  
  /** The publisher instance we write with. */
  protected Publisher publisher;
  
  /** The name of the topic we're writing to. */
  protected ProjectTopicName topic;
  
  /** The serdes implementation to use. */
  protected TimeSeriesDataSerdes serdes;
  
  /**
   * Default ctor.
   */
  public PubSubWriter() {
  }
  
  @Override
  public Deferred<WriteStatus> write(final AuthState state, 
                                     final TimeSeriesDatum datum,
                                     final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getName() + ".write").start();
    } else {
      child = null;
    }
    
    final Deferred<WriteStatus> deferred = new Deferred<WriteStatus>();
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      serdes.serialize(null, datum, baos, null);
      final PubsubMessage message = PubsubMessage.newBuilder()
          .setData(ByteString.copyFrom(baos.toByteArray()))
          .build();
      
      final ApiFuture<String> future = publisher.publish(message);
      ApiFutures.addCallback(future, new ApiFutureCallback<String>() {
  
        @Override
        public void onFailure(final Throwable throwable) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Exception publishing to the PubSub endpoint", 
                throwable);
          }
          if (child != null) {
            child.setErrorTags(throwable).finish();
          }
          if (throwable instanceof ApiException) {
            final ApiException api_exception = ((ApiException) throwable);
            if (api_exception.isRetryable()) {
              deferred.callback(WriteStatus.retry(
                  api_exception.getStatusCode().getCode().toString()));
            } else {
              deferred.callback(WriteStatus.error(
                  api_exception.getStatusCode().getCode().toString(), 
                  throwable));
            }
          } else {
            deferred.callback(WriteStatus.error(
                throwable.getMessage(), throwable));
          }
        }
  
        @Override
        public void onSuccess(final String id) {
          if (child != null) {
            child.setSuccessTags().finish();
          }
          deferred.callback(WriteStatus.OK);
        }
      });
      return deferred;
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags(e).finish();
      }
      LOG.error("Unexpected exception publishing message", e);
      return Deferred.fromResult(WriteStatus.error(e.getMessage(), e));
    }
  }

  @Override
  public Deferred<List<WriteStatus>> write(final AuthState state,
                                           final TimeSeriesSharedTagsAndTimeData data, 
                                           final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getName() + ".write").start();
    } else {
      child = null;
    }
    
    final Deferred<List<WriteStatus>> deferred = 
        new Deferred<List<WriteStatus>>();
    
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      serdes.serialize(null, data, baos, null);
      final PubsubMessage message = PubsubMessage.newBuilder()
          .setData(ByteString.copyFrom(baos.toByteArray()))
          .build();
      
      final ApiFuture<String> future = publisher.publish(message);
      ApiFutures.addCallback(future, new ApiFutureCallback<String>() {
  
        @Override
        public void onFailure(final Throwable throwable) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Exception publishing to the PubSub endpoint", 
                throwable);
          }
          if (child != null) {
            child.setErrorTags(throwable).finish();
          }
          if (throwable instanceof ApiException) {
            final ApiException api_exception = ((ApiException) throwable);
            if (api_exception.isRetryable()) {
              deferred.callback(buildList(WriteStatus.retry(
                  api_exception.getStatusCode().getCode().toString()), 
                  data.size()));
            } else {
              deferred.callback(buildList(WriteStatus.error(
                  api_exception.getStatusCode().getCode().toString(), 
                    throwable), 
                  data.size()));
            }
          } else {
            deferred.callback(buildList(WriteStatus.error(
                throwable.getMessage(), throwable), data.size()));
          }
        }
  
        @Override
        public void onSuccess(final String id) {
          if (child != null) {
            child.setSuccessTags().finish();
          }
          deferred.callback(buildList(WriteStatus.OK, data.size()));
        }
      });
      return deferred;
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags(e).finish();
      }
      LOG.error("Unexpected exception publishing message", e);
      return Deferred.fromResult(buildList(
          WriteStatus.error(e.getMessage(), e),
          data.size()));
    }
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
    
    serdes = tsdb.getRegistry().getDefaultPlugin(TimeSeriesDataSerdes.class);
    if (serdes == null) {
      return Deferred.fromError(new IllegalArgumentException(
          "Serdes can't be null."));
    }
    
    try {
      final GoogleCredentials credentials = GoogleCredentials.fromStream(
          new FileInputStream(json_key));
      topic = ProjectTopicName.of(project_id, topic_id);
      publisher = Publisher.newBuilder(topic)
//          .setBatchingSettings(BatchingSettings.newBuilder()
//              .setElementCountThreshold(950L)
//              .setRequestByteThreshold(9500000L)
//              .build())
          .setCredentialsProvider(
              FixedCredentialsProvider.create(credentials))
          .build();
      LOG.info("Successfully initialized Google PubSub for topic: " + topic_id);
    } catch (FileNotFoundException e) {
      return Deferred.fromError(e);
    } catch (IOException e) {
      return Deferred.fromError(e);
    }

    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
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
  }

  List<WriteStatus> buildList(final WriteStatus status, final int size) {
    final List<WriteStatus> list = Lists.newArrayListWithExpectedSize(size);
    for (int i = 0; i < size; i++) {
      list.add(status);
    }
    return list;
  }
}
