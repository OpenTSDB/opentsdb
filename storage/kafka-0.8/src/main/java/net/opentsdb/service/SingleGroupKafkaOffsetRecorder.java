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
package net.opentsdb.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import kafka.api.OffsetRequest;
import kafka.api.OffsetResponse;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.api.PartitionOffsetsResponse;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.consumer.SimpleConsumer;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Pair;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Records the lag for a single Kafka consumer group, e.g. for use with the
 * {@link net.opentsdb.storage.SimpleKafkaConsumerService}. It runs as a service
 * as well and will post to the stats object as the `kafka.lag` metric.
 * </p>
 * It also maintains a list of the last X lags for computing trends.
 *
 * @since 3.0
 */
public class SingleGroupKafkaOffsetRecorder extends BaseTSDBPlugin
        implements TSDBService, TimerTask {
  private static Logger LOG = LoggerFactory.getLogger(SingleGroupKafkaOffsetRecorder.class);
  private static final String TYPE = SingleGroupKafkaOffsetRecorder.class.getSimpleName();

  public static final String KEY_PREFIX = "kafka.offsetRecorder.";
  public static final String ZOOKEEPER_KEY = "zookeeper";
  public static final String GROUP_KEY = "group";
  public static final String INTERVAL_KEY = "interval";

  /** Used when a partition lacks an owner at the moment */
  public static final Option<String> NONE = Option.apply(null);
  
  private HashedWheelTimer timer;
  private Deque<Pair<Long, Long>> lag_history;
  private int measurement_size;
  private int interval;
  private AtomicBoolean running;
  private long last_start;
  
  private int zk_connect_timeout = 10000;
  private int zk_session_timeout = 10000;
  private int kafka_so_timeout = 10000;
  private int kafka_buffer_size = 100000;
  private String kafka_consumer_name = "TsdbKafkaOffsetRecorder";
  private String groupId;

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;
    registerConfigs(tsdb);

    timer = new HashedWheelTimer();
    timer.start();
    lag_history = new ArrayDeque<Pair<Long, Long>>();
    measurement_size = 60;
    interval = tsdb.getConfig().getInt(getConfigKey(INTERVAL_KEY));
    running = new AtomicBoolean(false);
    timer.newTimeout(this, interval, TimeUnit.SECONDS);
    groupId = tsdb.getConfig().getString(getConfigKey(GROUP_KEY));
    if (Strings.isNullOrEmpty(groupId)) {
      return Deferred.fromError(new IllegalArgumentException("The configuration "
        + getConfigKey(GROUP_KEY) + " cannot be null or empty."));
    }
    LOG.info("Initialized Kafka lag tracker to run every {}ms for the group {}",
            interval, groupId);
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    timer.stop();
    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }

  public List<Pair<Long, Long>> getHistory() {
    return Lists.newArrayList(lag_history);
  }
  
  @Override
  public void run(final Timeout ignored) throws Exception {
    last_start = DateTime.currentTimeMillis();
    if (running.compareAndSet(false, true)) {
      LOG.info("Running Kafka lag tracker.");
      long lag = 0;
      try {
        lag = getLag();
      } catch (Throwable t) {
        LOG.error("Failed to read the lag", t);
      }
      running.set(false);
      LOG.info("Completed running Kafka lag tracker: " + lag);
    } else {
      LOG.warn("WTF? The lag tracker was still running after " + interval + "ms??");
    }
    
    timer.newTimeout(this, 
        interval - (last_start - DateTime.currentTimeMillis()), 
        TimeUnit.MILLISECONDS);
  }
  
  long getLag() {
    final Map<Integer, String> broker_names = new HashMap<Integer, String>();
    final ZkClient zookeeper = new ZkClient(
            tsdb.getConfig().getString(getConfigKey(ZOOKEEPER_KEY)),
        zk_connect_timeout, 
        zk_session_timeout, 
        ZKStringSerializer$.MODULE$);
    
    // first off, get the list of broker IDs and setup consumers for each one.
    long lag = 0;
    final List<Object> brokerIDs = JavaConversions.asJavaList(
        ZkUtils.getSortedBrokerList(zookeeper));
    Map<Integer, SimpleConsumer> consumers = 
        new HashMap<Integer, SimpleConsumer>(brokerIDs.size());
    for (final Object bid : brokerIDs) {
      final Broker broker = ZkUtils.getBrokerInfo(zookeeper, (Integer)bid).get();
      final SimpleConsumer consumer = new SimpleConsumer(
          broker.host(), 
          broker.port(), 
          kafka_so_timeout, 
          kafka_buffer_size, 
          kafka_consumer_name);
      consumers.put((Integer)bid, consumer);
      broker_names.put((Integer)bid, broker.host());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Setup consumers for brokers: " + brokerIDs);
    }
    
    // TODO - does getChildren return an empty list if the path doens't exist?
    final List<String> groups = JavaConversions.asJavaList(
        ZkUtils.getChildren(zookeeper, ZkUtils.ConsumersPath()));
    if (groups == null || groups.isEmpty()) {
      LOG.error("No consumer groups found for cluster");
      return -1;
    }
    LOG.debug("Located consumer groups: " + groups);
    
    for (final String group : groups) {
      if (!group.equals(groupId)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping non-reporting group: " + group);
        }
        continue;
      }
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Working on group: " + group);
        }
        long group_start = DateTime.nanoTime();
        final Seq<String> topics = ZkUtils.getChildren(zookeeper, 
            ZkUtils.ConsumersPath() + "/" + group + "/offsets");
        
        final Map<String, Seq<Object>> topic_map = JavaConversions.mapAsJavaMap(
                ZkUtils.getPartitionsForTopics(zookeeper, topics));
        
        long orphans = 0;
        
        for (final Entry<String, Seq<Object>> entry : topic_map.entrySet()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Working topic: " + entry.getKey());
          }
          final List<Object> partitions = JavaConversions.asJavaList(entry.getValue());
          
          final Map<Integer, Map<TopicAndPartition, PartitionOffsetRequestInfo>> 
              brokers_to_requests = new HashMap<Integer, 
              Map<TopicAndPartition, PartitionOffsetRequestInfo>>(partitions.size());
          final Map<Integer, Entry<Long, String>> offsets_and_owners = new
              HashMap<Integer, Entry<Long, String>>(partitions.size());
          
          // build the map of broker ID to partition requests (to cut down on the 
          // calls to the Kafka brokers)
          for (final Object partitionId : partitions) {
            try {
              final long offset = Long.parseLong(
                  ZkUtils.readData(zookeeper, ZkUtils.ConsumersPath() + "/" + 
                      group + "/offsets/" + entry.getKey() + "/" + partitionId)._1());
              final Option<String> owner = ZkUtils.readDataMaybeNull(
                  zookeeper, ZkUtils.ConsumersPath() +  "/" + group + "/owners/" + 
                      entry.getKey() + "/" + partitionId)._1();
              if (owner == NONE) {
                orphans++;
              }
              final String owner_string = owner == NONE ? "None" : owner.get();
              offsets_and_owners.put((Integer)partitionId, 
                  new AbstractMap.SimpleEntry(offset, owner_string));
              
              final int brokerID = (Integer)ZkUtils.getLeaderForPartition(
                  zookeeper, entry.getKey(), (Integer)partitionId).get();
              final SimpleConsumer consumer = consumers.get(brokerID);
              if (consumer == null) {
                LOG.warn("No consumer found for broker ID: " + brokerID + 
                    " with topic + " + entry.getKey());
                continue;
              }
              
              final TopicAndPartition tap = new TopicAndPartition(entry.getKey(), 
                  (Integer)partitionId);
              final PartitionOffsetRequestInfo pori = 
                  new PartitionOffsetRequestInfo(OffsetRequest.LatestTime(), 1);
              
              Map<TopicAndPartition, PartitionOffsetRequestInfo> tapori =
                  brokers_to_requests.get(brokerID);
              if (tapori == null) {
                tapori = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
                brokers_to_requests.put(brokerID, tapori);
              }
              tapori.put(tap, pori);
            } catch (Exception e) {
              LOG.error("Failed to fetch data for partition: " + partitionId + 
                  " for topic " + entry.getKey(), e);
            }
          }
          
          if (LOG.isDebugEnabled()) {
            LOG.debug("Group to Broker map calls completed in " + 
                DateTime.msFromNanoDiff(DateTime.nanoTime(), group_start) + "ms for group " + group);
          }
          group_start = DateTime.nanoTime();
          
          // now that we have the requests, make em!
          for (final Entry<Integer, 
              Map<TopicAndPartition, PartitionOffsetRequestInfo>> btap : 
                brokers_to_requests.entrySet()) {
            try {
              final scala.collection.immutable.Map
                <TopicAndPartition, PartitionOffsetRequestInfo> tappor = 
                JavaConverters.mapAsScalaMapConverter(btap.getValue()).asScala()
                  .toMap(scala.Predef.<scala.Tuple2<TopicAndPartition, 
                    PartitionOffsetRequestInfo>>conforms());
              final SimpleConsumer consumer = consumers.get(btap.getKey());
              final OffsetRequest offset_request = new OffsetRequest(tappor, 0, 0);
              final OffsetResponse offset_response = 
                  consumer.getOffsetsBefore(offset_request);
              
              final Map<TopicAndPartition, PartitionOffsetsResponse> results = 
                  JavaConverters.mapAsJavaMapConverter(
                      offset_response.partitionErrorAndOffsets()).asJava();
              
              for (final Entry<TopicAndPartition, PartitionOffsetsResponse> tap : 
                results.entrySet()) {
                final PartitionOffsetsResponse result = tap.getValue();
                final List<Object> offset_list = JavaConversions.asJavaList(
                    result.offsets());
                if (offset_list == null || offset_list.isEmpty()) {
                  LOG.warn("No offsets found for partition " + 
                      tap.getKey().partition());
                  continue;
                }
                
                final long log_size = (Long)offset_list.get(0);
                final long offset = offsets_and_owners.get(tap.getKey().partition()).getKey();
                tsdb.getStatsCollector().setGauge("kafka.lag", (log_size - offset),
                    "group", group,
                    "topic", tap.getKey().topic(),
                    "partition", Integer.toString(tap.getKey().partition()),
                    "leader", broker_names.get(btap.getKey()));
                lag += (log_size - offset);
              }
            } catch (Exception e) {
              LOG.error("Failed issuing offset request for broker " + btap.getKey(), e);
            }
          }
          
          if (LOG.isDebugEnabled()) {
            LOG.debug("Broker processing calls completed in " + 
                DateTime.msFromNanoDiff(DateTime.nanoTime(), group_start) + "ms for group " + group);
          }
        }
        
        if (lag_history.size() >= measurement_size) {
          lag_history.removeLast();
        }
        lag_history.push(new Pair<Long, Long>(DateTime.currentTimeMillis() / 1000, lag));
      } catch (Exception e) {
        LOG.error("Unexpected exception processing group: " + group, e);
      }
    }
    
    // cleanup
    for (final SimpleConsumer consumer : consumers.values()) {
      consumer.close();
    }
    zookeeper.close();
    return lag;
  }

  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(getConfigKey(ZOOKEEPER_KEY))) {
      tsdb.getConfig().register(getConfigKey(ZOOKEEPER_KEY), null, false,
              "The list of one or more Zookeeper servers to connect to.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(GROUP_KEY))) {
      tsdb.getConfig().register(getConfigKey(GROUP_KEY), null, false,
              "The name of a group to monitor the lag for.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(INTERVAL_KEY))) {
      tsdb.getConfig().register(getConfigKey(INTERVAL_KEY), 60, false,
              "How often to run the recorder in seconds.");
    }
  }

  String getConfigKey(final String suffix) {
    return KEY_PREFIX + (Strings.isNullOrEmpty(id) || id.equals(TYPE) ?
            "" : id + ".")
            + suffix;
  }
}
