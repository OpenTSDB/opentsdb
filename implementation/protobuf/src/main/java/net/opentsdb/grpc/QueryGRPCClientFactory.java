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
package net.opentsdb.grpc;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.opentsdb.common.Const;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.grpc.QueryRpcBetaGrpc.QueryRpcBetaStub;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.serdes.PBufSerdesFactory;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;

/**
 * A factory for instantiating GRPC clients.
 * 
 * TODO - many more options like LB and such
 * 
 * @since 3.0
 */
public class QueryGRPCClientFactory extends BaseTSDBPlugin 
    implements TimeSeriesDataSourceFactory {
  private static final Logger LOG = LoggerFactory.getLogger(
      QueryGRPCClientFactory.class);
  
  public static final String TYPE = "GRPCQueryClient";
  
  public static final String PORT_KEY = "grpc.client.port";
  public static final String HOST_KEY = "grpc.client.host";
  
  /** The stub. */
  protected QueryRpcBetaStub stub;
  
  /** The serdes factory. */
  protected PBufSerdesFactory serdes_factory;
  
  /** The channel. */
  private ManagedChannel channel;
  
  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    registerConfig(tsdb);
    try {
      channel = ManagedChannelBuilder
          .forAddress(tsdb.getConfig().getString(HOST_KEY), 
                      tsdb.getConfig().getInt(PORT_KEY))
          .compressorRegistry(CompressorRegistry.getDefaultInstance())
          .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
          // TODO - many more config options.
          .build();
      stub = QueryRpcBetaGrpc.newStub(channel);
      LOG.info("Opened GPRC client connection to " 
          + tsdb.getConfig().getString(HOST_KEY) + ":" 
          + tsdb.getConfig().getString(PORT_KEY));
      serdes_factory = new PBufSerdesFactory();
    } catch (Exception e) {
      LOG.error("Unexpected exception setting up client.", e);
      return Deferred.fromError(e);
    }
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    if (channel != null) {
      // TODO - graceful
      channel.shutdownNow();
    }
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }
  
  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public boolean supportsPushdown(Class<? extends QueryNodeConfig> operation) {
    if (operation == DownsampleConfig.class) {
      return true;
    }
    // TODO - more!
    return false;
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException("Need a config.");
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final QueryNodeConfig config) {
    return new QueryGRPCClient(this, context, (TimeSeriesDataSourceConfig) config);
  }
  
  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(
      final TimeSeriesByteId id,
      final Span span) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(
      final List<String> join_keys,
      final Span span) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinMetrics(
      final List<String> join_metrics,
      final Span span) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RollupConfig rollupConfig() {
    return null;
  }
  
  void registerConfig(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(PORT_KEY)) {
      tsdb.getConfig().register(PORT_KEY, 4243, false, 
          "A port on the server to connect to for the GRPC client.");
    }
    if (!tsdb.getConfig().hasProperty(HOST_KEY)) {
      tsdb.getConfig().register(HOST_KEY, "localhost", false, 
          "The hostname for GRPC client to connect to.");
    }
  }
  
  public QueryRpcBetaStub stub() {
    return stub;
  }
  
  @Override
  public QueryNodeConfig parseConfig(final ObjectMapper mapper, 
                                     final TSDB tsdb,
                                     final JsonNode node) {
    return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
  }

  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final QueryNodeConfig config,
                         final QueryPlanner planner) {
    // no-op
  }

  @Override
  public boolean supportsQuery(TimeSeriesQuery query,
      TimeSeriesDataSourceConfig config) {
    // TODO Auto-generated method stub
    return true;
  }
  
}
