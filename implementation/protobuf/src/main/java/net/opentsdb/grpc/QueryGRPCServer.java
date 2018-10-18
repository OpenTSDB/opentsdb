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

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.pbuf.QueryResultPB.QueryResult;
import net.opentsdb.data.pbuf.TimeSeriesQueryPB.TimeSeriesQuery;
import net.opentsdb.grpc.QueryGRPCSink.GRPCSinkConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.query.execution.serdes.JsonV2QuerySerdesOptions;
import net.opentsdb.query.serdes.PBufSerdesFactory;
import net.opentsdb.rpc.RPCServer;
import net.opentsdb.utils.JSON;

/**
 * A GRPC server endpoint for hosting TSDB queries. More work to do.
 * 
 * TODO - tracing
 * TODO - more options
 * 
 * @since 3.0
 */
public class QueryGRPCServer extends QueryRpcBetaGrpc.QueryRpcBetaImplBase 
    implements RPCServer {
  private static final Logger LOG = LoggerFactory.getLogger(QueryGRPCServer.class);
  
  public static final String TYPE = "GRPCQueryServer";
  public static final String CERTIFICATE_KEY = "grpc.server.tls.certificate";
  public static final String KEY_KEY = "grpc.server.tls.key";
  public static final String PORT_KEY = "grpc.server.port";
  
  private String id;
  
  /** The TSDB to which we belong. */
  private TSDB tsdb;
  
  /** The RPC server. */
  private Server server;
  
  @Override
  public void remoteQuery(final TimeSeriesQuery query, 
                          final StreamObserver<QueryResult> observer) {
    final SemanticQuery.Builder query_builder;
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("QUERY=" + new String(query.getQuery().toByteArray()));
      }
      final JsonNode node = JSON.getMapper().readTree(query.getQuery().newInput());
      query_builder = SemanticQuery.parse(tsdb, node);
    } catch (Exception e) {
      LOG.error("Failed to parse query: " 
          + new String(query.getQuery().toByteArray()), e);
      observer.onError(e);
      return;
    }
    
    try {
      final SemanticQuery semantic_query = query_builder
          .addSerdesConfig(JsonV2QuerySerdesOptions.newBuilder()
              .setId(PBufSerdesFactory.TYPE)
              .build())
          .build();
      final SemanticQueryContext context = (SemanticQueryContext) 
          SemanticQueryContext.newBuilder()
              .setTSDB(tsdb)
              .setQuery(semantic_query)
              .addSink(GRPCSinkConfig.newBuilder()
                  .setObserver(observer)
                  .setOptions(JsonV2QuerySerdesOptions.newBuilder()
                    .setId(PBufSerdesFactory.TYPE)
                    .build())
                  .build())
              .build();
      context.initialize(null /* TODO */).join(1000);
      context.fetchNext(null /* TODO */);
    } catch (Throwable t) {
      LOG.error("Unexpected exception", t);
      observer.onError(t);
    }
  }

  @Override
  public String id() {
    return id;
  }
  
  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    
    // TODO - more config options
    registerConfig(tsdb);
    try {
      final ServerBuilder<?> builder = ServerBuilder
          .forPort(tsdb.getConfig().getInt(PORT_KEY))
          .compressorRegistry(CompressorRegistry.getDefaultInstance())
          .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
          .addService(this);
      if (!Strings.isNullOrEmpty(tsdb.getConfig().getString(CERTIFICATE_KEY))) {
        LOG.info("Starting GRPC server with TLS and certificate: " 
            + tsdb.getConfig().getString(CERTIFICATE_KEY));
        builder.useTransportSecurity(
            new File(tsdb.getConfig().getString(CERTIFICATE_KEY)), 
            new File(tsdb.getConfig().getString(KEY_KEY)));
      }
      server = builder.build();
      server.start();
    } catch (Exception e) {
      LOG.error("Failed to start GRPC server", e);
      return Deferred.fromError(e);
    }
    
    LOG.info("GRPC Server started, listening on port: " + server.getPort());
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    if (server != null) {
      // TODO - do this asynchronously
      server.shutdown();
      try {
        server.awaitTermination();
      } catch (InterruptedException e) {
        return Deferred.fromError(e);
      }
    }
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }
  
  void registerConfig(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(CERTIFICATE_KEY)) {
      tsdb.getConfig().register(CERTIFICATE_KEY, null, false, 
          "The full path to a public certificate for the GRPC server.");
    }
    if (!tsdb.getConfig().hasProperty(KEY_KEY)) {
      tsdb.getConfig().register(KEY_KEY, null, false, 
          "The full path to a private key for the GRPC server.");
    }
    if (!tsdb.getConfig().hasProperty(PORT_KEY)) {
      tsdb.getConfig().register(PORT_KEY, 4243, false, 
          "A port to listen on for the GRPC server.");
    }
  }
  
}
