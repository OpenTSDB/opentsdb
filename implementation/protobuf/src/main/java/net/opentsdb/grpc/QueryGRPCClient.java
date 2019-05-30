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

import com.google.common.collect.Lists;
import com.google.protobuf.UnsafeByteOperations;

import io.grpc.stub.StreamObserver;
import net.opentsdb.data.PBufQueryResult;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.pbuf.QueryResultPB;
import net.opentsdb.data.pbuf.TimeSeriesQueryPB;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.JSON;

/**
 * The client that communicates with GRPC and passes data upstream.
 * 
 * @since 3.0
 */
public class QueryGRPCClient extends AbstractQueryNode implements 
    TimeSeriesDataSource, 
    StreamObserver<QueryResultPB.QueryResult> {

  /** The context. */
  private final QueryPipelineContext context;
  
  /** The query source config. */
  private final TimeSeriesDataSourceConfig config;
  
  /** The factory we came from. */
  private final QueryGRPCClientFactory factory;
  
  /**
   * Default ctor.
   * @param factory The non-null factory we came from.
   * @param context The non-null context we're a part of.
   * @param config The non-null config to parse and send over GRPC.
   */
  public QueryGRPCClient(final QueryGRPCClientFactory factory, 
                         final QueryPipelineContext context,
                         final TimeSeriesDataSourceConfig config) {
    super(null, context);
    this.context = context;
    this.factory = factory;
    this.config = config;
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {
    // NOTE That we're not closing the client connection here.
  }

  @Override
  public void onComplete(final QueryNode downstream, 
                         final long final_sequence,
                         final long total_sequences) {
    completeUpstream(final_sequence, total_sequences);
  }

  @Override
  public void onNext(final QueryResult next) {
    sendUpstream(next);
  }
  
  @Override
  public void onNext(final QueryResultPB.QueryResult next) {
    try {
      final PBufQueryResult result = new PBufQueryResult(
          factory.serdes_factory, 
          this, 
          null, 
          next);
      onNext(result);
      completeUpstream(0, 0);
    } catch (Throwable t) {
      sendUpstream(t);
    }
  }

  @Override
  public void onError(final Throwable t) {
    sendUpstream(t);
  }

  @Override
  public void fetchNext(final Span span) {
    try {
      // build a new semantic query
      SemanticQuery query = SemanticQuery.newBuilder()
          .setStart(context.query().getStart())
          .setEnd(context.query().getEnd())
          .setMode(context.query().getMode())
          .setTimeZone(context.query().getTimezone())
          .setExecutionGraph(
              config.getPushDownNodes() == null || 
              config.getPushDownNodes().isEmpty() ? 
                  Lists.newArrayList(config) : 
                    config.getPushDownNodes())
          .build();
      
      // TODO - tracing
      TimeSeriesQueryPB.TimeSeriesQuery pb_query = 
          TimeSeriesQueryPB.TimeSeriesQuery.newBuilder()
            .setQuery(UnsafeByteOperations.unsafeWrap(
                JSON.serializeToBytes(query)))
            .build();
      
      factory.stub().remoteQuery(pb_query, 
          (StreamObserver<QueryResultPB.QueryResult>) this);
    } catch (Exception e) {
      sendUpstream(e);
    }
  }

  @Override
  public void onCompleted() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public String[] setIntervals() {
    // TODO Auto-generated method stub
    return null;
  }

}
