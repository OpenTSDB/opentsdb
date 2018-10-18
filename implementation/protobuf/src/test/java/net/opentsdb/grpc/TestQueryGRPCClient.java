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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import io.grpc.stub.StreamObserver;
import net.opentsdb.data.pbuf.TimeSeriesQueryPB;
import net.opentsdb.data.pbuf.QueryResultPB.QueryResult;
import net.opentsdb.grpc.QueryRpcBetaGrpc.QueryRpcBetaStub;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.BaseTimeSeriesDataSourceConfig;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ QueryRpcBetaStub.class, QueryResult.class })
public class TestQueryGRPCClient {

  private QueryGRPCClientFactory factory;
  private QueryPipelineContext context;
  private QueryRpcBetaStub stub;
  private QueryNode upstream;
  
  @Before
  public void before() throws Exception {
    factory = mock(QueryGRPCClientFactory.class);
    context = mock(QueryPipelineContext.class);
    stub = mock(QueryRpcBetaStub.class);
    when(factory.stub()).thenReturn(stub);
    
    upstream = mock(QueryNode.class);
    when(context.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
  }
  
  @Test
  public void fetchNext() throws Exception {
    SemanticQuery q = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .setExecutionGraph(Lists.newArrayList(
            DefaultTimeSeriesDataSourceConfig.newBuilder()
              .setMetric(MetricLiteralFilter.newBuilder()
                  .setMetric("sys.cpu.user")
                  .build())
              .setId("DataSource")
              .build()))
        .build();
    when(context.query()).thenReturn(q);
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setId("DataSource")
        .build();
    
    QueryGRPCClient node = new QueryGRPCClient(factory, context, config);
    node.initialize(null);
    node.fetchNext(null);
    verify(stub, times(1)).remoteQuery(any(TimeSeriesQueryPB.TimeSeriesQuery.class), 
        any(StreamObserver.class));
    verify(upstream, never()).onNext(any(net.opentsdb.query.QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    // oops!
    doThrow(new UnitTestException()).when(stub)
    .remoteQuery(any(TimeSeriesQueryPB.TimeSeriesQuery.class), 
      any(StreamObserver.class));
    
    node = new QueryGRPCClient(factory, context, config);
    node.initialize(null);
    node.fetchNext(null);
    verify(upstream, never()).onNext(any(net.opentsdb.query.QueryResult.class));
    verify(upstream, times(1)).onError(any(UnitTestException.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
  }
  
  @Test
  public void onComplete() throws Exception {
    BaseTimeSeriesDataSourceConfig config = mock(BaseTimeSeriesDataSourceConfig.class);
    QueryGRPCClient node = new QueryGRPCClient(factory, context, config);
    node.initialize(null);
    verify(upstream, never()).onNext(any(net.opentsdb.query.QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    node.onComplete(node, 0, 0);
    verify(upstream, never()).onNext(any(net.opentsdb.query.QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, times(1)).onComplete(any(QueryNode.class), anyLong(), anyLong());
  }
  
  @Test
  public void onNextPbuf() throws Exception {
    BaseTimeSeriesDataSourceConfig config = mock(BaseTimeSeriesDataSourceConfig.class);
    QueryGRPCClient node = new QueryGRPCClient(factory, context, config);
    node.initialize(null);
    verify(upstream, never()).onNext(any(net.opentsdb.query.QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    node.onNext(mock(QueryResult.class));
    verify(upstream, times(1)).onNext(any(net.opentsdb.query.QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, times(1)).onComplete(any(QueryNode.class), anyLong(), anyLong());
  }
  
  @Test
  public void onError() throws Exception {
    BaseTimeSeriesDataSourceConfig config = mock(BaseTimeSeriesDataSourceConfig.class);
    QueryGRPCClient node = new QueryGRPCClient(factory, context, config);
    node.initialize(null);
    verify(upstream, never()).onNext(any(net.opentsdb.query.QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    node.onError(new UnitTestException());
    verify(upstream, never()).onNext(any(net.opentsdb.query.QueryResult.class));
    verify(upstream, times(1)).onError(any(UnitTestException.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
  }
  
}
