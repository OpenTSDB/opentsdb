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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.stumbleupon.async.Deferred;

import io.grpc.BindableService;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.pbuf.QueryResultPB;
import net.opentsdb.query.QueryDataSourceFactory;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.filter.MetricLiteralFactory;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.QueryFilterFactory;
import net.opentsdb.query.serdes.PBufSerdesFactory;
import net.opentsdb.query.serdes.SerdesFactory;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ QueryGRPCServer.class, ServerBuilder.class,
  SemanticQueryContext.class, SemanticQueryContext.Builder.class,
  SemanticQuery.class, SemanticQuery.Builder.class })
public class TestQueryGRPCServer {

  private static MockTSDB TSDB;
  
  private ServerBuilder server_builder;
  private Server server;
  private QueryContext context;
  private SemanticQueryContext.Builder ctx_builder;
  private SemanticQueryContext ctx;
  
  @BeforeClass
  public static void beforeClasss() {
    TSDB = new MockTSDB();
    when(TSDB.registry.getQueryNodeFactory("datasource"))
      .thenReturn(new QueryDataSourceFactory());
    when(TSDB.registry.getPlugin(QueryFilterFactory.class, "MetricLiteral"))
      .thenReturn(new MetricLiteralFactory());
  }
  
  @Before
  public void before() throws Exception {
    server_builder = mock(ServerBuilder.class);
    server = mock(Server.class);
    context = mock(QueryContext.class);
    ctx_builder = mock(SemanticQueryContext.Builder.class);
    ctx = mock(SemanticQueryContext.class);

    PowerMockito.mockStatic(ServerBuilder.class);
    PowerMockito.when(ServerBuilder.forPort(anyInt()))
      .thenReturn(server_builder);
    when(server_builder.addService(any(BindableService.class)))
      .thenReturn(server_builder);
    when(server_builder.useTransportSecurity(any(File.class), any(File.class)))
      .thenReturn(server_builder);
    when(server_builder.compressorRegistry(any(CompressorRegistry.class)))
      .thenReturn(server_builder);
    when(server_builder.decompressorRegistry(any(DecompressorRegistry.class)))
      .thenReturn(server_builder);
    when(server_builder.build()).thenReturn(server);
    when(server.start()).thenReturn(server);
    
    PowerMockito.mockStatic(SemanticQueryContext.class);
    when(SemanticQueryContext.newBuilder()).thenReturn(ctx_builder);
    when(ctx_builder.setTSDB(TSDB)).thenReturn(ctx_builder);
    when(ctx_builder.setQuery(any(TimeSeriesQuery.class)))
      .thenReturn(ctx_builder);
    when(ctx_builder.addSink(any()))
    .thenReturn(ctx_builder);
    when(ctx_builder.build()).thenReturn(ctx);
    
    when(context.tsdb()).thenReturn(TSDB);
    when(ctx.initialize(any(Span.class)))
      .thenAnswer(new Answer<Deferred<Void>>() {
      @Override
      public Deferred<Void> answer(InvocationOnMock invocation)
          throws Throwable {
        return Deferred.fromResult(null);
      }
    });
    when(TSDB.getRegistry().getPlugin(SerdesFactory.class, PBufSerdesFactory.ID))
      .thenReturn(new PBufSerdesFactory());
  }
  
  @Test
  public void initialize() throws Exception {
    QueryGRPCServer rpc = new QueryGRPCServer();
    assertNull(rpc.initialize(TSDB).join(1));
    
    verify(server, times(1)).start();
    
    when(server.start()).thenThrow(new UnitTestException());
    rpc = new QueryGRPCServer();
    try {
      rpc.initialize(TSDB).join(1);
      fail("Expected UnitTestException");
    } catch (UnitTestException e) { }
  }
  
  @Test
  public void shutdown() throws Exception {
    QueryGRPCServer rpc = new QueryGRPCServer();
    assertNull(rpc.initialize(TSDB).join(1));
    
    assertNull(rpc.shutdown().join(1));
    verify(server, times(1)).shutdown();
  }
  
  @Test
  public void remoteQuery() throws Exception {
    QueryGRPCServer rpc = new QueryGRPCServer();
    assertNull(rpc.initialize(TSDB).join(1));
    
    SemanticQuery q = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .setExecutionGraph(Lists.newArrayList(
            QuerySourceConfig.newBuilder()
              .setMetric(MetricLiteralFilter.newBuilder()
                  .setMetric("sys.cpu.user")
                  .build())
              .setId("DataSource")
              .build()))
        .build();
    net.opentsdb.data.pbuf.TimeSeriesQueryPB.TimeSeriesQuery pbuf = 
        net.opentsdb.data.pbuf.TimeSeriesQueryPB.TimeSeriesQuery.newBuilder()
        .setQuery(ByteString.copyFrom(JSON.serializeToBytes(q)))
        .build();
    
    StreamObserver observer = mock(StreamObserver.class);
    rpc.remoteQuery(pbuf, observer);
    verify(ctx, times(1)).fetchNext(null);
    verify(observer, never()).onNext(any(QueryResultPB.QueryResult.class));
    verify(observer, never()).onCompleted();
    verify(observer, never()).onError(any(Throwable.class));
  }
  
  @Test
  public void remoteQueryFetchException() throws Exception {
    QueryGRPCServer rpc = new QueryGRPCServer();
    assertNull(rpc.initialize(TSDB).join(1));
    
    SemanticQuery q = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .setExecutionGraph(Lists.newArrayList(
            QuerySourceConfig.newBuilder()
              .setMetric(MetricLiteralFilter.newBuilder()
                  .setMetric("sys.cpu.user")
                  .build())
              .setId("DataSource")
              .build()))
        .build();
    net.opentsdb.data.pbuf.TimeSeriesQueryPB.TimeSeriesQuery pbuf = 
        net.opentsdb.data.pbuf.TimeSeriesQueryPB.TimeSeriesQuery.newBuilder()
        .setQuery(ByteString.copyFrom(JSON.serializeToBytes(q)))
        .build();
    
    doThrow(new UnitTestException()).when(ctx).fetchNext(null);
    StreamObserver observer = mock(StreamObserver.class);
    rpc.remoteQuery(pbuf, observer);
    verify(ctx, times(1)).fetchNext(null);
    verify(observer, never()).onNext(any(QueryResultPB.QueryResult.class));
    verify(observer, never()).onCompleted();
    verify(observer, times(1)).onError(any(UnitTestException.class));
  }
  
  @Test
  public void remoteQuerySinkOK() throws Exception {
    QueryGRPCServer rpc = new QueryGRPCServer();
    assertNull(rpc.initialize(TSDB).join(1));
    
    SemanticQuery q = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .setExecutionGraph(Lists.newArrayList(
            QuerySourceConfig.newBuilder()
              .setMetric(MetricLiteralFilter.newBuilder()
                  .setMetric("sys.cpu.user")
                  .build())
              .setId("DataSource")
              .build()))
        .build();
    net.opentsdb.data.pbuf.TimeSeriesQueryPB.TimeSeriesQuery pbuf = 
        net.opentsdb.data.pbuf.TimeSeriesQueryPB.TimeSeriesQuery.newBuilder()
        .setQuery(ByteString.copyFrom(JSON.serializeToBytes(q)))
        .build();
    when(ctx.query()).thenReturn(q);
    
    StreamObserver observer = mock(StreamObserver.class);
    rpc.remoteQuery(pbuf, observer);
    verify(ctx, times(1)).fetchNext(null);
    verify(observer, never()).onNext(any(net.opentsdb.query.QueryResult.class));
    verify(observer, never()).onCompleted();
    verify(observer, never()).onError(any(UnitTestException.class));
    
    net.opentsdb.query.QueryResult result = mock(net.opentsdb.query.QueryResult.class);
    when(result.dataSource()).thenReturn("UT");
    verify(ctx, times(1)).fetchNext(null);
    verify(observer, never()).onNext(any(QueryResultPB.QueryResult.class));
    verify(observer, never()).onCompleted();
    verify(observer, never()).onError(any(UnitTestException.class));
  }

}
