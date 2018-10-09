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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.expressions.ExpressionConfig;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ QueryGRPCClientFactory.class, ManagedChannelBuilder.class })
public class TestQueryGRPCClientFactory {

  private static MockTSDB TSDB;
  
  private ManagedChannelBuilder channel_builder;
  private ManagedChannel channel;
  
  @BeforeClass
  public static void beforeClasss() {
    TSDB = new MockTSDB();
  }
  
  @Before
  public void before() throws Exception {
    channel_builder = mock(ManagedChannelBuilder.class);
    channel = mock(ManagedChannel.class);
    
    PowerMockito.mockStatic(ManagedChannelBuilder.class);
    when(ManagedChannelBuilder.forAddress(anyString(), anyInt()))
      .thenReturn(channel_builder);
    when(channel_builder.compressorRegistry(any(CompressorRegistry.class)))
      .thenReturn(channel_builder);
    when(channel_builder.decompressorRegistry(any(DecompressorRegistry.class)))
      .thenReturn(channel_builder);
    when(channel_builder.build()).thenReturn(channel);
  }
  
  @Test
  public void initialize() throws Exception {
    QueryGRPCClientFactory factory = new QueryGRPCClientFactory();
    assertNull(factory.initialize(TSDB).join(1));
    assertNotNull(factory.stub());
    
    when(channel_builder.build()).thenThrow(new UnitTestException());
    factory = new QueryGRPCClientFactory();
    try {
      factory.initialize(TSDB).join(1);
      fail("Expected UnitTestException");
    } catch (UnitTestException e) { }
  }
  
  @Test
  public void shutdown() throws Exception {
    QueryGRPCClientFactory factory = new QueryGRPCClientFactory();
    assertNull(factory.initialize(TSDB).join(1));
    
    assertNull(factory.shutdown().join(1));
    verify(channel, times(1)).shutdownNow();
  }
  
  @Test
  public void supportsPushDown() throws Exception {
    QueryGRPCClientFactory factory = new QueryGRPCClientFactory();
    assertTrue(factory.supportsPushdown(DownsampleConfig.class));
    assertFalse(factory.supportsPushdown(ExpressionConfig.class));
  }
  
  @Test
  public void newNode() throws Exception {
    QueryGRPCClientFactory factory = new QueryGRPCClientFactory();
    PowerMockito.mockStatic(QueryGRPCClient.class);
    QueryGRPCClient node = mock(QueryGRPCClient.class);
    PowerMockito.whenNew(QueryGRPCClient.class).withAnyArguments()
      .thenReturn(node);
    
    assertSame(node, factory.newNode(mock(QueryPipelineContext.class), 
        "foo", mock(QuerySourceConfig.class)));
  }
}
