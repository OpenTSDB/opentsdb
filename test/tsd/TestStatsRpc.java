// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HttpJsonSerializer.class, TSDB.class, Config.class, 
  HttpQuery.class, Thread.class, HBaseClient.class })
public class TestStatsRpc {
  private TSDB tsdb;
  private HBaseClient client;
  
  @Before
  public void before() throws Exception {
    tsdb = NettyMocks.getMockedHTTPTSDB();
    client = mock(HBaseClient.class);
    when(tsdb.getClient()).thenReturn(client);
  }

  @Test
  public void statsWithOutPort() throws Exception {
    final StatsRpc rpc = new StatsRpc();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/stats");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String json = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertFalse(json.contains("port=4242"));
  }
  
  @Test
  public void statsWithPort() throws Exception {
    when(tsdb.getConfig().getBoolean("tsd.core.stats_with_port"))
      .thenReturn(true);
    when(tsdb.getConfig().getString("tsd.network.port"))
      .thenReturn("4242");
    StatsCollector.setGlobalTags(tsdb.getConfig());
    final StatsRpc rpc = new StatsRpc();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/stats");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String json = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("port=4242"));
  }
  
  @Test
  public void printThreadStats() throws Exception {
    final StatsRpc rpc = new StatsRpc();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/stats/threads");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String json = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertNotNull(json);
    // check for some standard JVM threads since we can't mock Thread easily
    assertTrue(json.contains("\"name\":\"Finalizer\""));
    assertTrue(json.contains("java.lang.ref.Finalizer$FinalizerThread.run"));
  }
  
  @Test
  public void printJVMStats() throws Exception {
    final StatsRpc rpc = new StatsRpc();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/stats/jvm");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String json = 
        query.response().getContent().toString(Charset.forName("UTF-8"));
    assertNotNull(json);
    assertTrue(json.contains("\"os\":{"));
    assertTrue(json.contains("\"gc\":{"));
    assertTrue(json.contains("\"runtime\":{"));
    assertTrue(json.contains("\"pools\":{"));
    assertTrue(json.contains("\"memory\":{"));
  }
}

