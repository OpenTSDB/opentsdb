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
import com.stumbleupon.async.Deferred;

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
public class TestStatusRpc {
  private TSDB tsdb;
  private HBaseClient client;
  private RpcManager.Status rpc;

  @Before
  public void before() throws Exception {
    rpc = new RpcManager.Status();
    tsdb = NettyMocks.getMockedHTTPTSDB();
    client = mock(HBaseClient.class);
    when(tsdb.getClient()).thenReturn(client);
  }

  private String getStatus() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/status");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String json =
      query.response().getContent().toString(Charset.forName("UTF-8"));
    assertNotNull(json);
    return json;
  }

  @Test
  public void printStatus() throws Exception {
    // Initial status is "startup"
    when(tsdb.checkNecessaryTablesAvailability()).
      thenReturn(Deferred.fromResult(TSDB.TableAvailability.NONE));
    assertEquals(getStatus(), "{\"status\":\"startup\"}");

    // Partial availability:
    when(tsdb.checkNecessaryTablesAvailability()).
      thenReturn(Deferred.fromResult(TSDB.TableAvailability.PARTIAL));
    assertEquals(getStatus(), "{\"status\":\"partial\"}");

    // Full availibility:
    when(tsdb.checkNecessaryTablesAvailability()).
      thenReturn(Deferred.fromResult(TSDB.TableAvailability.FULL));
    assertEquals(getStatus(), "{\"status\":\"ok\"}");

    // No availability (after having seen some in the past):
    when(tsdb.checkNecessaryTablesAvailability()).
      thenReturn(Deferred.fromResult(TSDB.TableAvailability.NONE));
    assertEquals(getStatus(), "{\"status\":\"error\"}");

    // After shutdown status is "shutting-down", regardless of availability:
    rpc.shutdown();
    when(tsdb.checkNecessaryTablesAvailability()).
      thenReturn(Deferred.fromResult(TSDB.TableAvailability.NONE));
    assertEquals(getStatus(), "{\"status\":\"shutting-down\"}");
    when(tsdb.checkNecessaryTablesAvailability()).
      thenReturn(Deferred.fromResult(TSDB.TableAvailability.PARTIAL));
    assertEquals(getStatus(), "{\"status\":\"shutting-down\"}");
    when(tsdb.checkNecessaryTablesAvailability()).
      thenReturn(Deferred.fromResult(TSDB.TableAvailability.FULL));
    assertEquals(getStatus(), "{\"status\":\"shutting-down\"}");
  }
}
