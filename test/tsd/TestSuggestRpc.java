// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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

import com.codahale.metrics.MetricRegistry;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TsdbBuilder;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;

public final class TestSuggestRpc {
  private TSDB tsdb = null;
  private SuggestRpc s = null;
  
  @Before
  public void before() throws IOException {
    s = new SuggestRpc();

    final Config config = new Config(false);
    final TsdbStore store = new MemoryStore();

    tsdb = TsdbBuilder.createFromConfig(config)
            .withStore(store)
            .build();

    store.allocateUID("sys.cpu.0.system", UniqueIdType.METRIC);
    store.allocateUID("sys.mem.free", UniqueIdType.METRIC);
    store.allocateUID("host", UniqueIdType.TAGK);
    store.allocateUID("web01.mysite.com", UniqueIdType.TAGV);
  }
  
  @Test
  public void metricsQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?type=metrics&q=s");
    s.execute(tsdb, query);
    assertEquals("[\"sys.cpu.0.system\",\"sys.mem.free\"]", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void metricsPOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "{\"type\":\"metrics\",\"q\":\"s\"}", "application/json");
    query.getQueryBaseRoute();
    s.execute(tsdb, query);
    assertEquals("[\"sys.cpu.0.system\",\"sys.mem.free\"]", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void metricQSMax() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?type=metrics&q=s&max=1");
    s.execute(tsdb, query);
    assertEquals("[\"sys.cpu.0.system\"]", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void metricsPOSTMax() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "{\"type\":\"metrics\",\"q\":\"s\",\"max\":1}", "application/json");
    query.getQueryBaseRoute();
    s.execute(tsdb, query);
    assertEquals("[\"sys.cpu.0.system\"]", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void tagkQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?type=tagk&q=h");
    s.execute(tsdb, query);
    assertEquals("[\"host\"]", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void tagkPOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "{\"type\":\"tagk\",\"q\":\"h\"}", "application/json");
    query.getQueryBaseRoute();
    s.execute(tsdb, query);
    assertEquals("[\"host\"]", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void tagvQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?type=tagv&q=w");
    s.execute(tsdb, query);
    assertEquals("[\"web01.mysite.com\"]", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void tagvPOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "{\"type\":\"tagv\",\"q\":\"w\"}", "application/json");
    query.getQueryBaseRoute();
    s.execute(tsdb, query);
    assertEquals("[\"web01.mysite.com\"]", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test (expected = BadRequestException.class)
  public void badMethod() throws Exception {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/suggest?type=metrics&q=h");
    req.setMethod(HttpMethod.PUT);
    s.execute(tsdb, new HttpQuery(tsdb, req, channelMock, new TsdStats(new
            MetricRegistry())));
  }
  
  @Test (expected = BadRequestException.class)
  public void missingType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?q=h");
    s.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void missingContent() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "", "application/json");
    query.getQueryBaseRoute();
    s.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void badType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?type=doesnotexist&q=h");
    s.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void missingTypePOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "{\"q\":\"w\"}", "application/json");
    query.getQueryBaseRoute();
    s.execute(tsdb, query);
  }

  @Test (expected = BadRequestException.class)
  public void badMaxQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?type=tagv&q=w&max=foo");
    s.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void badMaxPOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "{\"type\":\"metrics\",\"q\":\"s\",\"max\":\"foo\"}", 
        "application/json");
    query.getQueryBaseRoute();
    s.execute(tsdb, query);
  }
}
