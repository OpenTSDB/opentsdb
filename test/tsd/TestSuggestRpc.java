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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import net.opentsdb.core.TSDB;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class})
public final class TestSuggestRpc {
  private TSDB tsdb = null;
  private SuggestRpc s = null;
  
  @Before
  public void before() {
    s = new SuggestRpc();
    tsdb = NettyMocks.getMockedHTTPTSDB();
    final List<String> metrics = new ArrayList<String>();
    metrics.add("sys.cpu.0.system"); 
    when(tsdb.suggestMetrics("s")).thenReturn(metrics);
    final List<String> tagks = new ArrayList<String>();
    tagks.add("host");
    when(tsdb.suggestTagNames("h")).thenReturn(tagks);
    final List<String> tagvs = new ArrayList<String>();
    tagvs.add("web01.mysite.com");
    when(tsdb.suggestTagValues("w")).thenReturn(tagvs);
  }
  
  @Test
  public void metricsQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?type=metrics&q=s");
    s.execute(tsdb, query);
    assertEquals("[\"sys.cpu.0.system\"]", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void metricsPOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "{\"type\":\"metrics\",\"q\":\"s\"}", "application/json");
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
    s.execute(tsdb, new HttpQuery(tsdb, req, channelMock));
  }
  
  @Test (expected = BadRequestException.class)
  public void missingType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?q=h");
    s.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void missingQ() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/suggest?type=metrics");
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
  public void missingQPOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/suggest", 
        "{\"type\":\"metrics\"}", "application/json");
    query.getQueryBaseRoute();
    s.execute(tsdb, query);
  }
}
