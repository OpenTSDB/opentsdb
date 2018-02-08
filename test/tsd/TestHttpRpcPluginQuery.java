// This file is part of OpenTSDB.
// Copyright (C) 2011-2014  The OpenTSDB Authors.
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
import static org.mockito.Mockito.mock;

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

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, HttpRpcPluginQuery.class})
public final class TestHttpRpcPluginQuery {
  private TSDB mockTsdb;
  private Channel mockChannel;
  
  @Before
  public void before() {
    mockTsdb = mock(TSDB.class);
    mockChannel = NettyMocks.fakeChannel();
  }
  
  @Test
  public void getQueryBaseRoute() {
    assertEquals("test", makeQuery("/plugin/test/this/path").getQueryBaseRoute());
    assertEquals("test", makeQuery("/plugin/test/").getQueryBaseRoute());
    assertEquals("test", makeQuery("/plugin/test?some=else&this=that").getQueryBaseRoute());
  }
  
  @Test(expected=BadRequestException.class)
  public void getQueryBaseRouteNoSlash() {
    makeQuery("plugin/test?some=else&this=that").getQueryBaseRoute();
  }
  
  @Test(expected=BadRequestException.class)
  public void getQueryBaseRouteNoPluginBase() {
    makeQuery("/test?some=else&this=that").getQueryBaseRoute();
  }
  
  @Test(expected=BadRequestException.class)
  public void getQueryBaseRouteNoPath() {
    makeQuery("/plugin?some=else&this=that").getQueryBaseRoute();
  }
  
  private HttpRpcPluginQuery makeQuery(final String uriString) {
    HttpRequest req = new DefaultHttpRequest(
        HttpVersion.HTTP_1_1, 
        HttpMethod.GET, 
        uriString);
    return new HttpRpcPluginQuery(mockTsdb, req, mockChannel);
  }
}