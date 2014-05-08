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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.net.HttpHeaders;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, Config.class, HBaseClient.class, RpcHandler.class, 
  HttpQuery.class, MessageEvent.class, DefaultHttpResponse.class, 
  ChannelHandlerContext.class })
public final class TestRpcHandler {
  private TSDB tsdb = null;
  private ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
  private HBaseClient client = mock(HBaseClient.class);
  private MessageEvent message = mock(MessageEvent.class);
  
  @Before
  public void before() throws Exception {
    final Config config = new Config(false);
    PowerMockito.whenNew(HBaseClient.class)
      .withArguments(anyString(), anyString()).thenReturn(client);
    tsdb = new TSDB(config);
  }
  
  @Test
  public void ctorDefaults() {
    final RpcHandler rpc = new RpcHandler(tsdb);
    assertNotNull(rpc);
  }
  
  @Test
  public void ctorCORSPublic() {
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", "*");
    final RpcHandler rpc = new RpcHandler(tsdb);
    assertNotNull(rpc);
  }
  
  @Test
  public void ctorCORSSeparated() {
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", 
        "aurther.com,dent.net,beeblebrox.org");
    final RpcHandler rpc = new RpcHandler(tsdb);
    assertNotNull(rpc);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorCORSPublicAndDomains() {
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", 
        "*,aurther.com,dent.net,beeblebrox.org");
    new RpcHandler(tsdb);
  }
  
  @Test
  public void httpCORSIgnored() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");

    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.OK, response.getStatus());
          assertNull(response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    final RpcHandler rpc = new RpcHandler(tsdb);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpCORSPublicSimple() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");

    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.OK, response.getStatus());
          assertEquals("42.com", 
              response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", "*");
    final RpcHandler rpc = new RpcHandler(tsdb);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpCORSSpecificSimple() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");

    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.OK, response.getStatus());
          assertEquals("42.com", 
              response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", 
        "aurther.com,dent.net,42.com,beeblebrox.org");
    final RpcHandler rpc = new RpcHandler(tsdb);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpCORSNotAllowedSimple() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");

    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.OK, response.getStatus());
          assertNull(response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", 
        "aurther.com,dent.net,beeblebrox.org");
    final RpcHandler rpc = new RpcHandler(tsdb);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpOptionsNoCORS() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");

    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.METHOD_NOT_ALLOWED, response.getStatus());
          assertNull(response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    final RpcHandler rpc = new RpcHandler(tsdb);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpOptionsCORSNotConfigured() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");
    
    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.METHOD_NOT_ALLOWED, response.getStatus());
          assertNull(response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    final RpcHandler rpc = new RpcHandler(tsdb);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpOptionsCORSPublic() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");
    
    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.OK, response.getStatus());
          assertEquals("42.com", 
              response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", "*");
    final RpcHandler rpc = new RpcHandler(tsdb);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpOptionsCORSSpecific() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");
    
    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.OK, response.getStatus());
          assertEquals("42.com", 
              response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", 
      "aurther.com,dent.net,42.com,beeblebrox.org");
    final RpcHandler rpc = new RpcHandler(tsdb);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpOptionsCORSNotAllowed() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");
    
    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        public ChannelFuture answer(final InvocationOnMock args) 
          throws Throwable {
          DefaultHttpResponse response = 
            (DefaultHttpResponse)args.getArguments()[0];
          assertEquals(HttpResponseStatus.OK, response.getStatus());
          assertNull(response.headers().get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
          return null;
        }        
      }
    );
    
    tsdb.getConfig().overrideConfig("tsd.http.request.cors_domains", 
      "aurther.com,dent.net,beeblebrox.org");
    final RpcHandler rpc = new RpcHandler(tsdb);
    rpc.messageReceived(ctx, message);
  }
  
  private void handleHttpRpc(final HttpRequest req, final Answer<?> answer) {
    final Channel channel = NettyMocks.fakeChannel();
    when(message.getMessage()).thenReturn(req);
    when(message.getChannel()).thenReturn(channel);
    when(channel.write((DefaultHttpResponse)any())).thenAnswer(answer);
  }
}
