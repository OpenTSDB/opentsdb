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
import static org.mockito.Mockito.when;

import com.typesafe.config.ConfigValueFactory;
import dagger.ObjectGraph;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.core.TSDB;
import com.typesafe.config.Config;

import com.codahale.metrics.MetricRegistry;
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
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.net.HttpHeaders;

import javax.inject.Inject;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, Config.class, HBaseClient.class, RpcHandler.class,
  HttpQuery.class, MessageEvent.class, DefaultHttpResponse.class, 
  ChannelHandlerContext.class })
public final class TestRpcHandler {
  private ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
  private MessageEvent message = mock(MessageEvent.class);
  private TsdStats tsdStats;

  @Inject Config config;
  @Inject TSDB tsdb;
  @Inject MetricRegistry metricRegistry;

  @Before
  public void before() throws Exception {
    ObjectGraph.create(new TestModuleMemoryStore()).inject(this);
    tsdStats = new TsdStats(metricRegistry);
  }
  
  @Test
  public void ctorDefaults() {
    assertNotNull(new RpcHandler(tsdb, metricRegistry, tsdStats));
  }
  
  @Test
  public void ctorCORSPublic() {
    config = config.withValue("tsd.http.request.cors_domains",
            ConfigValueFactory.fromAnyRef("*"));

    ObjectGraph.create(new TestModuleMemoryStore(config)).inject(this);

    assertNotNull(new RpcHandler(tsdb, metricRegistry, tsdStats));
  }
  
  @Test
  public void ctorCORSSeparated() {
    config = config.withValue("tsd.http.request.cors_domains",
            ConfigValueFactory.fromAnyRef("aurther.com,dent.net,beeblebrox.org"));

    ObjectGraph.create(new TestModuleMemoryStore(config)).inject(this);

    assertNotNull(new RpcHandler(tsdb, metricRegistry, tsdStats));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorCORSPublicAndDomains() {
    config = config.withValue("tsd.http.request.cors_domains",
            ConfigValueFactory.fromAnyRef("*,aurther.com,dent.net,beeblebrox.org"));

    ObjectGraph.create(new TestModuleMemoryStore(config)).inject(this);

    new RpcHandler(tsdb, metricRegistry, tsdStats);
  }
  
  @Test
  public void httpCORSIgnored() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");

    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        @Override
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
    
    final RpcHandler rpc = new RpcHandler(tsdb, metricRegistry, tsdStats);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpCORSPublicSimple() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");

    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        @Override
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

    config = config.withValue("tsd.http.request.cors_domains",
            ConfigValueFactory.fromAnyRef("*"));

    ObjectGraph.create(new TestModuleMemoryStore(config)).inject(this);

    final RpcHandler rpc = new RpcHandler(tsdb, metricRegistry, tsdStats);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpCORSSpecificSimple() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");

    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        @Override
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

    config = config.withValue("tsd.http.request.cors_domains",
            ConfigValueFactory.fromAnyRef("aurther.com,dent.net,42.com,beeblebrox.org"));

    ObjectGraph.create(new TestModuleMemoryStore(config)).inject(this);

    final RpcHandler rpc = new RpcHandler(tsdb, metricRegistry, tsdStats);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpCORSNotAllowedSimple() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");

    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        @Override
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

    config = config.withValue("tsd.http.request.cors_domains",
            ConfigValueFactory.fromAnyRef("aurther.com,dent.net,beeblebrox.org"));

    ObjectGraph.create(new TestModuleMemoryStore(config)).inject(this);

    final RpcHandler rpc = new RpcHandler(tsdb, metricRegistry, tsdStats);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpOptionsNoCORS() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");

    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        @Override
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
    
    final RpcHandler rpc = new RpcHandler(tsdb, metricRegistry, tsdStats);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpOptionsCORSNotConfigured() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");
    
    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        @Override
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
    
    final RpcHandler rpc = new RpcHandler(tsdb, metricRegistry, tsdStats);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpOptionsCORSPublic() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");
    
    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        @Override
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

    config = config.withValue("tsd.http.request.cors_domains",
            ConfigValueFactory.fromAnyRef("*"));

    ObjectGraph.create(new TestModuleMemoryStore(config)).inject(this);

    final RpcHandler rpc = new RpcHandler(tsdb, metricRegistry, tsdStats);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpOptionsCORSSpecific() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");
    
    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        @Override
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

    config = config.withValue("tsd.http.request.cors_domains",
            ConfigValueFactory.fromAnyRef("aurther.com,dent.net,42.com,beeblebrox.org"));

    ObjectGraph.create(new TestModuleMemoryStore(config)).inject(this);

    final RpcHandler rpc = new RpcHandler(tsdb, metricRegistry, tsdStats);
    rpc.messageReceived(ctx, message);
  }
  
  @Test
  public void httpOptionsCORSNotAllowed() {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.OPTIONS, "/api/v1/version");
    req.headers().add(HttpHeaders.ORIGIN, "42.com");
    
    handleHttpRpc(req,
      new Answer<ChannelFuture>() {
        @Override
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

    config = config.withValue("tsd.http.request.cors_domains",
            ConfigValueFactory.fromAnyRef("aurther.com,dent.net,beeblebrox.org"));

    ObjectGraph.create(new TestModuleMemoryStore(config)).inject(this);

    final RpcHandler rpc = new RpcHandler(tsdb, metricRegistry, tsdStats);
    rpc.messageReceived(ctx, message);
  }
  
  private void handleHttpRpc(final HttpRequest req, final Answer<?> answer) {
    final Channel channel = NettyMocks.fakeChannel();
    when(message.getMessage()).thenReturn(req);
    when(message.getChannel()).thenReturn(channel);
    when(channel.write((DefaultHttpResponse)any())).thenAnswer(answer);
  }
}
