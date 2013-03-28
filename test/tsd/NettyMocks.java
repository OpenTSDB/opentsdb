// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
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

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.junit.Ignore;

/**
 * Helper class that provides mockups for testing any OpenTSDB processes that
 * deal with Netty.
 */
@Ignore
public final class NettyMocks {

  /**
   * Returns a mocked Channel object that simply sets the name to
   * [fake channel]
   * @return A Channel mock
   */
  public static Channel fakeChannel() {
    final Channel chan = mock(Channel.class);
    when(chan.toString()).thenReturn("[fake channel]");
    return chan;
  }
  
  /**
   * Returns a simple pipeline with an HttpRequestDecoder and an 
   * HttpResponseEncoder. No mocking, returns an actual pipeline
   * @return The pipeline
   */
  private DefaultChannelPipeline createHttpPipeline() {
    DefaultChannelPipeline pipeline = new DefaultChannelPipeline();
    pipeline.addLast("requestDecoder", new HttpRequestDecoder());
    pipeline.addLast("responseEncoder", new HttpResponseEncoder());
    return pipeline;
  }
}
