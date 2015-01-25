// This file is part of OpenTSDB.
// Copyright (C) 2010-2014  The OpenTSDB Authors.
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

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpRequest;

import net.opentsdb.core.TSDB;

/**
 * Query class for {@link HttpRpcPlugin}s.  Binds together a request, its 
 * owning channel, and reponse helpers.
 * 
 * @since 2.2
 */
public final class HttpRpcPluginQuery extends AbstractHttpQuery {
  public HttpRpcPluginQuery(final TSDB tsdb, final HttpRequest request, final Channel chan) {
    super(tsdb, request, chan);
  }

  /**
   * Return the base route with no plugin prefix in it.  This is matched with
   * values returned by {@link HttpRpcPlugin#getPath()}.
   * @return the base route path (no query parameters, etc.)
   */
  @Override
  public String getQueryBaseRoute() {
    final String[] parts = explodePath();
    if (parts.length < 2) { // Must be at least something like: /plugin/blah
      throw new BadRequestException("Invalid plugin request path: " + getQueryPath());
    }
    // Lop off the first element (which is the "plugin" base path).
    // The remaining elements are the base route.
    final StringBuilder joined = new StringBuilder();
    for (int i=1; i<parts.length; i++) {
      if (i != 1) {
        joined.append('/');
      }
      joined.append(parts[i]);
    }
    return joined.toString();
  }
}
