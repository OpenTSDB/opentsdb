// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.context;

import java.util.Map;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timer;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.DataMerger;
import net.opentsdb.query.execution.HttpEndpoints;

/**
 * TODO - stub
 *
 * @since 3.0
 */
public class HttpContextFactory {

  private HttpEndpoints endpoints;
  
  private Map<TypeToken<?>, DataMerger<?>> mergers;
  
  public HttpContextFactory() {
  }
  
  public Deferred<Object> initialize(final TSDB tsdb, final Timer timer) {
    endpoints = new HttpEndpoints(tsdb.getConfig(), timer);
    mergers = tsdb.getRegistry().dataMergers();
    return Deferred.fromResult(null);
  }
  
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }
  
  public RemoteContext getContext(final QueryContext context, 
      final Map<String, String> headers) {
    // TODO headers
    return new HttpContext(context, endpoints, mergers, headers);
  }
}
