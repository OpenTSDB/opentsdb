// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

/**
 * This is a dummy HTTP RPC plugin implementation for unit test purposes.
 * @since 2.1
 */
public class DummyHttpRpcPlugin extends HttpRpcPlugin {
  @Override
  public void initialize(TSDB tsdb) {
    Preconditions.checkNotNull(tsdb);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "2.0.0";
  }

  @Override
  public void collectStats(StatsCollector collector) {
    collector.record("http_rpcplugin.dummy.value", 1);
  }

  @Override
  public String getPath() {
    return "/dummy/test";
  }

  @Override
  public void execute(TSDB tsdb, HttpRpcPluginQuery query) throws IOException {
  }
}
