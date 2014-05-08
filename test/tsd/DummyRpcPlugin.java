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

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

import org.junit.Ignore;

import com.stumbleupon.async.Deferred;

/**
 * This is a dummy RPC plugin implementation for unit test purposes
 * @since 2.0
 */
@Ignore
public class DummyRpcPlugin extends RpcPlugin {

  @Override
  public void initialize(TSDB tsdb) {
    if (tsdb == null) {
      throw new IllegalArgumentException("The TSDB object was null");
    }
    // some dummy configs to check to throw exceptions
    if (!tsdb.getConfig().hasProperty("tsd.rpcplugin.DummyRPCPlugin.hosts")) {
      throw new IllegalArgumentException("Missing hosts config");
    }
    if (tsdb.getConfig().getString("tsd.rpcplugin.DummyRPCPlugin.hosts")
        .isEmpty()) {
      throw new IllegalArgumentException("Empty Hosts config");
    }
    // throw an NFE for fun
    tsdb.getConfig().getInt("tsd.rpcplugin.DummyRPCPlugin.port");
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
    collector.record("rpcplugin.dummy.writes", 1);
  }

}
