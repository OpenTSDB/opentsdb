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

import com.google.common.collect.Maps;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public final class TestRpcPlugin {
  private TSDB tsdb;
  private Config config;
  private RpcPlugin rpc_plugin;
  
  @Before
  public void before() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.rpcplugin.DummyRPCPlugin.hosts", "localhost");
    overrides.put("tsd.rpcplugin.DummyRPCPlugin.port", "42");
    config = new Config(false, overrides);
    tsdb = new TSDB(new MemoryStore(), config);

    PluginLoader.loadJAR("plugin_test.jar");
    rpc_plugin = PluginLoader.loadSpecificPlugin(
        "net.opentsdb.tsd.DummyRpcPlugin", RpcPlugin.class);
  }
  
  @Test
  public void initialize() throws Exception {
    rpc_plugin.initialize(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeMissingHost() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.rpcplugin.DummyRPCPlugin.port", "42");
    config = new Config(false, overrides);
    tsdb = new TSDB(new MemoryStore(), config);

    rpc_plugin.initialize(tsdb);
  }

  @Test (expected = IllegalArgumentException.class)
  public void initializeEmptyHost() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.rpcplugin.DummyRPCPlugin.hosts", "");
    overrides.put("tsd.rpcplugin.DummyRPCPlugin.port", "42");
    config = new Config(false, overrides);
    tsdb = new TSDB(new MemoryStore(), config);

    rpc_plugin.initialize(tsdb);
  }
  
  @Test (expected = NumberFormatException.class)
  public void initializeMissingPort() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.rpcplugin.DummyRPCPlugin.hosts", "localhost");
    config = new Config(false, overrides);
    tsdb = new TSDB(new MemoryStore(), config);

    rpc_plugin.initialize(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeInvalidPort() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.rpcplugin.DummyRPCPlugin.hosts", "localhost");
    overrides.put("tsd.rpcplugin.DummyRPCPlugin.port", "not a number");
    config = new Config(false, overrides);
    tsdb = new TSDB(new MemoryStore(), config);

    rpc_plugin.initialize(tsdb);
  }
  
  @Test
  public void shutdown() throws Exception  {
    assertNotNull(rpc_plugin.shutdown());
  }
  
  @Test
  public void version() throws Exception  {
    assertEquals("2.0.0", rpc_plugin.version());
  }
  
}
