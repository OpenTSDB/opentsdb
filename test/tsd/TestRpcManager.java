// This file is part of OpenTSDB.
// Copyright (C) 2014 The OpenTSDB Authors.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import org.hbase.async.HBaseClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, Config.class, HBaseClient.class, RpcManager.class })
public class TestRpcManager {
  private TSDB mock_tsdb_no_plugins;
  
  // Set in individual test methods; shutdown by after() if set.
  private RpcManager mgr_under_test;
  
  @Before
  public void before() {
    Config config = mock(Config.class);
    TSDB tsdb = mock(TSDB.class);
    when(tsdb.getConfig()).thenReturn(config);
    mock_tsdb_no_plugins = tsdb;
  }
  
  @After
  public void after() throws Exception {
    if (mgr_under_test != null) {
      mgr_under_test.shutdown().join();
    }
  }
  
  @Test
  public void loadHttpRpcPlugins() throws Exception {
    Config config = mock(Config.class);
    when(config.hasProperty("tsd.http.rpc.plugins"))
    .thenReturn(true);
    when(config.getString("tsd.http.rpc.plugins"))
      .thenReturn("net.opentsdb.tsd.DummyHttpRpcPlugin");
    
    TSDB tsdb = mock(TSDB.class);
    when(tsdb.getConfig()).thenReturn(config);
    
    PluginLoader.loadJAR("plugin_test.jar");
    mgr_under_test = RpcManager.instance(tsdb);
    
    HttpRpcPlugin plugin = mgr_under_test.lookupHttpRpcPlugin("dummy/test");
    assertNotNull(plugin);
  }
  
  @Test
  public void loadRpcPlugin() throws Exception {
    Config config = mock(Config.class);
    when(config.hasProperty("tsd.rpc.plugins"))
    .thenReturn(true);
    when(config.getString("tsd.rpc.plugins"))
      .thenReturn("net.opentsdb.tsd.DummyRpcPlugin");
    
    when(config.hasProperty("tsd.rpcplugin.DummyRPCPlugin.hosts"))
    .thenReturn(true);
    when(config.getString("tsd.rpcplugin.DummyRPCPlugin.hosts"))
      .thenReturn("blah");
    when(config.getInt("tsd.rpcplugin.DummyRPCPlugin.port"))
      .thenReturn(1000);
    
    TSDB tsdb = mock(TSDB.class);
    when(tsdb.getConfig()).thenReturn(config);
    
    PluginLoader.loadJAR("plugin_test.jar");
    mgr_under_test= RpcManager.instance(tsdb);
    
    assertFalse(mgr_under_test.getRpcPlugins().isEmpty());
  }
  
  @Test
  public void isHttpRpcPluginPathValid() {
    mgr_under_test = RpcManager.instance(mock_tsdb_no_plugins);
    assertTrue(mgr_under_test.isHttpRpcPluginPath("/plugin/my/http/plugin"));
    assertTrue(mgr_under_test.isHttpRpcPluginPath("plugin/my/http/plugin"));
    assertTrue(mgr_under_test.isHttpRpcPluginPath("/plugin/my?hey=hi&howdy=ho"));
    assertTrue(mgr_under_test.isHttpRpcPluginPath("plugin/my?hey=hi&howdy=ho"));
    assertTrue(mgr_under_test.isHttpRpcPluginPath("plugin/my/?hey=hi&howdy=ho"));
  }
  
  @Test
  public void isHttpRpcPluginPathInvalid() {
    mgr_under_test = RpcManager.instance(mock_tsdb_no_plugins);
    assertFalse(mgr_under_test.isHttpRpcPluginPath("/plugin/"));
    assertFalse(mgr_under_test.isHttpRpcPluginPath("plugin/"));
    assertFalse(mgr_under_test.isHttpRpcPluginPath("plugin"));
    assertFalse(mgr_under_test.isHttpRpcPluginPath("/plugin"));
    assertFalse(mgr_under_test.isHttpRpcPluginPath("/plugin?howdy=ho"));
    assertFalse(mgr_under_test.isHttpRpcPluginPath("/plugin/?howdy=ho"));
    assertFalse(mgr_under_test.isHttpRpcPluginPath("api/query"));
  }
  
  @Test
  public void validateHttpRpcPluginPathValid() {
    mgr_under_test = RpcManager.instance(mock_tsdb_no_plugins);
    mgr_under_test.validateHttpRpcPluginPath("/my/test/path");
    mgr_under_test.validateHttpRpcPluginPath("my/test/path");
    mgr_under_test.validateHttpRpcPluginPath("my/test/path");
    mgr_under_test.validateHttpRpcPluginPath("api/query");
  }
  
  @Test
  public void validateHttpRpcPluginPathInvalid() {
    mgr_under_test = RpcManager.instance(mock_tsdb_no_plugins);
    try {
      mgr_under_test.validateHttpRpcPluginPath("/plugin/my/test");
      assertTrue(false);
    } catch (IllegalArgumentException e) { }
    try {
      mgr_under_test.validateHttpRpcPluginPath("plugin/my/test");
      assertTrue(false);
    } catch (IllegalArgumentException e) { }
    try {
      mgr_under_test.validateHttpRpcPluginPath("plugin/");
      assertTrue(false);
    } catch (IllegalArgumentException e) { }
    try {
      mgr_under_test.validateHttpRpcPluginPath("/plugin/");
      assertTrue(false);
    } catch (IllegalArgumentException e) { }
    try {
      mgr_under_test.validateHttpRpcPluginPath("/plugin");
      assertTrue(false);
    } catch (IllegalArgumentException e) { }
    try {
      mgr_under_test.validateHttpRpcPluginPath("plugin");
      assertTrue(false);
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void canonicalizePluginPathsValid() throws Exception {
    mgr_under_test = RpcManager.instance(mock_tsdb_no_plugins);
    assertEquals("my/test/path",
        mgr_under_test.canonicalizePluginPath("/my/test/path"));
    assertEquals("my/test/path", 
        mgr_under_test.canonicalizePluginPath("/my/test/path/"));
    assertEquals("my/test/path",
        mgr_under_test.canonicalizePluginPath("my/test/path/"));
    assertEquals("my/test/path", 
        mgr_under_test.canonicalizePluginPath("my/test/path"));
    
    assertEquals("my", 
        mgr_under_test.canonicalizePluginPath("/my/"));
    assertEquals("my", 
        mgr_under_test.canonicalizePluginPath("my/"));
    assertEquals("my", 
        mgr_under_test.canonicalizePluginPath("my"));
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void canonicalizePluginPathIsRoot() {
    mgr_under_test = RpcManager.instance(mock_tsdb_no_plugins);
    assertEquals(RpcManager.PLUGIN_BASE_WEBPATH + "/", 
        mgr_under_test.canonicalizePluginPath(""));
  }
  
  @Test
  public void validHttpPathEndToEnd() {
    mgr_under_test = RpcManager.instance(mock_tsdb_no_plugins);
    mgr_under_test.validateHttpRpcPluginPath("myplugin");
    assertEquals("myplugin", mgr_under_test.canonicalizePluginPath("myplugin"));
    mgr_under_test.validateHttpRpcPluginPath("/myplugin");
    assertEquals("myplugin", mgr_under_test.canonicalizePluginPath("/myplugin"));
    
    mgr_under_test.validateHttpRpcPluginPath("myplugin/subcommand");
    assertEquals("myplugin/subcommand", mgr_under_test.canonicalizePluginPath("myplugin/subcommand"));
    mgr_under_test.validateHttpRpcPluginPath("/myplugin/subcommand");
    assertEquals("myplugin/subcommand", mgr_under_test.canonicalizePluginPath("/myplugin/subcommand"));
  }
}
