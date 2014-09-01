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
import static org.junit.Assert.assertTrue;

import org.hbase.async.HBaseClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, Config.class, HBaseClient.class, RpcPluginsManager.class })
public class TestRpcPluginsManager {
  @Test
  public void isHttpRpcPluginPathValid() {
    RpcPluginsManager mgr = new RpcPluginsManager();
    assertTrue(mgr.isHttpRpcPluginPath("/plugin/my/http/plugin"));
    assertTrue(mgr.isHttpRpcPluginPath("plugin/my/http/plugin"));
    assertTrue(mgr.isHttpRpcPluginPath("/plugin/my?hey=hi&howdy=ho"));
    assertTrue(mgr.isHttpRpcPluginPath("plugin/my?hey=hi&howdy=ho"));
  }
  
  @Test
  public void isHttpRpcPluginPathInvalid() {
    RpcPluginsManager mgr = new RpcPluginsManager();
    assertFalse(mgr.isHttpRpcPluginPath("/plugin/"));
    assertFalse(mgr.isHttpRpcPluginPath("plugin/"));
    assertFalse(mgr.isHttpRpcPluginPath("plugin"));
    assertFalse(mgr.isHttpRpcPluginPath("/plugin"));
    assertFalse(mgr.isHttpRpcPluginPath("api/query"));
  }
  
  @Test
  public void validateHttpRpcPluginPathValid() {
    RpcPluginsManager mgr = new RpcPluginsManager();
    mgr.validateHttpRpcPluginPath("/my/test/path");
    mgr.validateHttpRpcPluginPath("my/test/path");
    mgr.validateHttpRpcPluginPath("my/test/path");
    mgr.validateHttpRpcPluginPath("api/query");
  }
  
  @Test
  public void validateHttpRpcPluginPathInvalid() {
    RpcPluginsManager mgr = new RpcPluginsManager();
    try {
      mgr.validateHttpRpcPluginPath("/plugin/my/test");
      assertTrue(false);
    } catch (IllegalArgumentException e) { }
    try {
      mgr.validateHttpRpcPluginPath("plugin/my/test");
      assertTrue(false);
    } catch (IllegalArgumentException e) { }
    try {
      mgr.validateHttpRpcPluginPath("plugin/");
      assertTrue(false);
    } catch (IllegalArgumentException e) { }
    try {
      mgr.validateHttpRpcPluginPath("/plugin/");
      assertTrue(false);
    } catch (IllegalArgumentException e) { }
    try {
      mgr.validateHttpRpcPluginPath("/plugin");
      assertTrue(false);
    } catch (IllegalArgumentException e) { }
    try {
      mgr.validateHttpRpcPluginPath("plugin");
      assertTrue(false);
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void canonicalizePluginPathsValid() throws Exception {
    RpcPluginsManager mgr = new RpcPluginsManager();
    assertEquals(RpcPluginsManager.PLUGIN_BASE_WEBPATH + "/my/test/path",
        mgr.canonicalizePluginPath("/my/test/path"));
    assertEquals(RpcPluginsManager.PLUGIN_BASE_WEBPATH + "/my/test/path", 
        mgr.canonicalizePluginPath("/my/test/path/"));
    assertEquals(RpcPluginsManager.PLUGIN_BASE_WEBPATH + "/my/test/path",
        mgr.canonicalizePluginPath("my/test/path/"));
    assertEquals(RpcPluginsManager.PLUGIN_BASE_WEBPATH + "/my/test/path", 
        mgr.canonicalizePluginPath("my/test/path"));
    
    assertEquals(RpcPluginsManager.PLUGIN_BASE_WEBPATH + "/my", 
        mgr.canonicalizePluginPath("/my/"));
    assertEquals(RpcPluginsManager.PLUGIN_BASE_WEBPATH + "/my", 
        mgr.canonicalizePluginPath("my/"));
    assertEquals(RpcPluginsManager.PLUGIN_BASE_WEBPATH + "/my", 
        mgr.canonicalizePluginPath("my"));
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void canonicalizePluginPathIsRoot() {
    assertEquals(RpcPluginsManager.PLUGIN_BASE_WEBPATH + "/", 
        new RpcPluginsManager().canonicalizePluginPath(""));
  }
  
  @Test
  public void validHttpPathEndToEnd() {
    RpcPluginsManager mgr = new RpcPluginsManager();
    mgr.validateHttpRpcPluginPath("myplugin");
    assertEquals("plugin/myplugin", mgr.canonicalizePluginPath("myplugin"));
    mgr.validateHttpRpcPluginPath("/myplugin");
    assertEquals("plugin/myplugin", mgr.canonicalizePluginPath("/myplugin"));
    
    mgr.validateHttpRpcPluginPath("myplugin/subcommand");
    assertEquals("plugin/myplugin/subcommand", mgr.canonicalizePluginPath("myplugin/subcommand"));
    mgr.validateHttpRpcPluginPath("/myplugin/subcommand");
    assertEquals("plugin/myplugin/subcommand", mgr.canonicalizePluginPath("/myplugin/subcommand"));
  }
}
