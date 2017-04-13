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
package net.opentsdb.query.execution.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.JavaType;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.reflect.TypeToken;

import net.opentsdb.query.execution.cluster.ClusterConfigPlugin.Config;
import net.opentsdb.utils.JSON;

public class TestClusterConfigResolver {

  protected JavaType type;
  
  @Before
  public void before() throws Exception {
    type = JSON.getMapper().getTypeFactory()
        .constructSimpleType(ClusterConfigPlugin.Config.class, new JavaType[0]);
  }
  
  @Test
  public void init() throws Exception {
    final ClusterConfigResolver resolver = new ClusterConfigResolver();
    resolver.init(type);
    assertEquals("net.opentsdb.query.execution.cluster", 
        ClusterConfigResolver.PACKAGE);
    assertEquals(Id.CUSTOM, resolver.getMechanism());
    assertEquals("ClusterConfigPlugin", resolver.idFromBaseType());
    
    try {
      resolver.init(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void idFromValue() throws Exception {
    final ClusterConfigResolver resolver = new ClusterConfigResolver();
    resolver.init(type);
    
    // simple type, no declaring class.
    assertEquals("String", resolver.idFromValue("Hello!"));
    
    // compound class
    final TestClusterConfigPlugin.UTPlugin.Config config = 
        (TestClusterConfigPlugin.UTPlugin.Config) 
        new TestClusterConfigPlugin.UTPlugin.Config.Builder()
      .setId("Conf")
      .setClusters(Lists.newArrayList(mock(ClusterDescriptor.class)))
      .setImplementation("TestClusterConfigPlugin.UTPlugin")
      .build();
    assertEquals("UTPlugin", resolver.idFromValue(config));
    
    try {
      resolver.idFromValue(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
  }
  
  @Test
  public void idFromValueAndType() throws Exception {
    final ClusterConfigResolver resolver = new ClusterConfigResolver();
    resolver.init(type);
    assertEquals("String", resolver.idFromValueAndType(null, 
        "Hello!".getClass()));
    
    // compound class
    final TestClusterConfigPlugin.UTPlugin.Config config = 
        (TestClusterConfigPlugin.UTPlugin.Config) 
        new TestClusterConfigPlugin.UTPlugin.Config.Builder()
      .setId("Conf")
      .setClusters(Lists.newArrayList(mock(ClusterDescriptor.class)))
      .setImplementation("TestClusterConfigPlugin.UTPlugin")
      .build();
    assertEquals("UTPlugin", resolver.idFromValueAndType(null, 
        config.getClass()));
    
    // switcher-roo
    assertEquals("UTPlugin", resolver.idFromValueAndType(config, 
        "Hello!".getClass()));
    
    try {
      resolver.idFromValueAndType(null, null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
  }
  
  @Test
  public void typeOf() throws Exception {
    TypeToken<?> token = 
        ClusterConfigResolver.typeOf("TestClusterConfigPlugin$UTPlugin");
    assertEquals(TestClusterConfigPlugin.UTPlugin.Config.class, 
        token.getRawType());
    
    token = ClusterConfigResolver.typeOf(
        "net.opentsdb.query.execution.cluster.TestClusterConfigResolver$UTConfig");
    assertEquals(UTConfig.class, token.getRawType());
    
    try {
      ClusterConfigResolver.typeOf("");
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    try {
      ClusterConfigResolver.typeOf("net.opentsdb.nosuchclass");
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    try {
      ClusterConfigResolver.typeOf(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
  }
  
  @Test
  public void typeFromId() throws Exception {
    final ClusterConfigResolver resolver = new ClusterConfigResolver();
    resolver.init(type);
    JavaType type = resolver.typeFromId("TestClusterConfigPlugin$UTPlugin");
    assertEquals(TestClusterConfigPlugin.UTPlugin.Config.class, 
        type.getRawClass());
        
    type = resolver.typeFromId(
        "net.opentsdb.query.execution.cluster.TestClusterConfigResolver$UTConfig");
    assertEquals(UTConfig.class, type.getRawClass());
    
    try {
      resolver.typeFromId("");
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    try {
      resolver.typeFromId(
          "net.opentsdb.query.execution.cluster.TestClusterConfigResolver");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      resolver.typeFromId("net.opentsdb.nosuchclass");
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    try {
      resolver.typeFromId(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
  }
  
  /**
   * Little helper to show we can load the local config.
   */
  public static class UTConfig extends ClusterConfigPlugin.Config {

    protected UTConfig(final Builder builder) {
      super(builder);
    }

    @Override
    public boolean equals(Object o) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public int hashCode() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public HashCode buildHashCode() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int compareTo(Config config) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public String toString() {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
}
