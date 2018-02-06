// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.io.Files;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.common.Const;
import net.opentsdb.utils.Config;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HttpEndpoints.class, File.class, Files.class })
public class TestHttpEndpoints {

  private Config config;
  private HashedWheelTimer timer;
  private File file;
  
  @Before
  public void before() throws Exception {
    config = new Config(false);
    timer = mock(HashedWheelTimer.class);
    
    config.overrideConfig("tsd.query.http.endpoints.config", "test.json");
    
    PowerMockito.mockStatic(Files.class);
    file = mock(File.class);
    PowerMockito.whenNew(File.class).withAnyArguments().thenReturn(file);
  }

  @Test
  public void ctor() throws Exception {
    final HttpEndpoints endpoints = new HttpEndpoints(config, timer);
    
    verify(file, times(1)).exists();
    verify(timer, times(1)).newTimeout(endpoints, 
        HttpEndpoints.DEFAULT_LOAD_INTERVAL, TimeUnit.MILLISECONDS);
    PowerMockito.verifyStatic(never());
    Files.toString(file, Const.UTF8_CHARSET);
    assertEquals(0, endpoints.getEndpoints().size());
  }
  
  @Test
  public void ctorOverrideLoadInterval() throws Exception {
    config.overrideConfig("tsd.query.http.endpoints.load_interval", "42");
    final HttpEndpoints endpoints = new HttpEndpoints(config, timer);
    
    verify(file, times(1)).exists();
    verify(timer, times(1)).newTimeout(endpoints, 42L, TimeUnit.MILLISECONDS);
    PowerMockito.verifyStatic(never());
    Files.toString(file, Const.UTF8_CHARSET);
    assertEquals(0, endpoints.getEndpoints().size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNoPath() throws Exception {
    config.overrideConfig("tsd.query.http.endpoints.config", null);
    new HttpEndpoints(config, timer);
  }

  @Test
  public void loadFile() throws Exception {
    setFile(null);
    final HttpEndpoints endpoints = new HttpEndpoints(config, timer);
    
    verify(file, times(1)).exists();
    verify(timer, times(1)).newTimeout(endpoints, 
        HttpEndpoints.DEFAULT_LOAD_INTERVAL, TimeUnit.MILLISECONDS);
    PowerMockito.verifyStatic(times(1));
    Files.toString(file, Const.UTF8_CHARSET);
    assertEquals(4, endpoints.getEndpoints().size());
    assertEquals("host1", endpoints.getEndpoints()
        .get(HttpEndpoints.DEFAULT_KEY).get(0));
    assertEquals("host2", endpoints.getEndpoints()
        .get(HttpEndpoints.DEFAULT_KEY).get(1));
    assertEquals("host3", endpoints.getEndpoints().get("cluster1").get(0));
    assertEquals("host4", endpoints.getEndpoints().get("cluster1").get(1));
    assertEquals("host5", endpoints.getEndpoints().get("cluster2").get(0));
    assertEquals(0, endpoints.getEndpoints().get("cluster3").size());
  }
  
  @Test
  public void loadFileSameTwice() throws Exception {
    setFile(null);
    final HttpEndpoints endpoints = new HttpEndpoints(config, timer);
    int last_hash = endpoints.getLastHash();
    endpoints.run(null);
    
    verify(file, times(2)).exists();
    verify(timer, times(2)).newTimeout(endpoints, 
        HttpEndpoints.DEFAULT_LOAD_INTERVAL, TimeUnit.MILLISECONDS);
    PowerMockito.verifyStatic(times(2));
    Files.toString(file, Const.UTF8_CHARSET);
    assertEquals(4, endpoints.getEndpoints().size());
    assertEquals("host1", endpoints.getEndpoints()
        .get(HttpEndpoints.DEFAULT_KEY).get(0));
    assertEquals("host2", endpoints.getEndpoints()
        .get(HttpEndpoints.DEFAULT_KEY).get(1));
    assertEquals("host3", endpoints.getEndpoints().get("cluster1").get(0));
    assertEquals("host4", endpoints.getEndpoints().get("cluster1").get(1));
    assertEquals("host5", endpoints.getEndpoints().get("cluster2").get(0));
    assertEquals(0, endpoints.getEndpoints().get("cluster3").size());
    assertEquals(last_hash, endpoints.getLastHash());
  }
  
  @Test
  public void loadFileDifferent() throws Exception {
    setFile(null);
    final HttpEndpoints endpoints = new HttpEndpoints(config, timer);
    int last_hash = endpoints.getLastHash();
    
    setFile("{\"" + HttpEndpoints.DEFAULT_KEY + "\":[\"host1\",\"host2\"]}");
    endpoints.run(null);
    
    verify(file, times(2)).exists();
    verify(timer, times(2)).newTimeout(endpoints, 
        HttpEndpoints.DEFAULT_LOAD_INTERVAL, TimeUnit.MILLISECONDS);
    PowerMockito.verifyStatic(times(2));
    Files.toString(file, Const.UTF8_CHARSET);
    assertEquals(1, endpoints.getEndpoints().size());
    assertEquals("host1", endpoints.getEndpoints()
        .get(HttpEndpoints.DEFAULT_KEY).get(0));
    assertEquals("host2", endpoints.getEndpoints()
        .get(HttpEndpoints.DEFAULT_KEY).get(1));
    assertFalse(endpoints.getLastHash() == last_hash);
  }
  
  @Test
  public void loadFileBadJson() throws Exception {
    setFile(null);
    final HttpEndpoints endpoints = new HttpEndpoints(config, timer);
    int last_hash = endpoints.getLastHash();
    
    setFile("{\"" + HttpEndpoints.DEFAULT_KEY + "\":[\"host1\"");
    endpoints.run(null);
    
    verify(file, times(2)).exists();
    verify(timer, times(2)).newTimeout(endpoints, 
        HttpEndpoints.DEFAULT_LOAD_INTERVAL, TimeUnit.MILLISECONDS);
    PowerMockito.verifyStatic(times(2));
    Files.toString(file, Const.UTF8_CHARSET);
    assertEquals(4, endpoints.getEndpoints().size());
    assertEquals("host1", endpoints.getEndpoints()
        .get(HttpEndpoints.DEFAULT_KEY).get(0));
    assertEquals("host2", endpoints.getEndpoints()
        .get(HttpEndpoints.DEFAULT_KEY).get(1));
    assertEquals("host3", endpoints.getEndpoints().get("cluster1").get(0));
    assertEquals("host4", endpoints.getEndpoints().get("cluster1").get(1));
    assertEquals("host5", endpoints.getEndpoints().get("cluster2").get(0));
    assertEquals(0, endpoints.getEndpoints().get("cluster3").size());
    assertEquals(last_hash, endpoints.getLastHash());
  }
  
  @Test
  public void loadFileExceptionOnExists() throws Exception {
    when(file.exists()).thenThrow(new RuntimeException("Boo!"));
    final HttpEndpoints endpoints = new HttpEndpoints(config, timer);
    
    verify(file, times(1)).exists();
    verify(timer, times(1)).newTimeout(endpoints, 
        HttpEndpoints.DEFAULT_LOAD_INTERVAL, TimeUnit.MILLISECONDS);
    PowerMockito.verifyStatic(never());
    Files.toString(file, Const.UTF8_CHARSET);
    assertEquals(0, endpoints.getEndpoints().size());
  }
  
  @Test
  public void loadFileExceptionOnRead() throws Exception {
    when(Files.toString(file, Const.UTF8_CHARSET))
      .thenThrow(new RuntimeException("Boo!"));
    final HttpEndpoints endpoints = new HttpEndpoints(config, timer);
    
    verify(file, times(1)).exists();
    verify(timer, times(1)).newTimeout(endpoints, 
        HttpEndpoints.DEFAULT_LOAD_INTERVAL, TimeUnit.MILLISECONDS);
    PowerMockito.verifyStatic(never());
    Files.toString(file, Const.UTF8_CHARSET);
    assertEquals(0, endpoints.getEndpoints().size());
  }
  
  @Test
  public void getEndpoints() throws Exception {
    setFile(null);
    final HttpEndpoints endpoints = new HttpEndpoints(config, timer);
    
    List<String> results = endpoints.getEndpoints(null);
    assertEquals(2, results.size());
    assertEquals("host1", results.get(0));
    assertEquals("host2", results.get(1));
    
    results = endpoints.getEndpoints("");
    assertEquals(2, results.size());
    assertEquals("host1", results.get(0));
    assertEquals("host2", results.get(1));
    
    results = endpoints.getEndpoints(HttpEndpoints.DEFAULT_KEY);
    assertEquals(2, results.size());
    assertEquals("host1", results.get(0));
    assertEquals("host2", results.get(1));
    
    results = endpoints.getEndpoints("cluster1");
    assertEquals(2, results.size());
    assertEquals("host3", results.get(0));
    assertEquals("host4", results.get(1));
    
    try {
      endpoints.getEndpoints("doesnotexist");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // straight url overrides
    try {
      endpoints.getEndpoints("htp://typo");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    results = endpoints.getEndpoints("http://localhost:4242");
    assertEquals(1, results.size());
    assertEquals("http://localhost:4242", results.get(0));
    
    results = endpoints.getEndpoints("http://localhost:4242;http://otherhost:4242");
    assertEquals(2, results.size());
    assertEquals("http://localhost:4242", results.get(0));
    assertEquals("http://otherhost:4242", results.get(1));
    
    results = endpoints.getEndpoints("http://localhost:4242;justsomeotherstring");
    assertEquals(2, results.size());
    assertEquals("http://localhost:4242", results.get(0));
    assertEquals("justsomeotherstring", results.get(1));
  }
  
  /**
   * Helper that marks the file as extant and returns either the given JSON
   * string or the default.
   * @param json An optional JSON string to return.
   * @throws Exception If something goes pear shaped.
   */
  private void setFile(String json) throws Exception {
    when(file.exists()).thenReturn(true);
    
    if (json == null || json.isEmpty()) {
      json = "{\"" + HttpEndpoints.DEFAULT_KEY + "\":[\"host1\",\"host2\"],"
          + "\"cluster1\":[\"host3\",\"host4\"],\"cluster2\":[\"host5\"],"
          + "\"cluster3\":[]}";
    }
    when(Files.toString(file, Const.UTF8_CHARSET)).thenReturn(json);
  }
}
